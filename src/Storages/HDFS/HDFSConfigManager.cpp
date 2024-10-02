#include "HDFSConfigManager.h"

#include <Interpreters/Context.h>
#include <ServiceDiscovery/ServiceDiscoveryConsul.h>
#include <ServiceDiscovery/ServiceDiscoveryFactory.h>
#include <Poco/DOM/AutoPtr.h>
#include <Poco/DOM/DOMImplementation.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/Element.h>
#include <Poco/DOM/Text.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>
#include <Poco/XML/XMLWriter.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
}

HDFSConfigManager & HDFSConfigManager::instance()
{
    static HDFSConfigManager instance;
    return instance;
}

void HDFSConfigManager::addProperty(
    Poco::AutoPtr<Poco::XML::Document> doc,
    Poco::AutoPtr<Poco::XML::Element> parent,
    const String & property_name,
    const String & property_value)
{
    Poco::AutoPtr<Poco::XML::Element> property = doc->createElement("property");

    Poco::AutoPtr<Poco::XML::Element> name = doc->createElement("name");
    Poco::AutoPtr<Poco::XML::Text> name_text = doc->createTextNode(property_name);
    name->appendChild(name_text);
    property->appendChild(name);

    Poco::AutoPtr<Poco::XML::Element> value = doc->createElement("value");
    Poco::AutoPtr<Poco::XML::Text> value_text = doc->createTextNode(property_value);
    value->appendChild(value_text);
    property->appendChild(value);

    parent->appendChild(property);
}

HDFSConfigManager::HDFSConfigManager()
{
    const_cast<String &>(local_dir) = Context::getGlobalContextInstance()->getConfigRef().getString("hdfs_conf_manager.dir", "");
    const_cast<int64_t &>(force_update_interval)
        = Context::getGlobalContextInstance()->getConfigRef().getInt64("hdfs_conf_manager.force_update_interval", 86400L);
    const_cast<String &>(nnproxy) = Context::getGlobalContextInstance()->getHdfsNNProxy();
    const_cast<bool &>(enable) = !local_dir.empty() && !nnproxy.empty();
}

void HDFSConfigManager::updateAll(bool force)
{
    {
        // Use shared lock first
        std::shared_lock<std::shared_mutex> lock(mutex);
        if (!enable)
            return;

        if (!force && Clock::now() - last_update_time < std::chrono::seconds(force_update_interval))
            return;
    }

    LOG_INFO(log, "Update all hdfs config, force: {}, managed_domains: [{}]", force, fmt::join(managed_domains, ","));

    // Then upgrade to unique lock
    std::unique_lock<std::shared_mutex> lock(mutex);

    auto endpoints_opt = getNProxyEndpoints();
    if (!endpoints_opt.has_value())
        return;

    flushXml(endpoints_opt.value());
}

void HDFSConfigManager::tryUpdate(const String & uri)
{
    update(uri);
    updateAll(false);
}

void HDFSConfigManager::update(const String & uri)
{
    String domain;
    {
        // Use shared lock first
        std::shared_lock<std::shared_mutex> lock(mutex);
        if (!enable)
            return;

        Poco::URI parsed_uri(uri);
        domain = parsed_uri.getHost();
        Poco::Net::IPAddress ip;
        if (Poco::Net::IPAddress::tryParse(domain, ip))
            return;

        if (managed_domains.find(domain) != managed_domains.end())
            return;
    }

    // Then upgrade to unique lock
    std::unique_lock<std::shared_mutex> lock(mutex);

    auto endpoints_opt = getNProxyEndpoints();
    if (!endpoints_opt.has_value())
        return;

    const auto & endpoints = endpoints_opt.value();
    managed_domains.insert(domain);
    flushXml(endpoints);
}

void HDFSConfigManager::flushXml(const std::vector<cpputil::consul::ServiceEndpoint> & endpoints)
{
    Poco::AutoPtr<Poco::XML::Document> doc = new Poco::XML::Document();
    Poco::AutoPtr<Poco::XML::Element> configuration = doc->createElement("configuration");
    doc->appendChild(configuration);

    addProperty(doc, configuration, "dfs.nameservices", fmt::format("{}", fmt::join(managed_domains, ",")));

    // Let every nameservice name point to the same endpoints
    std::vector<String> name_flags;
    for (size_t i = 0; i < endpoints.size(); ++i)
    {
        name_flags.push_back(fmt::format("p{}", i));
    }

    for (const auto & domain : managed_domains)
    {
        addProperty(doc, configuration, fmt::format("dfs.ha.namenodes.{}", domain), fmt::format("{}", fmt::join(name_flags, ",")));

        for (size_t i = 0; i < endpoints.size(); ++i)
        {
            const auto & endpoint = endpoints[i];
            Poco::Net::IPAddress ip;
            String formatted_host
                = (Poco::Net::IPAddress::tryParse(endpoint.host, ip) && ip.family() == Poco::Net::IPAddress::IPv6
                       ? fmt::format("[{}]", endpoint.host)
                       : endpoint.host);
            addProperty(
                doc,
                configuration,
                fmt::format("dfs.namenode.rpc-address.{}.{}", domain, name_flags[i]),
                fmt::format("{}:{}", formatted_host, endpoint.port));
        }

        addProperty(
            doc,
            configuration,
            fmt::format("dfs.client.failover.proxy.provider.{}", domain),
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        addProperty(doc, configuration, fmt::format("dfs.ha.automatic-failover.enabled.{}", domain), "true");
    }

    // Write xml to disk
    const String target_file = fmt::format("{}/{}", local_dir, HDFS_SITE_XML);
    const String tmp_file = fmt::format("{}.tmp", target_file);
    std::filesystem::create_directories(std::filesystem::path(target_file).parent_path());

    Poco::XML::DOMWriter writer;
    writer.setOptions(Poco::XML::XMLWriter::PRETTY_PRINT);
    std::ofstream out(tmp_file);
    writer.writeNode(out, doc);
    out.close();

    std::filesystem::rename(tmp_file, target_file);
    last_update_time = Clock::now();

    LOG_INFO(log, "Sync xml to disk successfully, path: {}", target_file);
}

std::optional<std::vector<cpputil::consul::ServiceEndpoint>> HDFSConfigManager::getNProxyEndpoints()
{
    std::vector<cpputil::consul::ServiceEndpoint> endpoints;
    try
    {
        auto sd_consul = ServiceDiscoveryFactory::instance().tryGet(ServiceDiscoveryMode::CONSUL);
        if (sd_consul)
        {
            int retry = 0;
            do
            {
                if (retry++ > 2)
                    return std::nullopt;
                endpoints = sd_consul->lookupEndpoints(nnproxy);
            } while (endpoints.empty());
        }
        else
        {
            int retry = 0;
            do
            {
                if (retry++ > 2)
                    return std::nullopt;
                endpoints = cpputil::consul::lookup_name(nnproxy);
            } while (endpoints.empty());
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        return std::nullopt;
    }

    auto transform_endponits = [&endpoints]() -> std::vector<String> {
        std::vector<String> result;
        std::transform(
            endpoints.begin(), endpoints.end(), std::back_inserter(result), [](const cpputil::consul::ServiceEndpoint & endpoint) {
                return fmt::format("{}:{}", endpoint.host, endpoint.port);
            });
        return result;
    };

    LOG_INFO(log, "Lookup service'{}': [{}]", nnproxy, fmt::join(transform_endponits(), ","));
    return endpoints;
}
}
