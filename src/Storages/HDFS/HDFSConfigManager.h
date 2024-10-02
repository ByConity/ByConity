#pragma once

#include <set>
#include <shared_mutex>
#include <consul/bridge.h>
#include <Poco/DOM/AutoPtr.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/Element.h>
#include <common/logger_useful.h>
#include <common/types.h>

namespace DB
{

class HDFSConfigManager
{
public:
    static HDFSConfigManager & instance();

private:
    static void addProperty(
        Poco::AutoPtr<Poco::XML::Document> doc,
        Poco::AutoPtr<Poco::XML::Element> parent,
        const String & property_name,
        const String & property_value);

public:
    void updateAll(bool force);
    void tryUpdate(const String & uri);

private:
    HDFSConfigManager();
    void update(const String & uri);

    void flushXml(const std::vector<cpputil::consul::ServiceEndpoint> & endpoints);
    std::optional<std::vector<cpputil::consul::ServiceEndpoint>> getNProxyEndpoints();

    inline static String HDFS_SITE_XML = "hdfs-site.xml";

    const bool enable{false};
    const String nnproxy{};
    const String local_dir{};
    const int64_t force_update_interval{0};

    using Clock = std::chrono::steady_clock;
    Clock::time_point last_update_time{};

    std::shared_mutex mutex;
    std::set<String> managed_domains;

    Poco::Logger * log{&Poco::Logger::get("HDFSConfigManager")};
};
}
