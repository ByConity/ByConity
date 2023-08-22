#include "S3MetaSanitizer.h"
#include <iostream>
#include <Poco/Exception.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/Option.h>
#include <Poco/Util/OptionSet.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/Path.h>
#include <Poco/PatternFormatter.h>
#include <common/logger_useful.h>
#include <Common/Exception.h>
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <Catalog/MetastoreFDBImpl.h>
#include <Catalog/StringHelper.h>
#include <Catalog/CatalogConfig.h>
#include <Catalog/MetastoreProxy.h>
#include <Protos/data_models.pb.h>

namespace brpc
{
namespace policy
{
    DECLARE_string(consul_agent_addr);
}
}

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int METASTORE_EXCEPTION;
}

void S3MetaSanitizer::defineOptions(Poco::Util::OptionSet& options)
{
    Poco::Util::Application::defineOptions(options);

    options.addOption(
        Poco::Util::Option("help", "h", "show help message")
            .required(false)
            .repeatable(false)
            .binding("help")
    );
    options.addOption(
        Poco::Util::Option("config-file", "C", "configuration of bycontiy service path")
            .required(true)
            .repeatable(false)
            .argument("<file>")
            .binding("config-file"));
    options.addOption(
        Poco::Util::Option("log-level", "L", "logging_level")
            .required(false)
            .repeatable(false)
            .binding("log-level")
    );
}

void S3MetaSanitizer::initialize(Poco::Util::Application& self)
{
    std::string conf_path = config().getString("config-file");
    loadConfiguration(conf_path);

    Application::initialize(self);

    Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter("%Y.%m.%d %H:%M:%S.%F <%p> %s: %t"));
    Poco::AutoPtr<Poco::ConsoleChannel> console_chanel(new Poco::ConsoleChannel);
    Poco::AutoPtr<Poco::FormattingChannel> channel(new Poco::FormattingChannel(formatter, console_chanel));
    Poco::Logger::root().setLevel(config().getString("log-level", "debug"));
    Poco::Logger::root().setChannel(channel);

    initializeMetastore();

    logger = &Poco::Logger::get("S3MetaSanitizer");
}

int S3MetaSanitizer::main(const std::vector<std::string>&)
{
    if (config().has("help"))
    {
        std::cout << "Usage: \n" << "meta_sanitinzer --config-file <configuration-file> --log-level \"debug\"" << std::endl;
        return Poco::Util::Application::EXIT_OK;
    }

    try
    {
        sanitizePartsMeta(Catalog::escapeString(catalog_namespace) + '_' + PART_STORE_PREFIX);
        sanitizePartsMeta(Catalog::escapeString(catalog_namespace) + '_' + STAGED_PART_STORE_PREFIX);
    }
    catch (...)
    {
        tryLogCurrentException(&Poco::Logger::get("S3MetaSanitizer"));
        return Poco::Util::Application::EXIT_SOFTWARE;
    }
    return Poco::Util::Application::EXIT_OK;
}

void S3MetaSanitizer::initializeMetastore()
{
    Catalog::CatalogConfig catalog_conf(config());
    const char * consul_http_host = getenv("CONSUL_HTTP_HOST");
    const char * consul_http_port = getenv("CONSUL_HTTP_PORT");
    if (consul_http_host != nullptr && consul_http_port != nullptr)
        brpc::policy::FLAGS_consul_agent_addr = "http://" + std::string(consul_http_host) + ":" + std::string(consul_http_port);

    catalog_namespace = config().getString("catalog.name_space", "default");

    if (catalog_conf.type == Catalog::StoreType::FDB)
    {
        metastore_ptr = std::make_shared<Catalog::MetastoreFDBImpl>(catalog_conf.fdb_conf.cluster_conf_path);
    }
    else
    {
        throw Exception(ErrorCodes::METASTORE_EXCEPTION, "Catalog must be correctly configured. Only support foundationdb and bytekv now.");
    }
}

void S3MetaSanitizer::sanitizePartsMeta(const std::string& prefix)
{
    Catalog::IMetaStore::IteratorPtr iter = metastore_ptr->getByPrefix(prefix);
    while (iter->next())
    {
        Protos::S3SanitizerDataModelPart origin_part;
        origin_part.ParseFromString(iter->value());

        if (origin_part.has_part_id())
        {
            LOG_INFO(logger, "Sanitize key {}", iter->key());

            (*origin_part.mutable_new_part_id()) = origin_part.part_id();
            origin_part.clear_part_id();

            metastore_ptr->put(iter->key(), origin_part.SerializeAsString());
        }
        else
        {
            LOG_INFO(logger, "Skipping key {} since there is no part id", iter->key());
        }
    }
}

}

int mainEntryClickhouseS3MetaSanitizer(int argc, char** argv)
{
    Poco::AutoPtr<DB::S3MetaSanitizer> s3_meta_sanitizer = new DB::S3MetaSanitizer;
    s3_meta_sanitizer->init(argc, argv);
    return s3_meta_sanitizer->run();
}
