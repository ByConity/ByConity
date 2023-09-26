#pragma once

#include <string>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>


namespace DB
{

#define CATALOG_SERVICE_CONFIGURE "catalog_service"
#define TSO_SERVICE_CONFIGURE "tso_service"

namespace ErrorCodes
{
    extern const int METASTORE_EXCEPTION;
} // namespace DB::ErrorCodes

enum class MetaStoreType
{
    UNINIT,
    BYTEKV,
    FDB
};

struct MetastoreConfig
{
    struct FDBConf
    {
        std::string cluster_conf_path;
    };

    struct ByteKVConf
    {
        std::string service_discovery_type;
        std::string service_name;
        uint64_t service_port;
        std::string cluster_name;
        std::string name_space;
        std::string table_name;
    };

    MetastoreConfig() {}

    MetastoreConfig(const Poco::Util::AbstractConfiguration & poco_config, const std::string & service_name)
    {
        if (poco_config.has(service_name))
        {
            if (poco_config.has(service_name + ".type"))
            {
                std::string metastore_type = poco_config.getString(service_name + ".type");
                if (metastore_type == "fdb")
                    type = MetaStoreType::FDB;
                else
                    throw Exception("Unsupportted metastore type " + metastore_type, ErrorCodes::METASTORE_EXCEPTION);
            }

            if (type == MetaStoreType::FDB)
            {
                fdb_conf.cluster_conf_path = poco_config.getString(service_name + ".fdb.cluster_file");
            }

            if (poco_config.has(service_name + ".topology_key"))
            {
                topology_key = poco_config.getString(service_name + ".topology_key");
            }

            if (service_name == TSO_SERVICE_CONFIGURE)
                key_name = poco_config.getString(service_name + ".key_name", "tso");
        }
    }

    MetaStoreType type = MetaStoreType::UNINIT;
    FDBConf fdb_conf;
    ByteKVConf bytekv_conf;
    String topology_key;

    // TSO service only.
    String key_name;
};

}
