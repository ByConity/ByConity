#include <TSO/TSOProxy.h>
#include <TSO/TSOMetaByteKVImpl.h>
#include <TSO/TSOMetaFDBImpl.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TSO_INTERNAL_ERROR;
}

namespace TSO
{

TSOProxy::TSOProxy(const TSOConfig & config)
{
    if (config.type == StoreType::BYTEKV)
    {
        metastore_ptr = std::make_shared<TSOMetaByteKVImpl>(
            config.bytekv_conf.service_name,
            config.bytekv_conf.cluster_name,
            config.bytekv_conf.name_space,
            config.bytekv_conf.table_name,
            config.key_name);
    }
    else if (config.type == StoreType::FDB)
    {
        metastore_ptr = std::make_shared<TSOMetaFDBImpl>(config.fdb_conf.cluster_conf_path, config.key_name);
    }
    else
        throw Exception("TSO metastore type should be set. Only support foundationdb and bytekv.", ErrorCodes::TSO_INTERNAL_ERROR);
}

void TSOProxy::setTimestamp(UInt64 timestamp)
{
    metastore_ptr->put(std::to_string(timestamp));
}

UInt64 TSOProxy::getTimestamp()
{
    String timestamp_str;
    metastore_ptr->get(timestamp_str);
    if (timestamp_str.empty())
    {
        return 0;
    }
    else
    {
        return std::stoull(timestamp_str);
    }
}

void TSOProxy::clean()
{
    metastore_ptr->clean();
}

}

}
