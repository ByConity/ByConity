#include <TSO/TSOProxy.h>

namespace DB
{

namespace TSO
{

TSOProxy::TSOProxy(TSOConfig & config)
{
    kv_ptr = std::make_shared<TSOByteKVImpl>(
        config.service_name,
        config.cluster_name,
        config.name_space,
        config.table_name,
        config.key_name);
}

void TSOProxy::setTimestamp(UInt64 timestamp)
{
    kv_ptr->put(std::to_string(timestamp));
}

void TSOProxy::getTimestamp(UInt64 & timestamp)
{
    String timestamp_str;
    kv_ptr->get(timestamp_str);
    if (timestamp_str.empty())
    {
        timestamp = 0;
    }
    else
    {
        timestamp = std::stoull(timestamp_str);
    }
}

void TSOProxy::clean()
{
    kv_ptr->clean();
}

}

}
