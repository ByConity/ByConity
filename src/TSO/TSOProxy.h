#pragma once

#include <TSO/TSOByteKVImpl.h>

namespace DB
{

namespace TSO
{

struct TSOConfig
{
    String service_name;
    String cluster_name;
    String name_space;
    String table_name;
    String key_name;
};

class TSOProxy
{
public:
    using ByteKVPtr = std::shared_ptr<TSOByteKVImpl>;

    TSOProxy(TSOConfig & config);
    ~TSOProxy() {}

    void setTimestamp(UInt64 timestamp);
    void getTimestamp(UInt64 & timestamp);
    void clean();

private:
    ByteKVPtr kv_ptr;
};

}

}

