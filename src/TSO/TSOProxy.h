#pragma once

#include <TSO/TSOConfig.h>
#include <TSO/TSOMetastore.h>

namespace DB
{

namespace TSO
{

class TSOProxy
{
public:

    TSOProxy(const TSOConfig & config);
    ~TSOProxy() {}

    void setTimestamp(UInt64 timestamp);
    UInt64 getTimestamp();
    void clean();

private:
    TSOMetastorePtr metastore_ptr;
};

}

}
