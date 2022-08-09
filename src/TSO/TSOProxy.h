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

    TSOProxy(TSOConfig & config);
    ~TSOProxy() {}

    void setTimestamp(UInt64 timestamp);
    void getTimestamp(UInt64 & timestamp);
    void clean();

private:
    TSOMetastorePtr metastore_ptr;
};

}

}

