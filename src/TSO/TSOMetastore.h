#pragma once
#include <Core/Types.h>

namespace DB
{

namespace TSO
{

class TSOMetastore
{
public:
    TSOMetastore(const String & key_name_)
        : key_name(key_name_) {}
    virtual void put(const String & value) = 0;
    virtual void get(String & value) = 0;
    virtual void clean() = 0;
    virtual ~TSOMetastore() {}

    String key_name;
};

using TSOMetastorePtr = std::shared_ptr<TSOMetastore>;

}

}
