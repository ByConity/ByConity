#pragma once
#include <Core/Types.h>
#include <memory>
#include <map>

namespace DB
{

namespace Catalog
{

/// Helper class for write and delte data from metastore in one transaction.
class IMultiWrite
{
public:
    virtual void addPut(const String & key, const String & value, const String & expected = "", bool if_not_exists = false);
    virtual void addDelete(const String & key, const UInt64 & expected_version = 0);
    virtual bool commit(bool allow_cas_fail = true);
    virtual size_t getPutsSize();
    virtual size_t getDeleteSize();
    virtual bool isEmpty();
    virtual void setCommitTimeout(const UInt32 & timeout_ms);
    virtual std::map<int, String> collectConflictInfo();
    virtual ~IMultiWrite();
};

using MultiWritePtr = std::shared_ptr<IMultiWrite>;

}
}
