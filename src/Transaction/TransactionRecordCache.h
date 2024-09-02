#pragma once

#include <memory>
#include <Common/ProfileEvents.h>
#include <Common/LRUCache.h>
#include <Core/Types.h>
#include <Transaction/TransactionCommon.h>

namespace ProfileEvents
{
    extern const Event CnchTxnRecordCacheHits;
    extern const Event CnchTxnRecordCacheMisses;
}

namespace DB
{

class TransactionRecordCache : public LRUCache<UInt64, TransactionRecordLite>
{

private:
    using Base = LRUCache<UInt64, TransactionRecordLite>;

public:
    explicit TransactionRecordCache(size_t max_txn_records) : Base(max_txn_records) 
    {
    }

    MappedPtr get(const Key & key)
    {
        auto result = Base::get(key);
        if (result)
            ProfileEvents::increment(ProfileEvents::CnchTxnRecordCacheHits);
        else
            ProfileEvents::increment(ProfileEvents::CnchTxnRecordCacheMisses);

        return result;
    }

    void insert(const Key & key, const MappedPtr & value)
    {
        Base::set(key, value);
    }

    void dropCache()
    {
        Base::reset();
    }

};

using TransactionRecordCachePtr = std::unique_ptr<TransactionRecordCache>;

}
