#pragma once

#include <istream>
#include <ostream>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/Types.h>
#include <common/types.h>

namespace DB::HybridCache
{
// abstract class of a cache engine.
class CacheEngine
{
public:
    virtual ~CacheEngine() = default;

    // return the size of cache space.
    virtual UInt64 getSize() const = 0;

    // return true if the entry could exist.
    virtual bool couldExist(HashedKey key) = 0;

    // return the estimate write size.
    virtual UInt64 estimateWriteSize(HashedKey key, BufferView value) const = 0;

    virtual Status insert(HashedKey key, BufferView value) = 0;

    virtual Status lookup(HashedKey key, Buffer & value) = 0;

    virtual Status remove(HashedKey key) = 0;

    // flush all buffered operations
    virtual void flush() = 0;

    virtual void reset() = 0;

    // serializes engine state
    virtual void persist(std::ostream * os) = 0;

    // deserialize engine state
    virtual bool recover(std::istream * is) = 0;

    virtual UInt64 getMaxItemSize() const = 0;

    // get key and buffer for a random sample
    virtual std::pair<Status, std::string> getRandomAlloc(Buffer & value) = 0;
};
}
