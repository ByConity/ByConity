#pragma once

#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/Types.h>

namespace DB::HybridCache
{
using InsertCallback = std::function<void(Status status, HashedKey key)>;

using LookupCallback = std::function<void(Status status, HashedKey key, Buffer value)>;

using RemoveCallback = std::function<void(Status status, HashedKey key)>;

class AbstractCache
{
public:
    virtual ~AbstractCache() = default;

    virtual bool isItemLarge(HashedKey key, BufferView value, EngineTag tag) const = 0;

    virtual bool couldExist(HashedKey key, EngineTag tag) = 0;

    virtual Status insert(HashedKey key, BufferView value, EngineTag tag) = 0;

    virtual Status insertAsync(HashedKey key, BufferView value, InsertCallback cb, EngineTag tag) = 0;

    virtual Status lookup(HashedKey key, Buffer & value, EngineTag tag) = 0;

    virtual void lookupAsync(HashedKey key, LookupCallback cb, EngineTag tag) = 0;

    virtual Status remove(HashedKey key, EngineTag tag) = 0;

    virtual void removeAsync(HashedKey key, RemoveCallback cb, EngineTag tag) = 0;

    virtual void flush() = 0;

    virtual void reset() = 0;

    virtual void persist() const = 0;

    virtual bool recover() = 0;

    // virtual uint64_t getSize() const = 0;

    // virtual uint64_t getUsableSize() const = 0;

    // virtual std::pair<Status, std::string /* key */> getRandomAlloc(Buffer & value) = 0;
};
}
