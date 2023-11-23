#include <Storages/DiskCache/Bucket.h>

#include <Common/thread_local_rng.h>

namespace DB::HybridCache
{
static_assert(sizeof(Bucket) == 24, "Bucket overhead");

namespace
{
    const BucketEntry * getIteratorEntry(BucketStorage::Allocation iter)
    {
        return reinterpret_cast<const BucketEntry *>(iter.view().data());
    }
}

BufferView Bucket::Iterator::key() const
{
    return getIteratorEntry(iterator)->key();
}

UInt64 Bucket::Iterator::keyHash() const
{
    return getIteratorEntry(iterator)->keyHash();
}

BufferView Bucket::Iterator::value() const
{
    return getIteratorEntry(iterator)->value();
}

bool Bucket::Iterator::keyEqualsTo(HashedKey key) const
{
    return getIteratorEntry(iterator)->keyEqualsTo(key);
}

UInt32 Bucket::computeChecksum(BufferView view)
{
    constexpr auto k_checksum_start = sizeof(checksum);
    auto data = view.slice(k_checksum_start, view.size() - k_checksum_start);
    return HybridCache::checksum(data);
}

Bucket & Bucket::initNew(MutableBufferView view, UInt64 generation_time)
{
    return *new (view.data()) Bucket(generation_time, view.size() - sizeof(Bucket));
}

BufferView Bucket::find(HashedKey key) const
{
    auto itr = storage.getFirst();
    while (!itr.done())
    {
        const auto * entry = getIteratorEntry(itr);
        if (entry->keyEqualsTo(key))
            return entry->value();
        itr = storage.getNext(itr);
    }
    return {};
}

std::pair<UInt32, UInt32>
Bucket::insert(HashedKey key, BufferView value, const ExpiredCheck & check_expired, const DestructorCallback & destructor_callback)
{
    const auto size = BucketEntry::computeSize(key.key().size, value.size());
    chassert(size <= storage.capacity());

    auto ret = makeSpace(size, check_expired, destructor_callback);
    auto alloc = storage.allocate(size);
    chassert(!alloc.done());
    BucketEntry::create(alloc.view(), key, value);

    return ret;
}

std::pair<UInt32, UInt32> Bucket::makeSpace(UInt32 size, const ExpiredCheck & check_expired, const DestructorCallback & destructor_callback)
{
    const auto required_size = BucketStorage::slotSize(size);
    chassert(required_size <= storage.capacity());

    if (storage.remainingCapacity() >= required_size)
        return {};

    UInt32 eviction_expired = removeExpired(storage.getFirst(), check_expired, destructor_callback);
    UInt32 evictions = eviction_expired;
    // Check available space again after evictions
    auto cur_free_space = storage.remainingCapacity();
    if (eviction_expired > 0 && cur_free_space >= required_size)
    {
        return std::make_pair(evictions, eviction_expired);
    }

    auto itr = storage.getFirst();
    while (true)
    {
        evictions++;

        if (destructor_callback)
        {
            const auto * entry = getIteratorEntry(itr);
            destructor_callback(entry->hashedKey(), entry->value(), DestructorEvent::Recycled);
        }

        cur_free_space += BucketStorage::slotSize(itr.view().size());
        if (cur_free_space >= required_size)
        {
            storage.removeUntil(itr);
            break;
        }
        itr = storage.getNext(itr);
        chassert(!itr.done());
    }
    return std::make_pair(evictions, eviction_expired);
}

UInt32
Bucket::removeExpired(BucketStorage::Allocation itr, const ExpiredCheck & check_expired, const DestructorCallback & destructor_callback)
{
    if (!check_expired)
        return 0;

    UInt32 evictions = 0;
    std::vector<BucketStorage::Allocation> removed;
    while (!itr.done())
    {
        const auto * entry = getIteratorEntry(itr);
        if (!check_expired(entry->value()))
        {
            itr = storage.getNext(itr);
            continue;
        }

        // Remove expired entry
        if (destructor_callback)
            destructor_callback(entry->hashedKey(), entry->value(), DestructorEvent::Recycled);
        removed.emplace_back(itr);
        itr = storage.getNext(itr);
        evictions++;
    }
    storage.remove(removed);
    return evictions;
}

UInt32 Bucket::remove(HashedKey key, const DestructorCallback & destructor_callback)
{
    auto itr = storage.getFirst();
    while (!itr.done())
    {
        const auto * entry = getIteratorEntry(itr);
        if (entry->keyEqualsTo(key))
        {
            if (destructor_callback)
                destructor_callback(entry->hashedKey(), entry->value(), DestructorEvent::Removed);
            storage.remove(itr);
            return 1;
        }
        itr = storage.getNext(itr);
    }
    return 0;
}

std::pair<std::string, BufferView> Bucket::getRandomAlloc()
{
    pcg64 thread_local_rng;
    std::uniform_int_distribution<UInt64> dis(0, storage.capacity());
    const auto rand_offset = dis(thread_local_rng);

    auto itr = storage.getFirst();
    while (!itr.done())
    {
        auto offset = storage.getOffset(itr);
        auto size = itr.view().size();
        if (rand_offset < offset + size)
        {
            const auto * entry = getIteratorEntry(itr);
            return std::make_pair(reinterpret_cast<const char *>(entry->key().data()), entry->value());
        }
        itr = storage.getNext(itr);
    }
    return {};
}

Bucket::Iterator Bucket::getFirst() const
{
    return Iterator{storage.getFirst()};
}

Bucket::Iterator Bucket::getNext(Iterator itr) const
{
    return Iterator{storage.getNext(itr.iterator)};
}
}
