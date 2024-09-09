#include <unistd.h>

#include <Storages/DiskCache/LruPolicy.h>
#include <Storages/DiskCache/Types.h>
#include <common/chrono_io.h>
#include <common/logger_useful.h>

namespace DB::HybridCache
{
LruPolicy::LruPolicy(UInt32 expected_num_regions)
{
    array.reserve(expected_num_regions);
    LOG_INFO(log, "LRU policy: expected {} regions", expected_num_regions);
}

void LruPolicy::touch(RegionId rid)
{
    chassert(rid.valid());
    auto i = rid.index();
    std::lock_guard<TimedMutex> guard{mutex};
    if (i >= array.size())
        array.resize(i + 1);

    if (array[i].inList())
    {
        array[i].hits++;
        array[i].last_update_time = getSteadyClockSeconds();
        unlink(i);
        linkAtHead(i);
    }
}

void LruPolicy::track(const Region & region)
{
    auto rid = region.id();
    chassert(rid.valid());
    auto i = rid.index();
    std::lock_guard<TimedMutex> guard{mutex};
    if (i >= array.size())
        array.resize(i + 1);

    array[i].hits = 0;
    array[i].creation_time = getSteadyClockSeconds();
    array[i].last_update_time = getSteadyClockSeconds();
    if (!array[i].inList())
        linkAtHead(i);
}

RegionId LruPolicy::evict()
{
    UInt32 ret_region{kInvalidIndex};

    {
        std::lock_guard<TimedMutex> guard{mutex};
        if (tail == kInvalidIndex)
            return RegionId{};
        ret_region = tail;
        unlink(tail);
    }

    return RegionId{ret_region};
}

void LruPolicy::reset()
{
    std::lock_guard<TimedMutex> lock{mutex};
    array.clear();
    head = kInvalidIndex;
    tail = kInvalidIndex;
}

void LruPolicy::unlink(UInt32 i)
{
    auto & node = array[i];
    chassert(tail != kInvalidIndex);
    if (tail == i)
        tail = node.prev;
    else
    {
        chassert(node.next != kInvalidIndex);
        array[node.next].prev = node.prev;
    }
    chassert(head != kInvalidIndex);
    if (head == i)
        head = node.next;
    else
    {
        chassert(node.prev != kInvalidIndex);
        array[node.prev].next = node.next;
    }
    node.next = kInvalidIndex;
    node.prev = kInvalidIndex;
}

void LruPolicy::linkAtHead(UInt32 i)
{
    if (i != head)
    {
        if (head != kInvalidIndex)
            array[head].prev = i;
        array[i].next = head;
        head = i;
        if (tail == kInvalidIndex)
            tail = i;
    }
}

void LruPolicy::linkAtTail(UInt32 i)
{
    if (i != tail)
    {
        if (tail != kInvalidIndex)
            array[tail].next = i;
        array[i].prev = tail;
        tail = i;
        if (head == kInvalidIndex)
            head = i;
    }
}

size_t LruPolicy::memorySize() const
{
    return sizeof(*this) + sizeof(ListNode) * array.capacity();
}
}
