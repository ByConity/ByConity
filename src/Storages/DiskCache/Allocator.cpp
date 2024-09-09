#include <tuple>

#include <fmt/core.h>

#include <Storages/DiskCache/Allocator.h>
#include <Storages/DiskCache/Region.h>
#include <Storages/DiskCache/RegionManager.h>
#include <Storages/DiskCache/Types.h>
#include <common/defines.h>
#include <common/logger_useful.h>

namespace DB::HybridCache
{
void RegionAllocator::setAllocationRegion(RegionId rid_)
{
    chassert(!rid.valid());
    rid = rid_;
}

void RegionAllocator::reset()
{
    rid = RegionId{};
}

Allocator::Allocator(RegionManager & region_manager_, UInt16 num_priorities) : region_manager{region_manager_}
{
    LOG_INFO(log, fmt::format("Enable priority-based allocation for Allocator. Number of priorities: {}", num_priorities));
    for (UInt16 i = 0; i < num_priorities; i++)
        allocators.emplace_back(i);
}

std::tuple<RegionDescriptor, UInt32, RelAddress> Allocator::allocate(UInt32 size, UInt16 priority, bool can_wait)
{
    chassert(priority < allocators.size());
    RegionAllocator * ra = &allocators[priority];
    if (size == 0 || size > region_manager.regionSize())
        return std::make_tuple(RegionDescriptor{OpenStatus::Error}, size, RelAddress{});
    return allocateWith(*ra, size, can_wait);
}

std::tuple<RegionDescriptor, UInt32, RelAddress> Allocator::allocateWith(RegionAllocator & ra, UInt32 size, bool can_wait)
{
    std::unique_lock<folly::fibers::TimedMutex> lock{ra.getLock()};
    RegionId rid = ra.getAllocationRegion();
    if (rid.valid())
    {
        auto & region = region_manager.getRegion(rid);
        auto [desc, addr] = region.openAndAllocate(size);
        chassert(OpenStatus::Retry != desc.getStatus());
        if (desc.isReady())
            return std::make_tuple(std::move(desc), size, addr);
        chassert(OpenStatus::Error == desc.getStatus());
        flushAndReleaseRegionFromRALocked(ra, true);
        rid = RegionId{};
    }

    chassert(!rid.valid());

    if (can_wait && !getCurrentFiberThread())
    {
        // Waiting on main thread could cause indefinite blocking, so do not wait
        can_wait = false;
    }

    auto [status, waiter] = region_manager.getCleanRegion(rid, can_wait);
    if (status == OpenStatus::Retry)
    {
        lock.unlock();
        if (waiter)
        {
            waiter->baton.wait();
        }
        return std::make_tuple(RegionDescriptor{status}, size, RelAddress{});
    }
    chassert(status == OpenStatus::Ready);
    chassert(!waiter);

    auto & region = region_manager.getRegion(rid);
    region.setPriority(ra.getPriority());

    ra.setAllocationRegion(rid);
    auto [desc, addr] = region.openAndAllocate(size);
    chassert(OpenStatus::Ready == desc.getStatus());
    return std::make_tuple(std::move(desc), size, addr);
}

void Allocator::close(RegionDescriptor && desc)
{
    region_manager.close(std::move(desc));
}

void Allocator::flushAndReleaseRegionFromRALocked(RegionAllocator & ra, bool flushAsync)
{
    auto rid = ra.getAllocationRegion();
    if (rid.valid())
    {
        region_manager.doFlush(rid, flushAsync);
        ra.reset();
    }
}

void Allocator::flush()
{
    for (auto & ra : allocators)
    {
        std::unique_lock<folly::fibers::TimedMutex> lock{ra.getLock()};
        flushAndReleaseRegionFromRALocked(ra, false);
    }
}

void Allocator::reset()
{
    region_manager.reset();
    for (auto & ra : allocators)
    {
        std::unique_lock<folly::fibers::TimedMutex> lock{ra.getLock()};
        ra.reset();
    }
}
}
