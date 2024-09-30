#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <utility>
#include <unistd.h>

#include <fmt/core.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/util/delimited_message_util.h>

#include <Protos/disk_cache.pb.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/EvictionPolicy.h>
#include <Storages/DiskCache/JobScheduler.h>
#include <Storages/DiskCache/Region.h>
#include <Storages/DiskCache/RegionManager.h>
#include <Storages/DiskCache/Types.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/InjectPause.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <common/chrono_io.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <common/types.h>

namespace ProfileEvents
{
extern const Event RegionManagerPhysicalWrittenCount;
extern const Event RegionManagerReclaimRegionErrorCount;
extern const Event RegionManagerReclaimCount;
extern const Event RegionManagerReclaimTimeCount;
extern const Event RegionManagerEvictionCount;
extern const Event RegionManagerNumInMemBufFlushRetries;
extern const Event RegionManagerNumInMemBufFlushFailures;
extern const Event RegionManagerNumInMemBufCleanupRetries;
extern const Event RegionManagerCleanRegionRetries;
}

namespace CurrentMetrics
{
extern const Metric RegionManagerExternalFragmentation;
extern const Metric RegionManagerNumInMemBufActive;
extern const Metric RegionManagerNumInMemBufWaitingFlush;
}

namespace DB::ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
}

namespace DB::HybridCache
{
RegionManager::RegionManager(
    UInt32 num_regions_,
    UInt64 region_size_,
    UInt64 base_offset_,
    Device & device_,
    UInt32 num_clean_regions_,
    UInt32 num_workers_,
    RegionEvictCallback evict_callback_,
    RegionCleanupCallback cleanup_callback_,
    std::unique_ptr<EvictionPolicy> policy_,
    UInt32 num_in_mem_buffers_,
    UInt16 num_priorities_,
    UInt16 in_mem_buf_flush_retry_limit_)
    : num_priorities{num_priorities_}
    , in_mem_buf_flush_retry_limit{in_mem_buf_flush_retry_limit_}
    , num_regions{num_regions_}
    , region_size{region_size_}
    , base_offset{base_offset_}
    , device{device_}
    , policy{std::move(policy_)}
    , regions{std::make_unique<std::unique_ptr<Region>[]>(num_regions_)}
    , num_clean_regions{num_clean_regions_}
    , evict_callback{evict_callback_}
    , cleanup_callback{cleanup_callback_}
    , num_in_mem_buffers(num_in_mem_buffers_)
{
    LOG_INFO(log, fmt::format("{} regions, {} bytes each, {} priorities", num_regions, region_size, num_priorities));
    for (UInt32 i = 0; i < num_regions_; i++)
        regions[i] = std::make_unique<Region>(RegionId{i}, region_size);

    chassert(0u < num_in_mem_buffers);

    for (UInt32 i = 0; i < num_in_mem_buffers; i++)
        buffers.push_back(std::make_unique<Buffer>(device.makeIOBuffer(region_size)));

    for (uint32_t i = 0; i < num_workers_; i++)
    {
        auto name = fmt::format("fiber_region_manager_{}", i);
        workers.emplace_back(std::make_unique<FiberThread>(name));
        worker_set.insert(workers.back().get());
    }

    resetEvictionPolicy();
}

RegionId RegionManager::evict()
{
    auto rid = policy->evict();
    if (!rid.valid())
        LOG_ERROR(log, "Eviction failed");
    else
        LOG_DEBUG(log, fmt::format("Evict {}", rid.index()));
    return rid;
}

void RegionManager::touch(RegionId rid)
{
    auto & region = getRegion(rid);
    chassert(rid == region.id());
    if (!region.hasBuffer())
        policy->touch(rid);
}

void RegionManager::track(RegionId rid)
{
    auto & region = getRegion(rid);
    chassert(rid == region.id());
    policy->track(region);
}

void RegionManager::reset()
{
    for (UInt32 i = 0; i < num_regions; i++)
        regions[i]->reset();
    {
        std::lock_guard<TimedMutex> guard{clean_regions_mutex};
        chassert(reclaim_scheduled == 0u);
        clean_regions.clear();
        if (clean_regions_cond.numWaiters() > 0)
            clean_regions_cond.notifyAll();
    }
    seq_number.store(0, std::memory_order_release);

    resetEvictionPolicy();
}

Region::FlushRes RegionManager::flushBuffer(const RegionId & rid)
{
    auto & region = getRegion(rid);
    auto callback = [this](RelAddress addr, BufferView view) {
        auto write_buffer = device.makeIOBuffer(view.size());
        write_buffer.copyFrom(0, view);
        if (!deviceWrite(addr, std::move(write_buffer)))
            return false;
        CurrentMetrics::sub(CurrentMetrics::RegionManagerNumInMemBufWaitingFlush);
        return true;
    };

    return region.flushBuffer(std::move(callback));
}

void RegionManager::detachBuffer(const RegionId & rid)
{
    auto & region = getRegion(rid);
    auto buf = region.detachBuffer();
    chassert(!!buf);
    returnBufferToPool(std::move(buf));
}

void RegionManager::cleanupBufferOnFlushFailure(const RegionId & rid)
{
    auto & region = getRegion(rid);
    auto callback = [this](RegionId regionId, BufferView buf) {
        cleanup_callback(regionId, buf);
        CurrentMetrics::sub(CurrentMetrics::RegionManagerNumInMemBufWaitingFlush);
        ProfileEvents::increment(ProfileEvents::RegionManagerNumInMemBufFlushFailures);
    };

    region.cleanupBuffer(std::move(callback));
    detachBuffer(rid);
}

void RegionManager::releaseCleanedupRegion(RegionId rid)
{
    auto & region = getRegion(rid);
    CurrentMetrics::sub(CurrentMetrics::RegionManagerExternalFragmentation, region.getFragmentationSize());

    seq_number.fetch_add(1, std::memory_order_acq_rel);

    region.reset();
    {
        std::lock_guard<TimedMutex> guard{clean_regions_mutex};
        clean_regions.push_back(rid);
        INJECT_PAUSE(pause_blockcache_clean_free_locked);
        if (clean_regions_cond.numWaiters() > 0)
            clean_regions_cond.notifyAll();
    }
}

std::pair<OpenStatus, std::unique_ptr<CondWaiter>> RegionManager::assignBufferToRegion(RegionId rid, bool add_waiter)
{
    chassert(rid.valid());
    auto [buf, waiter] = claimBufferFromPool(add_waiter);
    if (!buf)
        return {OpenStatus::Retry, std::move(waiter)};

    auto & region = getRegion(rid);
    region.attachBuffer(std::move(buf));
    return {OpenStatus::Ready, std::move(waiter)};
}

std::pair<std::unique_ptr<Buffer>, std::unique_ptr<CondWaiter>> RegionManager::claimBufferFromPool(bool add_waiter)
{
    std::unique_ptr<Buffer> buf;
    {
        std::lock_guard<TimedMutex> guard{buffer_mutex};
        if (buffers.empty())
        {
            std::unique_ptr<CondWaiter> waiter;
            if (add_waiter)
            {
                waiter = std::make_unique<CondWaiter>();
                buffer_cond.addWaiter(waiter.get());
            }
            return {nullptr, std::move(waiter)};
        }
        buf = std::move(buffers.back());
        buffers.pop_back();
    }
    CurrentMetrics::add(CurrentMetrics::RegionManagerNumInMemBufActive);
    return {std::move(buf), nullptr};
}

void RegionManager::returnBufferToPool(std::unique_ptr<Buffer> buf)
{
    {
        std::lock_guard<TimedMutex> guard{buffer_mutex};
        buffers.push_back(std::move(buf));
        if (buffer_cond.numWaiters() > 0)
            buffer_cond.notifyAll();
    }
    CurrentMetrics::sub(CurrentMetrics::RegionManagerNumInMemBufActive);
}

std::pair<OpenStatus, std::unique_ptr<CondWaiter>> RegionManager::getCleanRegion(RegionId & rid, bool add_waiter)
{
    auto status = OpenStatus::Retry;
    std::unique_ptr<CondWaiter> waiter;
    UInt32 new_shced = 0;
    {
        std::lock_guard<TimedMutex> guard{clean_regions_mutex};
        if (!clean_regions.empty())
        {
            rid = clean_regions.back();
            clean_regions.pop_back();
            INJECT_PAUSE(pause_blockcache_clean_alloc_locked);
            status = OpenStatus::Ready;
        }
        else
        {
            if (add_waiter)
            {
                waiter = std::make_unique<CondWaiter>();
                clean_regions_cond.addWaiter(waiter.get());
            }
            status = OpenStatus::Retry;
        }

        auto planned_clean = clean_regions.size() + reclaim_scheduled;
        if (planned_clean < num_clean_regions)
        {
            new_shced = num_clean_regions - planned_clean;
            reclaim_scheduled += new_shced;
        }
    }
    for (UInt32 i = 0; i < new_shced; i++)
        startReclaim();

    if (status == OpenStatus::Ready)
    {
        chassert(!waiter);
        std::tie(status, waiter) = assignBufferToRegion(rid, add_waiter);
        if (status != OpenStatus::Ready)
        {
            std::lock_guard<TimedMutex> guard{clean_regions_mutex};
            clean_regions.push_back(rid);
            INJECT_PAUSE(pause_blockcache_clean_free_locked);
            if (clean_regions_cond.numWaiters() > 0)
                clean_regions_cond.notifyAll();
        }
    }
    else
    {
        if (status == OpenStatus::Retry)
            ProfileEvents::increment(ProfileEvents::RegionManagerCleanRegionRetries);
    }
    return {status, std::move(waiter)};
}


bool RegionManager::isOnWorker()
{
    auto * thread = getCurrentFiberThread();
    if (!thread)
    {
        return false;
    }

    return worker_set.count(thread) > 0;
}

void RegionManager::doFlush(RegionId rid, bool async)
{
    auto & region = getRegion(rid);
    CurrentMetrics::add(CurrentMetrics::RegionManagerExternalFragmentation, region.getFragmentationSize());

    region.setPendingFlush();
    CurrentMetrics::add(CurrentMetrics::RegionManagerNumInMemBufWaitingFlush);

    if (!async || isOnWorker())
        doFlushInternal(rid);
    else
        getNextWorker().addTaskRemote([this, rid]() { doFlushInternal(rid); });
}

void RegionManager::doFlushInternal(RegionId rid)
{
    INJECT_PAUSE(pause_flush_begin);
    int retry_attempts = 0;
    while (retry_attempts < in_mem_buf_flush_retry_limit)
    {
        auto res = flushBuffer(rid);
        if (res == Region::FlushRes::kSuccess)
        {
            break;
        }
        else if (res == Region::FlushRes::kRetryDeviceFailure)
        {
            // We have a limited retry limit for flush errors due to device
            retry_attempts++;
            ProfileEvents::increment(ProfileEvents::RegionManagerNumInMemBufCleanupRetries);
        }

        // Device write failed; retry after 100ms
        folly::fibers::Baton b;
        b.try_wait_for(std::chrono::milliseconds(100));
    }

    if (retry_attempts >= in_mem_buf_flush_retry_limit)
    {
        // Flush failure reaches retry limit, stop flushing and start to
        // clean up the buffer.
        cleanupBufferOnFlushFailure(rid);
        releaseCleanedupRegion(rid);
        INJECT_PAUSE(pause_flush_failure);
        return;
    }

    INJECT_PAUSE(pause_flush_detach_buffer);
    detachBuffer(rid);

    // Flush completed, track the region
    track(rid);
    INJECT_PAUSE(pause_flush_done);
}

void RegionManager::startReclaim()
{
    getNextWorker().addTaskRemote([&]() { doReclaim(); });
}

void RegionManager::doReclaim()
{
    RegionId rid;
    INJECT_PAUSE(pause_reclaim_begin);
    while (true)
    {
        rid = evict();
        // evict() can fail to find a victim, where it needs to be retried
        if (rid.valid())
        {
            break;
        }
        // This should never happen
        chassert(false);
    }

    const auto start_time = getSteadyClock();
    auto & region = getRegion(rid);
    bool status = region.readyForReclaim(true);
    chassert(status);

    if (region.getNumItems() != 0)
    {
        chassert(!region.hasBuffer());
        auto desc = RegionDescriptor::makeReadDescriptor(OpenStatus::Ready, RegionId{rid}, true);
        auto size_to_read = region.getLastEntryEndOffset();
        auto buffer = read(desc, RelAddress{rid, 0}, size_to_read);
        if (buffer.size() != size_to_read)
        {
            LOG_ERROR(
                log,
                fmt::format(
                    "Failed to read region {} during reclaim. Region size to read: {}, Actually read: {}",
                    rid.index(),
                    size_to_read,
                    buffer.size()));
            ProfileEvents::increment(ProfileEvents::RegionManagerReclaimRegionErrorCount);
        }
        else
            doEviction(rid, buffer.view());
    }
    releaseEvictedRegion(rid, start_time);
    INJECT_PAUSE(pause_reclaim_done);
}

RegionDescriptor RegionManager::openForRead(RegionId rid, UInt64 seq_number_)
{
    auto & region = getRegion(rid);
    auto desc = region.openForRead();
    if (!desc.isReady())
        return desc;

    if (seq_number.load(std::memory_order_acquire) != seq_number_)
    {
        region.close(std::move(desc));
        return RegionDescriptor{OpenStatus::Retry};
    }
    return desc;
}

void RegionManager::close(RegionDescriptor && desc)
{
    RegionId rid = desc.id();
    auto & region = getRegion(rid);
    region.close(std::move(desc));
}

void RegionManager::releaseEvictedRegion(RegionId rid, std::chrono::nanoseconds start_time)
{
    auto & region = getRegion(rid);
    CurrentMetrics::sub(CurrentMetrics::RegionManagerExternalFragmentation, region.getFragmentationSize());

    seq_number.fetch_add(1, std::memory_order_acq_rel);

    region.reset();
    {
        std::lock_guard<TimedMutex> guard{clean_regions_mutex};
        reclaim_scheduled--;
        clean_regions.push_back(rid);
        INJECT_PAUSE(pause_blockcache_clean_free_locked);
        if (clean_regions_cond.numWaiters() > 0)
            clean_regions_cond.notifyAll();
    }
    ProfileEvents::increment(ProfileEvents::RegionManagerReclaimTimeCount, toMicros(getSteadyClock() - start_time).count());
    ProfileEvents::increment(ProfileEvents::RegionManagerReclaimCount);
}

void RegionManager::doEviction(RegionId rid, BufferView buffer) const
{
    INJECT_PAUSE(pause_do_eviction_start);
    if (buffer.isNull())
        LOG_ERROR(log, "Error reading region {} on reclamation", rid.index());
    else
    {
        Stopwatch evict_stop_watch;
        LOG_DEBUG(log, "Evict region {} entries", rid.index());
        auto num_evicted = evict_callback(rid, buffer);
        LOG_DEBUG(log, "Evict region {} entries: {} us", rid.index(), evict_stop_watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::RegionManagerEvictionCount, num_evicted);
    }
    INJECT_PAUSE(pause_do_eviction_done);
}

void RegionManager::persist(google::protobuf::io::CodedOutputStream * stream) const
{
    Protos::RegionData region_data;
    region_data.set_region_size(region_size);
    for (UInt32 i = 0; i < num_regions; i++)
    {
        auto * region = region_data.add_regions();
        region->set_region_id(i);
        region->set_last_entry_end_offset(regions[i]->getLastEntryEndOffset());
        region->set_priority(regions[i]->getPriority());
        region->set_num_items(regions[i]->getNumItems());
    }
    google::protobuf::util::SerializeDelimitedToCodedStream(region_data, stream);
}

void RegionManager::recover(google::protobuf::io::CodedInputStream * stream)
{
    Protos::RegionData region_data;
    google::protobuf::util::ParseDelimitedFromCodedStream(&region_data, stream, nullptr);
    if (static_cast<UInt32>(region_data.regions_size()) != num_regions || region_data.region_size() != region_size)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Could not recover RegionManager. Invalid RegionData.");

    for (auto & region_proto : *region_data.mutable_regions())
    {
        UInt32 index = region_proto.region_id();
        if (index >= num_regions || region_proto.last_entry_end_offset() > region_size)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Could not recover RegionManager. Invalid RegionId.");

        if (num_priorities > 0 && region_proto.priority() >= num_priorities)
            region_proto.set_priority(num_priorities - 1);

        regions[index] = std::make_unique<Region>(region_proto, region_data.region_size());
    }

    resetEvictionPolicy();
}

void RegionManager::resetEvictionPolicy()
{
    chassert(num_regions > 0u);

    policy->reset();
    CurrentMetrics::set(CurrentMetrics::RegionManagerExternalFragmentation, 0);

    for (UInt32 i = 0; i < num_regions; i++)
    {
        CurrentMetrics::add(CurrentMetrics::RegionManagerExternalFragmentation, regions[i]->getFragmentationSize());
        if (regions[i]->getNumItems() == 0)
            track(RegionId{i});
    }

    for (UInt32 i = 0; i < num_regions; i++)
    {
        if (regions[i]->getNumItems() != 0)
            track(RegionId{i});
    }
}

bool RegionManager::isValidIORange(UInt32 offset, UInt32 size) const
{
    return static_cast<UInt64>(offset) + size <= region_size;
}

bool RegionManager::deviceWrite(RelAddress addr, Buffer buf)
{
    const auto buf_size = buf.size();
    chassert(isValidIORange(addr.offset(), buf_size));
    auto phys_offset = physicalOffset(addr);
    if (!device.write(phys_offset, std::move(buf)))
        return false;
    ProfileEvents::increment(ProfileEvents::RegionManagerPhysicalWrittenCount, buf_size);
    return true;
}

bool RegionManager::deviceWrite(RelAddress addr, BufferView buf)
{
    const auto buf_size = buf.size();
    chassert(isValidIORange(addr.offset(), buf_size));
    auto phys_offset = physicalOffset(addr);
    if (!device.write(phys_offset, buf))
        return false;
    ProfileEvents::increment(ProfileEvents::RegionManagerPhysicalWrittenCount, buf_size);
    return true;
}

void RegionManager::write(RelAddress addr, Buffer buf)
{
    auto rid = addr.rid();
    auto & region = getRegion(rid);
    region.writeToBuffer(addr.offset(), buf.view());
}

void RegionManager::write(RelAddress addr, BufferView buf)
{
    auto rid = addr.rid();
    auto & region = getRegion(rid);
    region.writeToBuffer(addr.offset(), buf);
}

Buffer RegionManager::read(const RegionDescriptor & desc, RelAddress addr, size_t size) const
{
    auto rid = addr.rid();
    const auto & region = getRegion(rid);
    chassert(addr.offset() + size <= region.getLastEntryEndOffset());
    if (!desc.isPhysReadMode())
    {
        auto buffer = Buffer(size);
        chassert(region.hasBuffer());
        region.readFromBuffer(addr.offset(), buffer.mutableView());
        return buffer;
    }
    chassert(isValidIORange(addr.offset(), size));

    return device.read(physicalOffset(addr), size);
}

size_t RegionManager::read(const RegionDescriptor & desc, RelAddress addr, size_t size, char * to) const
{
    auto rid = addr.rid();
    const auto & region = getRegion(rid);
    chassert(addr.offset() + size <= region.getLastEntryEndOffset());
    if (!desc.isPhysReadMode())
    {
        auto buffer = Buffer(size);
        chassert(region.hasBuffer());
        region.readFromBuffer(addr.offset(), size, to);
        return size;
    }
    chassert(isValidIORange(addr.offset(), size));

    if (device.read(physicalOffset(addr), size, to))
        return size;

    return 0;
}

void RegionManager::drain()
{
    for (auto & worker : workers)
    {
        worker->drain();
    }
}

void RegionManager::flush()
{
    drain();
    device.flush();
}
}
