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
    JobScheduler & scheduler_,
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
    , scheduler{scheduler_}
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
        LockGuard guard{clean_regions_mutex};
        chassert(reclaim_scheduled == 0u);
        clean_regions.clear();
    }
    seq_number.store(0, std::memory_order_release);

    resetEvictionPolicy();
}

Region::FlushRes RegionManager::flushBuffer(const RegionId & rid)
{
    auto & region = getRegion(rid);
    auto callback = [this](RelAddress addr, BufferView buf) {
        if (!deviceWrite(addr, buf))
            return false;
        CurrentMetrics::sub(CurrentMetrics::RegionManagerNumInMemBufWaitingFlush);
        return true;
    };

    return region.flushBuffer(std::move(callback));
}

bool RegionManager::detachBuffer(const RegionId & rid)
{
    auto & region = getRegion(rid);
    auto buf = region.detachBuffer();
    if (!buf)
        return false;
    returnBufferToPool(std::move(buf));
    return true;
}

bool RegionManager::cleanupBufferOnFlushFailure(const RegionId & rid)
{
    auto & region = getRegion(rid);
    auto callback = [this](RegionId regionId, BufferView buf) {
        cleanup_callback(regionId, buf);
        CurrentMetrics::sub(CurrentMetrics::RegionManagerNumInMemBufWaitingFlush);
        ProfileEvents::increment(ProfileEvents::RegionManagerNumInMemBufFlushFailures);
    };

    if (!region.cleanupBuffer(std::move(callback)))
        return false;

    return detachBuffer(rid);
}

void RegionManager::releaseCleanedupRegion(RegionId rid)
{
    auto & region = getRegion(rid);
    CurrentMetrics::sub(CurrentMetrics::RegionManagerExternalFragmentation, region.getFragmentationSize());

    seq_number.fetch_add(1, std::memory_order_acq_rel);

    region.reset();
    {
        LockGuard guard{clean_regions_mutex};
        clean_regions.push_back(rid);
    }
}

OpenStatus RegionManager::assignBufferToRegion(RegionId rid)
{
    chassert(rid.valid());
    auto buf = claimBufferFromPool();
    if (!buf)
        return OpenStatus::Retry;
    auto & region = getRegion(rid);
    region.attachBuffer(std::move(buf));
    return OpenStatus::Ready;
}

std::unique_ptr<Buffer> RegionManager::claimBufferFromPool()
{
    std::unique_ptr<Buffer> buf;
    {
        LockGuard guard{buffer_mutex};
        if (buffers.empty())
            return nullptr;
        buf = std::move(buffers.back());
        buffers.pop_back();
    }
    CurrentMetrics::add(CurrentMetrics::RegionManagerNumInMemBufActive);
    return buf;
}

void RegionManager::returnBufferToPool(std::unique_ptr<Buffer> buf)
{
    {
        std::lock_guard<std::mutex> guard{buffer_mutex};
        buffers.push_back(std::move(buf));
    }
    CurrentMetrics::sub(CurrentMetrics::RegionManagerNumInMemBufActive);
}

OpenStatus RegionManager::getCleanRegion(RegionId & rid)
{
    auto status = OpenStatus::Retry;
    UInt32 new_shced = 0;
    {
        LockGuard guard{clean_regions_mutex};
        if (!clean_regions.empty())
        {
            rid = clean_regions.back();
            clean_regions.pop_back();
            status = OpenStatus::Ready;
        }
        else
            status = OpenStatus::Retry;

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
        status = assignBufferToRegion(rid);
        if (status != OpenStatus::Ready)
        {
            LockGuard guard{clean_regions_mutex};
            clean_regions.push_back(rid);
        }
    }
    return status;
}

void RegionManager::doFlush(RegionId rid, bool async)
{
    auto & region = getRegion(rid);
    CurrentMetrics::add(CurrentMetrics::RegionManagerExternalFragmentation, region.getFragmentationSize());

    region.setPendingFlush();
    CurrentMetrics::add(CurrentMetrics::RegionManagerNumInMemBufWaitingFlush);

    Job flush_job = [this, rid, retry_attempts = 0, flushed = false]() mutable {
        if (!flushed)
        {
            if (retry_attempts >= in_mem_buf_flush_retry_limit)
            {
                if (cleanupBufferOnFlushFailure(rid))
                {
                    releaseCleanedupRegion(rid);
                    return JobExitCode::Done;
                }
                ProfileEvents::increment(ProfileEvents::RegionManagerNumInMemBufCleanupRetries);
                return JobExitCode::Reschedule;
            }
            auto res = flushBuffer(rid);
            if (res == Region::FlushRes::kSuccess)
                flushed = true;
            else
            {
                if (res == Region::FlushRes::kRetryDeviceFailure)
                {
                    retry_attempts++;
                    ProfileEvents::increment(ProfileEvents::RegionManagerNumInMemBufFlushRetries);
                }
                return JobExitCode::Reschedule;
            }
        }
        if (flushed)
        {
            if (detachBuffer(rid))
            {
                track(rid);
                return JobExitCode::Done;
            }
        }
        return JobExitCode::Reschedule;
    };

    if (async)
        scheduler.enqueue(std::move(flush_job), "flush", JobType::Flush);
    else
        while (flush_job() == JobExitCode::Reschedule)
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
}

void RegionManager::startReclaim()
{
    scheduler.enqueue(
        [this, rid = RegionId()]() mutable {
            if (!rid.valid())
            {
                rid = evict();
                if (!rid.valid())
                    return JobExitCode::Reschedule;
            }

            const auto start_time = getSteadyClock();
            auto & region = getRegion(rid);
            if (!region.readyForReclaim())
                return JobExitCode::Reschedule;

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
            return JobExitCode::Done;
        },
        "reclaim",
        JobType::Reclaim);
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
        LockGuard guard{clean_regions_mutex};
        reclaim_scheduled--;
        clean_regions.push_back(rid);
    }
    ProfileEvents::increment(ProfileEvents::RegionManagerReclaimTimeCount, toMicros(getSteadyClock() - start_time).count());
    ProfileEvents::increment(ProfileEvents::RegionManagerReclaimCount);
}

void RegionManager::doEviction(RegionId rid, BufferView buffer) const
{
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

void RegionManager::flush()
{
    device.flush();
}
}
