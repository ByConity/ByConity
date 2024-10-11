#include <Storages/DiskCache/FiberThread.h>
#include <Storages/DiskCache/Region.h>
#include <Storages/NexusFS/NexusFSBuffer.h>

#include <tuple>

namespace DB::HybridCache
{
bool Region::readyForReclaim(bool wait)
{
    std::unique_lock<TimedMutex> l{lock};
    flags |= kBlockAccess;
    bool ready = false;
    while (!(ready = (activeOpenLocked() == 0UL)) && wait)
    {
        cond.wait(l);
    }

    return ready;
}

UInt32 Region::activeOpenLocked() const
{
    return active_phys_readers + active_in_mem_readers + active_writers;
}

std::tuple<RegionDescriptor, RelAddress> Region::openAndAllocate(UInt32 size)
{
    std::lock_guard g{lock};
    chassert(!(flags & kBlockAccess));
    if (!canAllocateLocked(size))
        return std::make_tuple(RegionDescriptor{OpenStatus::Error}, RelAddress{});
    active_writers++;
    return std::make_tuple(RegionDescriptor::makeWriteDescriptor(OpenStatus::Ready, region_id), allocateLocked(size));
}

RegionDescriptor Region::openForRead()
{
    std::unique_lock<TimedMutex> l{lock};
    if (flags & kBlockAccess)
    {
        if (getCurrentFiberThread())
            cond.wait(l);
        return RegionDescriptor{OpenStatus::Retry};
    }
    bool phys_read_mode = false;
    if (isFlushedLocked() || !buffer)
    {
        phys_read_mode = true;
        active_phys_readers++;
    }
    else
        active_in_mem_readers++;
    return RegionDescriptor::makeReadDescriptor(OpenStatus::Ready, region_id, phys_read_mode);
}

std::unique_ptr<Buffer> Region::detachBuffer()
{
    std::unique_lock<TimedMutex> l{lock};
    chassert(buffer != nullptr);
    while (active_in_mem_readers != 0)
    {
        cond.wait(l);
    }

    chassert(active_writers == 0UL);
    auto ret_buf = std::move(buffer);
    buffer = nullptr;
    return ret_buf;
}

// Flushes the attached buffer if threre are no active writes by calling the callback function that is expected to write the
// buffer to underlying device.
Region::FlushRes Region::flushBuffer(std::function<bool(RelAddress, BufferView)> callback)
{
    std::unique_lock<TimedMutex> ulock{lock};
    if (active_writers != 0)
        return FlushRes::kRetryPendingWrites;
    if (!isFlushedLocked())
    {
        ulock.unlock();
        if (callback(RelAddress{region_id, 0}, buffer->view()))
        {
            ulock.lock();
            flags |= kFlushed;
            return FlushRes::kSuccess;
        }
        return FlushRes::kRetryDeviceFailure;
    }
    return FlushRes::kSuccess;
}

void Region::cleanupBuffer(std::function<void(RegionId, BufferView)> callback)
{
    std::unique_lock<TimedMutex> l{lock};
    while (active_writers != 0)
    {
        cond.wait(l);
    }
    if (!isCleanedupLocked())
    {
        lock.unlock();
        callback(region_id, buffer->view());
        lock.lock();
        flags |= kCleanedup;
    }
}

void Region::reset()
{
    std::lock_guard<TimedMutex> l{lock};
    chassert(activeOpenLocked() == 0U);
    priority = 0;
    flags = 0;
    active_writers = 0;
    active_phys_readers = 0;
    active_in_mem_readers = 0;
    last_entry_end_offset = 0;
    num_items = 0;
    cond.notifyAll();
}

void Region::close(RegionDescriptor && desc)
{
    std::lock_guard<TimedMutex> l{lock};
    switch (desc.getMode())
    {
        case OpenMode::Write:
            chassert(active_writers > 0u);
            if (--active_writers == 0)
                cond.notifyAll();
            break;
        case OpenMode::Read:
            if (desc.isPhysReadMode())
            {
                chassert(active_phys_readers > 0u);
                if (--active_phys_readers == 0)
                    cond.notifyAll();
            }
            else
            {
                chassert(active_in_mem_readers > 0u);
                if (--active_in_mem_readers == 0)
                    cond.notifyAll();
            }
            break;
        default:
            chassert(false);
    }
}

RelAddress Region::allocateLocked(UInt32 size)
{
    chassert(canAllocateLocked(size));
    auto offset = last_entry_end_offset;
    last_entry_end_offset += size;
    num_items++;
    return RelAddress{region_id, offset};
}

void Region::writeToBuffer(UInt32 offset, BufferView buf)
{
    std::lock_guard g{lock};
    chassert(buffer != nullptr);
    auto size = buf.size();
    chassert(offset + size <= buffer->size());
    memcpy(buffer->data() + offset, buf.data(), size);
}

void Region::readFromBuffer(UInt32 from_offset, MutableBufferView out_buf) const
{
    std::lock_guard g{lock};
    chassert(buffer != nullptr);
    chassert(from_offset + out_buf.size() <= buffer->size());
    memcpy(out_buf.data(), buffer->data() + from_offset, out_buf.size());
}

void Region::readFromBuffer(UInt32 from_offset, size_t size, char *to) const
{
    std::lock_guard g{lock};
    chassert(buffer != nullptr);
    chassert(from_offset + size <= buffer->size());
    memcpy(to, buffer->data() + from_offset, size);
}

void Region::addHandle(std::shared_ptr<NexusFSComponents::BlockHandle> &handle)
{
    std::lock_guard g{lock};
    handles.push_back(handle);
}

void Region::resetHandles()
{
    std::lock_guard g{lock};
    for (auto &handle : handles)
        handle->invalidRelAddress();
    handles.clear();
}

void Region::getHandles(std::vector<std::shared_ptr<NexusFSComponents::BlockHandle>> &handles_)
{
    std::lock_guard g{lock};
    handles_ = handles;
}

}
