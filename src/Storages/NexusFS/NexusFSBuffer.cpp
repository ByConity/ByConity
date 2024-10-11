#include <Storages/NexusFS/NexusFSBuffer.h>

#include <memory>

#include <Storages/DiskCache/RegionManager.h>
#include <Storages/DiskCache/Types.h>
#include <Storages/NexusFS/NexusFSInodeManager.h>
#include <folly/container/F14Set.h>
#include "Common/ProfileEvents.h"
#include <Common/Exception.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <common/types.h>


namespace ProfileEvents
{
extern const Event NexusFSBufferHit;
extern const Event NexusFSBufferMiss;
extern const Event NexusFSBufferPreload;
extern const Event NexusFSBufferPreloadRetry;
extern const Event NexusFSBufferEmptyCoolingQueue;
extern const Event NexusFSDiskCacheBytesRead;
}

namespace DB::ErrorCodes
{
extern const int CANNOT_OPEN_FILE;
}

namespace DB::NexusFSComponents
{

void BufferState::loadAndPin(const std::unique_lock<Mutex> &, std::shared_ptr<BlockHandle> & handle_)
{
    chassert(state == State::COLD);
    chassert(!handle);
    state = State::HOT;
    reader = 1;
    handle = handle_;
}

void BufferState::pin(const std::unique_lock<Mutex> &)
{
    chassert(handle);
    state = State::HOT;
    reader++;
}

void BufferState::unpin(const std::unique_lock<Mutex> & l)
{
    if (state != BufferState::State::HOT || reader == 0 || !handle)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "try to unpin a {} whose has invalid {}",
            handle ? handle->toStringSimple() : "BlockHandle(nullptr)",
            toString(l));
    reader--;
}

bool BufferState::markCooling(const std::unique_lock<Mutex> &)
{
    if (!handle)
        return false;
    if (state == State::HOT && reader == 0)
    {
        state = State::COOLING;
        return true;
    }
    return false;
}

bool BufferState::tryUnload(const std::unique_lock<Mutex> &)
{
    if (!handle)
        return false;
    if (state == State::COOLING)
    {
        if (reader != 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "try to unload a cooling {} whose reader={}", handle->toStringSimple(), reader);
        state = State::COLD;
        reader = 0;
        handle->resetBufferSlot();
        handle.reset();
        return true;
    }
    return false;
}

String BufferState::toString(const std::unique_lock<Mutex> &)
{
    return fmt::format(
        "BufferState(state={}, reader={}, buffer={}, handle={})",
        static_cast<UInt16>(state),
        reader,
        reinterpret_cast<void *>(buffer),
        reinterpret_cast<void *>(handle.get()));
}


BufferState * BlockHandle::getBuffer(const std::unique_lock<Mutex> & lock)
{
    auto slot_id = buffer_slot.load();
    auto * buffer_manager = BufferManager::getInstance();
    chassert(slot_id != INVALID_SLOT_ID);
    chassert(buffer_manager);
    chassert(&buffer_manager->getMetaMutex(slot_id) == lock.mutex());
    return &buffer_manager->getMetaState(slot_id);
}

BufferState * BlockHandle::getBufferStateAndLock(std::unique_lock<Mutex> & lock)
{
    auto slot_id = buffer_slot.load();
    while (true)
    {
        if (slot_id == INVALID_SLOT_ID)
            return nullptr;
        auto * buffer_manager = BufferManager::getInstance();
        chassert(buffer_manager);
        lock = std::unique_lock{buffer_manager->getMetaMutex(slot_id)};
        auto recheck_slot_id = buffer_slot.load();
        if (recheck_slot_id == slot_id)
            return &buffer_manager->getMetaState(slot_id);
        else
        {
            lock.unlock();
            lock.release();
            slot_id = recheck_slot_id;
        }
    }
}

bool BlockHandle::setBufferSlot(SlotId slot_id)
{
    auto expected = INVALID_SLOT_ID;
    return buffer_slot.compare_exchange_strong(expected, slot_id);
}


void BlockHandle::unpin()
{
    std::unique_lock<Mutex> lock;
    auto * state = getBufferStateAndLock(lock);
    chassert(state);
    state->unpin(lock);
}

String BlockHandle::toString()
{
    std::unique_lock<Mutex> lock;
    auto * state = getBufferStateAndLock(lock);
    if (state)
    {
        auto laddr = addr.load();
        return fmt::format(
            "BlockHandle({}, state={}, valid={}, addr=<{},{}>, size={})",
            reinterpret_cast<void *>(this),
            state->toString(lock),
            laddr.rid().valid(),
            laddr.rid().index(),
            laddr.offset(),
            size);
    }
    else
    {
        auto laddr = addr.load();
        return fmt::format(
            "BlockHandle({}, state=null, valid={}, addr=<{},{}>, size={})",
            reinterpret_cast<void *>(this),
            laddr.rid().valid(),
            laddr.rid().index(),
            laddr.offset(),
            size);
    }
}

String BlockHandle::toString(const std::unique_lock<Mutex> & lock)
{
    auto * state = getBuffer(lock);
    auto laddr = addr.load();
    return fmt::format(
        "BlockHandle({}, state={}, valid={}, addr=<{},{}>, size={})",
        reinterpret_cast<void *>(this),
        state->toString(lock),
        laddr.rid().valid(),
        laddr.rid().index(),
        laddr.offset(),
        size);
}

String BlockHandle::toStringSimple() const
{
    auto laddr = addr.load();
    return fmt::format(
        "BlockHandle({}, buffer_slot={}, valid={}, addr=<{},{}>, size={})",
        reinterpret_cast<const void *>(this),
        buffer_slot.load(),
        laddr.rid().valid(),
        laddr.rid().index(),
        laddr.offset(),
        size);
}


std::unique_ptr<BufferManager> BufferManager::buffer_manager = nullptr;

BufferManager * BufferManager::initInstance(
    size_t buffer_size_,
    UInt32 segment_size_,
    UInt32 filemate_gc_interval_,
    double cooling_percentage,
    double freed_percentage,
    HybridCache::RegionManager & region_manager_,
    InodeManager & inode_manager_)
{
    if (buffer_manager)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BufferManager already initialized");
    buffer_manager = std::make_unique<BufferManager>(
        buffer_size_, segment_size_, filemate_gc_interval_, cooling_percentage, freed_percentage, region_manager_, inode_manager_);
    return buffer_manager.get();
}

BufferManager * BufferManager::getInstance()
{
    return buffer_manager.get();
}

BufferManager::BufferManager(
    size_t buffer_size_,
    UInt32 segment_size_,
    UInt32 filemate_gc_interval_,
    double cooling_percentage,
    double freed_percentage,
    HybridCache::RegionManager & region_manager_,
    InodeManager & inode_manager_)
    : buffer_size(buffer_size_)
    , slot_size(buffer_size_ / segment_size_)
    , cooling_size(slot_size * cooling_percentage)
    , freed_size(slot_size * freed_percentage)
    , segment_size(segment_size_)
    , filemate_gc_interval(filemate_gc_interval_)
    , region_manager(region_manager_)
    , inode_manager(inode_manager_)
    , base_data(reinterpret_cast<uintptr_t>(mmap(nullptr, buffer_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0)))
    , meta_locks(slot_size)
    , free_list(folly::MPMCQueue<SlotId>(slot_size))
{
    chassert(buffer_size_ % segment_size_ == 0);
    chassert(base_data);
    chassert(base_data % 4096 == 0);

    meta_states.reserve(slot_size);
    for (size_t i = 0; i < slot_size; i++)
        meta_states.emplace_back(BufferState(calculateBuffer(i)));

    for (SlotId i = 0; i < slot_size; i++)
        free_list.write(i);

    cooling_and_gc_thread = std::thread([this] { coolDownBlocksAndGC(); });
}

BufferManager::~BufferManager()
{
    stop_cooling_and_gc.store(true, std::memory_order_relaxed);
    cooling_and_gc_thread.join();

    chassert(base_data);
    munmap(reinterpret_cast<void *>(base_data), buffer_size);
}

std::pair<OpResult, uintptr_t> BufferManager::pin(std::shared_ptr<BlockHandle> & handle, UInt64 seq_number)
{
    std::unique_lock<Mutex> lock;
    auto * state = handle->getBufferStateAndLock(lock);
    if (!state)
    {
        ProfileEvents::increment(ProfileEvents::NexusFSBufferMiss);
        return loadAndPin(handle, seq_number);
    }

    ProfileEvents::increment(ProfileEvents::NexusFSBufferHit);
    state->pin(lock);
    auto buffer = state->getBuffer();
    chassert(buffer != 0);
    return {OpResult::SUCCESS, buffer};
}

std::pair<OpResult, SlotId> BufferManager::alloc()
{
    SlotId id;
    if (!free_list.read(id))
    {
        ProfileEvents::increment(ProfileEvents::NexusFSBufferEmptyCoolingQueue);
        std::this_thread::yield();
        return {OpResult::RETRY, 0};
    }
    LOG_TRACE(log, "erase slot {} from free_list", id);
    return {OpResult::SUCCESS, id};
}

void BufferManager::free(SlotId slot_id)
{
    free_list.write(slot_id);
    LOG_TRACE(log, "insert slot {} to free_list", slot_id);
}

std::pair<OpResult, uintptr_t> BufferManager::loadAndPin(std::shared_ptr<BlockHandle> & handle, const UInt64 seq_number)
{
    if (!handle->isRelAddressValid())
        return {OpResult::DEEP_RETRY, 0};

    auto [op_result, slot_id] = alloc();
    if (op_result != OpResult::SUCCESS)
        return {op_result, 0};

    std::unique_lock<Mutex> l(meta_locks[slot_id]);
    if (!handle->setBufferSlot(slot_id))
    {
        LOG_TRACE(
            log,
            "try to set slot {} to BlockHandle({}, slot={}), but failed",
            slot_id,
            reinterpret_cast<void *>(handle.get()),
            handle->getBufferSlot());
        free(slot_id);
        return {OpResult::DEEP_RETRY, 0};
    }

    RelAddress addr = handle->getRelAddress();
    size_t size = handle->getSize();
    chassert(addr.rid().valid());
    chassert(size > 0);

    auto desc = region_manager.openForRead(addr.rid(), seq_number);
    if (desc.getStatus() != HybridCache::OpenStatus::Ready)
    {
        handle->resetBufferSlot();
        free(slot_id);
        if (desc.getStatus() == HybridCache::OpenStatus::Retry)
            return {OpResult::DEEP_RETRY, 0};
        if (desc.getStatus() == HybridCache::OpenStatus::Error)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "fail to open region for read");
    }

    auto & state = getMetaState(slot_id);
    chassert(!state.getHandle());
    uintptr_t buffer = state.getBuffer();
    chassert(buffer);
    size_t bytes_read = region_manager.read(desc, addr, size, reinterpret_cast<char *>(buffer));
    chassert(size == bytes_read);
    LOG_TRACE(
        log,
        "read {} bytes from disk(rid={}, offset={}, size={}) to buffer {}(slot={})",
        bytes_read,
        addr.rid().index(),
        addr.offset(),
        size,
        reinterpret_cast<void *>(buffer),
        slot_id);
    ProfileEvents::increment(ProfileEvents::NexusFSDiskCacheBytesRead, size);

    region_manager.touch(addr.rid());
    region_manager.close(std::move(desc));

    state.loadAndPin(l, handle);

    LOG_TRACE(log, "{} loadAndPin, buffer {}(slot={})", handle->toString(l), reinterpret_cast<void *>(buffer), slot_id);

    return {OpResult::SUCCESS, buffer};
}

void BufferManager::coolDownBlocksAndGC()
{
    SlotId cooling_itr = 0;
    Stopwatch watch;
    while (!stop_cooling_and_gc.load(std::memory_order_relaxed))
    {
        size_t current_freed = free_list.size();
        if (current_freed >= freed_size)
        {
            if (watch.elapsedSeconds() >= filemate_gc_interval)
            {
                watch.restart();
                inode_manager.cleanInvalidFiles();
            }
            else
                std::this_thread::yield();
            continue;
        }

        while (cooling_queue.size() < cooling_size)
        {
            std::unique_lock<Mutex> l(meta_locks[cooling_itr]);
            if (meta_states[cooling_itr].markCooling(l))
            {
                auto handle = meta_states[cooling_itr].getHandle();
                chassert(handle);
                LOG_TRACE(log, "{} on slot {} turns cooling", handle->toString(l), cooling_itr);
                cooling_queue.push(cooling_itr);
            }
            cooling_itr = (cooling_itr + 1) % slot_size;
        }

        while (current_freed < freed_size && !cooling_queue.empty())
        {
            SlotId id = cooling_queue.front();
            cooling_queue.pop();

            std::unique_lock<Mutex> l(meta_locks[id]);
            auto handle = meta_states[id].getHandle();
            if (meta_states[id].tryUnload(l))
            {
                LOG_TRACE(
                    log,
                    "BlockHandle({}) unloaded, {}(slot={}) retrived",
                    reinterpret_cast<void *>(handle.get()),
                    meta_states[id].toString(l),
                    id);
                free(id);
                current_freed++;
            }
        }
    }
}

}
