#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdint.h>

#include <Storages/DiskCache/RegionManager.h>
#include <Storages/DiskCache/Types.h>
#include <folly/fibers/TimedMutex.h>
#include <folly/MPMCQueue.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <common/defines.h>
#include <common/types.h>

namespace DB::NexusFSComponents
{

using Mutex = folly::fibers::TimedMutex;
using SlotId = UInt32;
using HybridCache::RelAddress;
class InodeManager;
class BlockHandle;
class BufferManager;

constexpr UInt32 INVALID_SLOT_ID = UINT32_MAX;

enum class OpResult : UInt16
{
    SUCCESS,
    RETRY,
    DEEP_RETRY,
    ERROR
};

class BufferState
{
public:
    explicit BufferState(uintptr_t buffer_) : handle(nullptr), buffer(buffer_), reader(0), state(State::COLD)
    {
        chassert(buffer != 0);
    }

    void loadAndPin(const std::unique_lock<Mutex> &, std::shared_ptr<BlockHandle> & handle);

    void pin(const std::unique_lock<Mutex> &l);
    void unpin(const std::unique_lock<Mutex> &l);

    bool markCooling(const std::unique_lock<Mutex> &);

    bool tryUnload(const std::unique_lock<Mutex> &);

    std::shared_ptr<BlockHandle> getHandle() const { return handle; }
    uintptr_t getBuffer() const { return buffer; }
    UInt16 getReader() const { return reader; }

    String toString(const std::unique_lock<Mutex> &);

private:
    enum class State : UInt8
    {
        HOT,
        COOLING,
        COLD
    };

    std::shared_ptr<BlockHandle> handle{nullptr};
    const uintptr_t buffer{0};
    UInt16 reader{0};
    State state{State::COLD};
};


class BlockHandle
{
public:
    explicit BlockHandle(RelAddress addr_, UInt32 size_) : addr(addr_), size(size_) { }
    BlockHandle(const BlockHandle &) = delete;
    BlockHandle & operator=(const BlockHandle &) = delete;

    UInt32 getSize() const { return size; }
    RelAddress getRelAddress() const { return addr; }
    bool isRelAddressValid() const { return addr.load().rid().valid(); }
    void invalidRelAddress() { addr.store(RelAddress()); }
    SlotId getBufferSlot() const { return buffer_slot.load(); }
    void resetBufferSlot() { buffer_slot.store(INVALID_SLOT_ID); }

    BufferState * getBuffer(const std::unique_lock<Mutex> & lock);
    BufferState * getBufferStateAndLock(std::unique_lock<Mutex> & lock);

    bool setBufferSlot(SlotId slot_id);

    void unpin();

    String toString();
    String toString(const std::unique_lock<Mutex> &);

private:
    friend class BufferManager;

    std::atomic<RelAddress> addr{RelAddress()};
    const UInt32 size{0};
    std::atomic<SlotId> buffer_slot{INVALID_SLOT_ID};
};


class BufferManager
{
public:
    static std::unique_ptr<BufferManager> buffer_manager;
    static BufferManager * initInstance(
        size_t buffer_size_,
        UInt32 segment_size_,
        UInt32 filemate_gc_interval_,
        double cooling_percentage,
        double freed_percentage,
        HybridCache::RegionManager & region_manager_,
        InodeManager & inode_manager_
    );
    static BufferManager * getInstance();
    static void destroy();

    explicit BufferManager(
        size_t buffer_size_,
        UInt32 segment_size_,
        UInt32 filemate_gc_interval_,
        double cooling_percentage,
        double freed_percentage,
        HybridCache::RegionManager & region_manager_,
        InodeManager & inode_manager_);
    ~BufferManager();
    BufferManager(const BufferManager &) = delete;
    BufferManager & operator=(const BufferManager &) = delete;

    std::pair<OpResult, uintptr_t> pin(std::shared_ptr<BlockHandle> & handle, UInt64 seq_number);

private:
    friend class BlockHandle;

    Mutex & getMetaMutex(SlotId id) 
    {
        chassert(id < slot_size);
        return meta_locks[id];
    }
    BufferState & getMetaState(SlotId id)
    {
        chassert(id < slot_size);
        return meta_states[id];
    }

    uintptr_t calculateBuffer(SlotId slot_id) const { return base_data + static_cast<uintptr_t>(slot_id) * segment_size; }

    std::pair<OpResult, SlotId> alloc();
    void free(SlotId slot_id);

    std::pair<OpResult, uintptr_t> loadAndPin(std::shared_ptr<BlockHandle> & handle, UInt64 seq_number);

    void coolDownBlocksAndGC();

    LoggerPtr log = getLogger("NexusFSBufferManager");

    const size_t buffer_size{};
    const size_t slot_size{};
    const size_t cooling_size{};
    const size_t freed_size{};
    const UInt32 segment_size{};
    const UInt32 filemate_gc_interval{};

    HybridCache::RegionManager & region_manager;
    InodeManager & inode_manager;

    const uintptr_t base_data;

    // TODO: optimize with lock manager
    std::vector<Mutex> meta_locks;
    std::vector<BufferState> meta_states;

    std::queue<SlotId> cooling_queue;
    folly::MPMCQueue<SlotId> free_list;

    std::atomic<bool> stop_cooling_and_gc{false};
    std::thread cooling_and_gc_thread;
};

}
