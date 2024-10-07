#pragma once

#include <Common/Logger.h>
#include <Core/Types.h>
#include <Core/BackgroundSchedulePool.h>
#include <Transaction/LockDefines.h>
#include <Transaction/LockRequest.h>
#include <common/logger_useful.h>

#include <array>
#include <chrono>
#include <unordered_map>
#include <common/singleton.h>
namespace DB
{
class Context;
struct StorageID;

class LockContext
{
public:
    LockContext();
    LockStatus lock(LockRequest * request);
    void unlock(LockRequest * request);
    const LockRequestList & getGrantedList() const { return granted_list; }
    const LockRequestList & getWaitingList() const { return waiting_list; }
    std::vector<TxnTimestamp> getGrantedTxnList() const;
    std::pair<UInt32, UInt32> getModes() const { return {granted_modes, conflicted_modes}; }
    bool empty() const { return !granted_modes && !conflicted_modes; }

private:
    void scheduleWaitingRequests();

    void incGrantedModeCount(LockMode mode);
    void decGrantedModeCount(LockMode mode);
    void incConflictedModeCount(LockMode mode);
    void decConflictedModeCount(LockMode mode);

    void incGrantedTxnCounts(const TxnTimestamp & txn_id);
    void decGrantedTxnCounts(const TxnTimestamp & txn_id);
    bool lockedBySameTxn(const TxnTimestamp & txn_id);


    // Counts the granted lock requests for each lock modes
    std::array<UInt32, LockModeSize> granted_counts;

    // Bit-mask of current granted lock modes
    UInt32 granted_modes;

    // List of granted lock requests
    LockRequestList granted_list;

    std::array<UInt32, LockModeSize> conflicted_counts;
    UInt32 conflicted_modes;
    LockRequestList waiting_list;

    //  Counts the num of granted lock requests for each txn_id
    using TxnID = UInt64;
    std::unordered_map<TxnID, UInt32> granted_txn_counts;
};

template <typename Key, typename T>
struct MapStripe
{
    std::mutex mutex;
    std::unordered_map<Key, T> map;
};

template <typename Key, typename T, typename Hash = std::hash<Key>>
struct StripedMap
{
    static constexpr size_t num_stripes = 16;
    Hash hash_fn;
    std::vector<MapStripe<Key, T>> map_stripes{num_stripes};

    MapStripe<Key, T> & getStripe(const Key & key) { return map_stripes.at(hash_fn(key) % num_stripes); }
};

using LockMapStripe = MapStripe<String, LockContext>;
using LockMap = StripedMap<String, LockContext>;
using LockMaps = std::vector<LockMap>;

class LockManager : public ext::singleton<LockManager>
{
public:
    LockManager();
    LockManager(const LockManager &) = delete;
    LockManager & operator=(const LockManager &) = delete;
    ~LockManager();

    void shutdown();

    LockStatus lock(LockRequest *, const Context & context);
    void unlock(LockRequest *);

    void lock(const LockInfoPtr &, const Context & context);
    void unlock(const LockInfoPtr &);
    void unlock(const TxnTimestamp & txn_id);
    void assertLockAcquired(const TxnTimestamp & txn_id, LockID lock);

    using Clock = std::chrono::steady_clock;
    void updateExpireTime(const TxnTimestamp & txn_id_, Clock::time_point expire_tp);

    // for unit test
    void initialize() { lock_maps = LockMaps(LockLevelSize); }
    void setEvictExpiredLocks(bool enable) { evict_expired_locks = enable; }
    LockMaps & getLockMaps() { return lock_maps; }

    void releaseLocksForTxn(const TxnTimestamp & txn_id, const Context & context);

    void releaseLocksForTable(const StorageID & storage_id, const Context & context);

private:
    bool evict_expired_locks{true};
    LockMaps lock_maps{LockLevelSize};

    using LockIdMap = std::unordered_map<LockID, LockInfoPtr>;

    struct TxnLockInfo
    {
        UInt64 txn_id;
        Clock::time_point expire_time;
        LockIdMap lock_ids;
        /// The topology version passed when acquiring lock
        PairInt64 topology_version;
    };

    using TxnLockMapStripe = MapStripe<UInt64, TxnLockInfo>;
    using TxnLockMap = StripedMap<UInt64, TxnLockInfo>;
    // Maps from txn_id -> TxnLockInfo
    TxnLockMap txn_locks_map;

    ContextPtr global_context;
    BackgroundSchedulePool::TaskHolder txn_checker;

    LoggerPtr log{getLogger("CnchLockManager")};

    std::atomic<bool> is_stopped{false};

    bool isActive() { return !is_stopped; }

    void checkTxnStatus();

    LockInfoPtr getLockInfoPtr(const TxnTimestamp & txn_id, LockID lock_id);

    bool isTxnExpired(const TxnTimestamp & txn_id, const Context & context);

    bool abortTxnAndReleaseLocks(const TxnTimestamp & txn_id, const Context & context);
};

}
