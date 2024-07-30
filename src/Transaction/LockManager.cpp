#include <Transaction/LockManager.h>
#include <Catalog/Catalog.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Transaction/LockDefines.h>
#include <Transaction/LockRequest.h>
#include <Transaction/TransactionCommon.h>
#include <common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>

#include <chrono>
#include <cassert>
#include <unordered_set>


namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CNCH_TRANSACTION_INTERNAL_ERROR;
}

LockContext::LockContext() : granted_counts{}, granted_modes(0), conflicted_counts{}, conflicted_modes(0)
{}

LockStatus LockContext::lock(LockRequest * request)
{
    bool granted = lockedBySameTxn(request->getTransactionID()) || (!conflicts(request->getMode(), granted_modes) && waiting_list.empty());

    if (granted)
    {
        incGrantedModeCount(request->getMode());
        incGrantedTxnCounts(request->getTransactionID());
        auto pos = granted_list.insert(granted_list.end(), request);
        request->setLockResult(LockStatus::LOCK_OK, pos);
        return LockStatus::LOCK_OK;
    }
    else
    {
        if (request->noWait())
        {
            request->setLockResult(LockStatus::LOCK_TIMEOUT, waiting_list.end());
            return LockStatus::LOCK_TIMEOUT;
        }
        else
        {
            incConflictedModeCount(request->getMode());
            auto pos = waiting_list.insert(waiting_list.end(), request);
            request->setLockResult(LockStatus::LOCK_WAITING, pos);
            return LockStatus::LOCK_WAITING;
        }
    }
}

void LockContext::unlock(LockRequest * request)
{
    LockStatus status = request->getStatus();
    if (status == LockStatus::LOCK_OK)
    {
        granted_list.erase(request->getRequestItor());
        decGrantedModeCount(request->getMode());
        decGrantedTxnCounts(request->getTransactionID());
        scheduleWaitingRequests();
        request->setLockResult(LockStatus::LOCK_INIT, granted_list.end());
    }
    else if (status == LockStatus::LOCK_WAITING)
    {
        waiting_list.erase(request->getRequestItor());
        decConflictedModeCount(request->getMode());
        request->setLockResult(LockStatus::LOCK_CANCELLED, waiting_list.end());
        request->notify();
    }

    /// ignore other cases;
}

std::vector<TxnTimestamp> LockContext::getGrantedTxnList() const
{
    std::vector<TxnTimestamp> res;
    res.reserve(granted_txn_counts.size());
    for (const auto & it : granted_txn_counts)
    {
        res.push_back(it.first);
    }

    return res;
}

void LockContext::scheduleWaitingRequests()
{
    auto it = waiting_list.begin();
    while (it != waiting_list.end())
    {
        LockRequest * request = *it;
        if (conflicts(request->getMode(), granted_modes))
        {
            ++it;
            continue;
        }

        it = waiting_list.erase(it);
        decConflictedModeCount(request->getMode());
        incGrantedModeCount(request->getMode());
        incGrantedTxnCounts(request->getTransactionID());
        auto pos = granted_list.insert(granted_list.end(), request);
        request->setLockResult(LockStatus::LOCK_OK, pos);
        request->notify();
    }
}

void LockContext::incGrantedModeCount(LockMode mode)
{
    ++granted_counts[to_underlying(mode)];
    if (granted_counts[to_underlying(mode)] == 1)
    {
        assert((granted_modes & modeMask(mode)) == 0);
        granted_modes |= modeMask(mode);
    }
}

void LockContext::decGrantedModeCount(LockMode mode)
{
    assert(granted_counts[to_underlying(mode)] > 0);
    --granted_counts[to_underlying(mode)];
    if (granted_counts[to_underlying(mode)] == 0)
    {
        assert((granted_modes & modeMask(mode)) == modeMask(mode));
        granted_modes &= ~(modeMask(mode));
    }
}

void LockContext::incConflictedModeCount(LockMode mode)
{
    ++conflicted_counts[to_underlying(mode)];
    if (conflicted_counts[to_underlying(mode)] == 1)
    {
        assert((conflicted_modes & modeMask(mode)) == 0);
        conflicted_modes |= modeMask(mode);
    }
}

void LockContext::decConflictedModeCount(LockMode mode)
{
    assert(conflicted_counts[to_underlying(mode)] > 0);
    --conflicted_counts[to_underlying(mode)];
    if (conflicted_counts[to_underlying(mode)] == 0)
    {
        assert((conflicted_modes & modeMask(mode)) == modeMask(mode));
        conflicted_modes &= ~(modeMask(mode));
    }
}

void LockContext::incGrantedTxnCounts(const TxnTimestamp & txn_id)
{
    granted_txn_counts[txn_id.toUInt64()]++;
}

void LockContext::decGrantedTxnCounts(const TxnTimestamp & txn_id)
{
    if (auto it = granted_txn_counts.find(txn_id.toUInt64()); it != granted_txn_counts.end())
    {
        if ((--it->second) == 0)
            granted_txn_counts.erase(it);
    }
    else
        assert(0);
}

bool LockContext::lockedBySameTxn(const TxnTimestamp & txn_id)
{
    return granted_txn_counts.find(txn_id.toUInt64()) != granted_txn_counts.end();
}

LockManager::LockManager(): global_context(Context::getGlobalContextInstance())
{
    txn_checker = global_context->getSchedulePool().createTask("LockManagerTxnChecker", [this] { checkTxnStatus(); });
    txn_checker->activateAndSchedule();
}

LockManager::~LockManager()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void LockManager::shutdown()
{
    if (!isActive())
        return;

    if (txn_checker)
        txn_checker->deactivate();

    is_stopped = true;
}

LockStatus LockManager::lock(LockRequest * request, const Context & context)
{
    LOG_TRACE(log, "lock request: " + request->toDebugString());

    auto level = to_underlying(request->getLevel());
    std::vector<TxnTimestamp> current_granted_txns;
    LockMapStripe & stripe = lock_maps[level].getStripe(request->getEntity());
    {
        std::lock_guard lock(stripe.mutex);
        // construct lock context if not exists
        auto it = stripe.map.find(request->getEntity());
        if (it == stripe.map.end())
        {
            it = stripe.map.try_emplace(request->getEntity()).first;
        }
        LockContext & lock_context = it->second;
        lock_context.lock(request);

        // If lock request needs to wait, check if there are expired txn
        if (evict_expired_locks && request->getStatus() != LockStatus::LOCK_OK)
        {
            current_granted_txns = lock_context.getGrantedTxnList();
        }
    }

    // clear expired txn and retry lock request
    if (evict_expired_locks && !current_granted_txns.empty())
    {
        for (const auto & txn_id : current_granted_txns)
            isTxnExpired(txn_id, context);
    }

    return request->getStatus();
}

void LockManager::unlock(LockRequest * request)
{
    LOG_TRACE(log, "unlock request: " + request->toDebugString());
    auto level = to_underlying(request->getLevel());
    LockMapStripe & stripe = lock_maps[level].getStripe(request->getEntity());
    {
        std::lock_guard lock(stripe.mutex);
        if (auto it = stripe.map.find(request->getEntity()); it != stripe.map.end())
        {
            it->second.unlock(request);

            // remove LockContext if it's empty
            if (it->second.empty())
                stripe.map.erase(it);
        }
    }
}

void LockManager::lock(const LockInfoPtr & info, const Context & context)
{
    /// set and verify topology_version to avoid parallel lock holding
    auto [current_topology_version, current_topology] = global_context->getCnchTopologyMaster()->getCurrentTopologyVersion();
    info->setTopologyVersion(current_topology_version);

    LOG_TRACE(log, "try lock: " + info->toDebugString());
    assert(info->lock_id != 0);
    // register transaction in LockManager
    UInt64 txn_id = UInt64(info->txn_id);
    /// ensure lock request is generated before put into map
    const auto & requests = info->getLockRequests();
    TxnLockMapStripe & stripe = txn_locks_map.getStripe(txn_id);
    {
        // update txn_locks_map, which is a map of txn_id -> lock_ids;
        std::lock_guard lock(stripe.mutex);
        auto expire_tp = Clock::now()
            + std::chrono::milliseconds(context.getSettingsRef().cnch_txn_lock_expire_duration_seconds.value.totalMilliseconds());
        if (auto it = stripe.map.find(txn_id); it != stripe.map.end())
        {
            auto & txn_locks_info = it->second;
            txn_locks_info.expire_time = expire_tp;
            txn_locks_info.lock_ids.emplace(info->lock_id, info);
            txn_locks_info.topology_version = info->topology_version;
        }
        else
        {
            stripe.map.emplace(txn_id, TxnLockInfo{txn_id, expire_tp, LockIdMap{{info->lock_id, info}}, info->topology_version});
        }
    }

    // TODO: fix here type conversion
    Int64 remain_wait_time = info->timeout;
    Stopwatch watch;
    for (const auto & request : requests)
    {
        request->setTimeout(remain_wait_time);
        bool lock_ok = request->lock(context);
        remain_wait_time -= watch.elapsedMilliseconds();
        if (!lock_ok || remain_wait_time < 0)
        {
            info->status = LockStatus::LOCK_TIMEOUT;
            // unlock all previous acquired lock requests
            unlock(info);
            return;
        }
    }

    info->status = LockStatus::LOCK_OK;
}

void LockManager::unlock(const LockInfoPtr & info)
{
    LOG_TRACE(log, "unlock: " + info->toDebugString());

    const UInt64 txn_id = info->txn_id.toUInt64();
    const LockID lock_id = info->lock_id;

    LockInfoPtr stored_info = getLockInfoPtr(info->txn_id, info->lock_id);
    if (!stored_info)
    {
        LOG_WARNING(log, "Unlock a nonexistent lock. lock id: {}, txn_id: {}\n", toString(info->lock_id),toString(txn_id));
        return;
    }
    else
    {
        TxnLockMapStripe & stripe = txn_locks_map.getStripe(txn_id);
        std::lock_guard lock(stripe.mutex);
        if (auto it = stripe.map.find(txn_id); it != stripe.map.end())
        {
            // erase lock id
            auto & txn_locks_info = it->second;
            txn_locks_info.lock_ids.erase(lock_id);

            if (txn_locks_info.lock_ids.empty())
            {
                stripe.map.erase(it);
            }
        }
    }

    const auto & requests = stored_info->getLockRequests();
    for (const auto & request : requests)
    {
        request->unlock();
    }

    /// XXX: If a topo switch occurs during the commit phase, it may lead to parallel lock holding.
    /// While this problem is difficult to solve because committed transactions are not supported to be rolled back. Temporarily use the time window of topo switching to avoid this problem
    /// Potential logs need to be printed
    auto current_topology_server = global_context->getCnchTopologyMaster()->getTargetServer(stored_info->table_uuid_with_prefix, DEFAULT_SERVER_VW_NAME, false);
    auto record = global_context->getCnchCatalog()->tryGetTransactionRecord(txn_id);
    if (record && record->status() == CnchTransactionStatus::Finished)
    {
        if (current_topology_server.topology_version != PairInt64{0, 0} && stored_info->topology_version != current_topology_server.topology_version)
        {
            bool is_local_server = isLocalServer(current_topology_server.getRPCAddress(), std::to_string(global_context->getRPCPort()));
            if (!is_local_server || stored_info->topology_version.high + 1 != current_topology_server.topology_version.high)
                LOG_WARNING(log, "Txn {} of table {} has been committed, but topo check is failed, reason {}, it may cause parallel lock holding", txn_id, stored_info->table_uuid_with_prefix,
                    !is_local_server ? "host server switched" : "topology_version can not be used");
        }
    }
}

void LockManager::unlock(const TxnTimestamp & txn_id_)
{
    UInt64 txn_id = txn_id_;
    LockIdMap lock_infos;

    // deregister transaction
    TxnLockMapStripe & stripe = txn_locks_map.getStripe(txn_id);
    {
        std::lock_guard lock(stripe.mutex);
        if (auto it = stripe.map.find(txn_id); it != stripe.map.end())
        {
            lock_infos = std::move(it->second.lock_ids);
            stripe.map.erase(it);
        }
    }

    for (const auto & [lock_id, info] : lock_infos)
    {
        const auto & requests = info->getLockRequests();
        for (const auto & request : requests)
        {
            request->unlock();
        }
    }
}

void LockManager::assertLockAcquired(const TxnTimestamp & txn_id, LockID lock_id)
{
    /// set and verify topology_version to avoid parallel lock holding
    auto [current_topology_version, current_topology] = global_context->getCnchTopologyMaster()->getCurrentTopologyVersion();

    TxnLockMapStripe & stripe = txn_locks_map.getStripe(txn_id);
    {
        std::lock_guard lock(stripe.mutex);
        if (auto it = stripe.map.find(txn_id); it != stripe.map.end())
        {
            auto & lock_id_map = it->second.lock_ids;
            if (lock_id_map.find(lock_id) == lock_id_map.end())
                throw Exception(fmt::format("lock assertion failed, lock id {} not found ", lock_id), ErrorCodes::CNCH_TRANSACTION_INTERNAL_ERROR);

            if (current_topology_version != PairInt64{0, 0} && it->second.topology_version != current_topology_version)
            {
                LOG_DEBUG(log, "txn id: {}, lock info's topology_version initialtime: {}, term: {}, current topology_version initialtime: {}, term: {}",
                    txn_id, it->second.topology_version.low, it->second.topology_version.high, current_topology_version.low, current_topology_version.high);
                /// do nothing if host server doesn't change in two consecutive topologies
                if (it->second.topology_version.high + 1 == current_topology_version.high)
                    return;
                else
                    throw Exception(fmt::format("lock assertion failed, topology_version can not be used, txn: {}", txn_id), ErrorCodes::CNCH_TRANSACTION_INTERNAL_ERROR);
            }
        }
        else
        {
            throw Exception(fmt::format("lock assertion failed, txn {} not found ", txn_id), ErrorCodes::CNCH_TRANSACTION_INTERNAL_ERROR);
        }
    }
}
void LockManager::updateExpireTime(const TxnTimestamp & txn_id_, Clock::time_point tp)
{
    UInt64 txn_id = UInt64(txn_id_);
    TxnLockMapStripe & stripe = txn_locks_map.getStripe(txn_id);
    {
        std::lock_guard lock(stripe.mutex);
        if (auto it = stripe.map.find(txn_id); it != stripe.map.end())
        {
            LOG_DEBUG(log, "Update txn expire time {}", txn_id);
            it->second.expire_time = tp;
        }
    }
}

void LockManager::checkTxnStatus()
{
    try
    {
        LOG_TRACE(log, "Begin to check transaction status");
        if (evict_expired_locks)
        {
            std::unordered_set<UInt64> txns_to_release;
            for (size_t i = 0; i < txn_locks_map.num_stripes; ++i)
            {
                TxnLockMapStripe & stripe = txn_locks_map.map_stripes.at(i);
                std::lock_guard lock(stripe.mutex);
                for (const auto & [txn_id, txn_lock_info] : stripe.map)
                {
                    if (txn_lock_info.expire_time < Clock::now())
                        txns_to_release.emplace(txn_id);
                }
            }
            if (!txns_to_release.empty())
            {
                LOG_DEBUG(
                    log,
                    "Txn checker found that {} transactions are expired. Will abort it and evict its locks from lock manager",
                    txns_to_release.size());
                for (const auto & txn_id : txns_to_release)
                {
                    LOG_DEBUG(log, "Release locks for txn {}", txn_id);
                    if (!abortTxnAndReleaseLocks(txn_id, *global_context))
                        LOG_WARNING(log, "Fail to evict txn {}", txn_id);
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    txn_checker->scheduleAfter(global_context->getSettingsRef().cnch_lock_manager_txn_checker_schedule_seconds.value.totalMilliseconds());
}

LockInfoPtr LockManager::getLockInfoPtr(const TxnTimestamp & txn_id, LockID lock_id)
{
    TxnLockMapStripe & stripe = txn_locks_map.getStripe(txn_id);
    // lookup stored lock info by (txn_id, lock_id)
    {
        std::lock_guard lock(stripe.mutex);
        if (auto it = stripe.map.find(txn_id); it != stripe.map.end())
        {
            auto & lock_id_map = it->second.lock_ids;
            if (auto it_inner = lock_id_map.find(lock_id); it_inner != lock_id_map.end())
                return it_inner->second;
        }
    }

    return {};
}

void LockManager::releaseLocksForTxn(const TxnTimestamp & txn_id, const Context & context)
{
    LOG_DEBUG(log, "Try to abort txn {} and release its locks from lock manager", txn_id);

    TxnLockMapStripe & stripe = txn_locks_map.getStripe(txn_id);
    {
        std::lock_guard lock(stripe.mutex);
        if (auto it = stripe.map.find(txn_id); it == stripe.map.end())
        {
            LOG_DEBUG(log, "Txn {} doesn't exist, do nothing", txn_id);
            return;
        }
    }

    if (!abortTxnAndReleaseLocks(txn_id, context))
        LOG_WARNING(log, "Fail to abort txn {}", txn_id);
}

void LockManager::releaseLocksForTable(const StorageID & storage_id, const Context & context)
{
    String target_table_prefix = UUIDHelpers::UUIDToString(storage_id.uuid);
    std::unordered_set<UInt64> txns_to_release;
    for (LockMap & lock_map : lock_maps)
    {
        for (LockMapStripe & stripe : lock_map.map_stripes)
        {
            std::lock_guard lock(stripe.mutex);
            for (const auto & [key, lock_context] : stripe.map)
            {
                Protos::DataModelLockField lock_key_model;
                lock_key_model.ParseFromString(key);
                String table_prefix = lock_key_model.table_prefix();
                if (table_prefix != target_table_prefix)
                    continue;

                for (const auto & granted_req : lock_context.getGrantedList())
                    txns_to_release.emplace(granted_req->getTransactionID().toUInt64());

                for (const auto & waiting_req : lock_context.getWaitingList())
                    txns_to_release.emplace(waiting_req->getTransactionID().toUInt64());
            }
        }
    }

    LOG_DEBUG(log, "Release locks for table {}, need release {} txn", storage_id.getNameForLogs(), txns_to_release.size());
    for (const auto & txn_id: txns_to_release)
    {
        LOG_DEBUG(log, "Release locks for txn {}", txn_id);
        releaseLocksForTxn(txn_id, context);
    }
}

bool LockManager::abortTxnAndReleaseLocks(const TxnTimestamp & txn_id, const Context & context)
{
    // Abort transaction and steal transaction's lock
    if (auto catalog = context.tryGetCnchCatalog(); catalog)
    {
        auto txn_record = catalog->tryGetTransactionRecord(txn_id);
        if (txn_record && !txn_record->ended())
        {
            TransactionRecord target_record = txn_record.value();
            target_record.setStatus(CnchTransactionStatus::Aborted).setCommitTs(context.getTimestamp());
            bool success = catalog->setTransactionRecord(txn_record.value(), target_record);
            // If abort txn successfully, clear locks belonging to this txn
            if (success)
                unlock(txn_id);
            else
                return false;
        }
        else
        {
            // txn record not exists
            unlock(txn_id);
        }
    }
    else
    {
        // Catalog is not initialized, for unit test
        unlock(txn_id);
    }
    return true;
}

bool LockManager::isTxnExpired(const TxnTimestamp & txn_id, const Context & context)
{
    bool expired = false;
    TxnLockMapStripe & stripe = txn_locks_map.getStripe(txn_id);
    {
        std::lock_guard lock(stripe.mutex);
        if (auto it = stripe.map.find(txn_id); it != stripe.map.end())
        {
            expired = (it->second.expire_time < Clock::now());
        }
        else
        {
            expired = true;
        }
    }

    if (expired)
    {
        LOG_DEBUG(log, "Txn {} is expired. Will abort it and evict its locks from lock manager", txn_id);
        if (!abortTxnAndReleaseLocks(txn_id, context))
        {
            LOG_WARNING(log, "Fail to evict txn {}", txn_id);
            return false;
        }
    }
    return expired;
}
}
