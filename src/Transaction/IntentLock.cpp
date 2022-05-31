#include "IntentLock.h"

#include <Catalog/Catalog.h>
#include "Common/randomSeed.h"
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
// #include <MergeTreeCommon/CnchServerClientPool.h>
#include <Transaction/TimestampCacheManager.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>

namespace ProfileEvents
{
    extern const Event IntentLockAcquiredAttempt;
    extern const Event IntentLockAcquiredSuccess;
    extern const Event IntentLockAcquiredElapsedMilliseconds;
    extern const Event IntentLockWriteIntentAttempt;
    extern const Event IntentLockWriteIntentSuccess;
    extern const Event IntentLockWriteIntentConflict;
    extern const Event IntentLockWriteIntentPreemption;
    extern const Event IntentLockWriteIntentElapsedMilliseconds;
    extern const Event TsCacheCheckSuccess;
    extern const Event TsCacheCheckFailed;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CNCH_TRANSACTION_TSCACHE_CHECK_FAILED;
    extern const int CONCURRENCY_NOT_ALLOWED_FOR_DDL;
}

static void doRetry(std::function<void()> action, size_t retry)
{
    // ExceptionHandler handler;
    pcg64 rng(randomSeed());

    while (retry--)
    {
        try
        {
            action();
            return;
        }
        catch (...)
        {
            // handler.setException(std::current_exception());
            if (retry)
            {
                auto duration = std::chrono::milliseconds(std::uniform_int_distribution<Int64>(0, 1000)(rng));
                std::this_thread::sleep_for(duration);
            }
            else throw;
        }
    }

    // beyond retry count, throw exception.
    // handler.throwIfException();
}

bool IntentLock::tryLock()
{
    try
    {
        doRetry([this] { lockImpl(); }, lock_retry);
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return locked;
}

void IntentLock::lock()
{
    doRetry([this] { lockImpl(); }, lock_retry);
}

void IntentLock::writeIntents()
{
    if (locked)
        throw Exception("IntentLock is already locked", ErrorCodes::LOGICAL_ERROR);

    ProfileEvents::increment(ProfileEvents::IntentLockWriteIntentAttempt);
    Stopwatch watch;

    auto catalog = context.getCnchCatalog();
    auto intents = createWriteIntents();
    std::map<std::pair<TxnTimestamp, String>, std::vector<String>> conflict_parts;
    bool lock_success = catalog->writeIntents(intent_prefix, intents, conflict_parts);
    watch.stop();

    if (lock_success)
    {
        locked = true;
        ProfileEvents::increment(ProfileEvents::IntentLockWriteIntentSuccess);
        ProfileEvents::increment(ProfileEvents::IntentLockAcquiredSuccess);
        LOG_DEBUG(log, "IntentLock " + intent_prefix + "-" + lock_entity.getNameForLogs() + " is created");
    }
    else
    {
        for (const auto & txn : conflict_parts)
        {
            const auto & [txn_id, location] = txn.first;

            // Locked by current transaction
            if (txn_id == txn_record.txnID())
            {
                Strings locked_intents = txn.second;
                std::sort(locked_intents.begin(), locked_intents.end());
                intent_names.erase(
                    std::remove_if(
                        intent_names.begin(),
                        intent_names.end(),
                        [&](String intent_name) { return std::binary_search(locked_intents.begin(), locked_intents.end(), intent_name); }),
                    intent_names.end());

                if (intent_names.empty())
                {
                    valid = false;
                    locked = true;
                    LOG_DEBUG(log, "IntentLock " + intent_prefix + "-" + lock_entity.getNameForLogs() + " is invalid");
                    return;
                }

                continue;
            }

            auto record = catalog->tryGetTransactionRecord(txn_id);
            if (!record.has_value() || record->status() == CnchTransactionStatus::Running)
            {
                CnchTransactionStatus txn_status{};

                if (location == txn_record.location())
                {
                    // get transaction status from local server
                    auto & coordinator = context.getCnchTransactionCoordinator();
                    txn_status = coordinator.getTransactionStatus(txn_id);
                }
                else
                {
                    // get transaction status from remote server
                    try
                    {
                        // auto server_client = context.getCnchServerClientPool().get(location);
                        // txn_status = server_client->getTransactionStatus(txn_id);
                    }
                    catch (...)
                    {
                        LOG_WARNING(log, "Unable to get transaction status from " + location);
                        txn_status = CnchTransactionStatus::Aborted;
                    }
                }

                if (txn_status == CnchTransactionStatus::Running)
                {
                    // preempt low prioity txn
                    if (record && record->priority() < txn_record.priority())
                    {
                        TransactionRecord target_record;
                        target_record.setStatus(CnchTransactionStatus::Aborted);
                        target_record.setCommitTs(context.getTimestamp());

                        bool success = catalog->setTransactionRecord(*record, target_record);
                        if (success)
                        {
                            LOG_DEBUG(log, "Will preempt a low-priority transaction (txn_id: " + txn_id.toString() + ")");
                        }
                        else
                            throw Exception(
                                "Fail to preempt the transaction (txn_id: " + txn_id.toString() + ")",
                                ErrorCodes::CONCURRENCY_NOT_ALLOWED_FOR_DDL);
                    }
                    else
                    {
                        ProfileEvents::increment(ProfileEvents::IntentLockWriteIntentConflict);
                        throw Exception(
                            "Current transaction is conflicted with other txn (txn_id: " + txn_id.toString() + ")",
                            ErrorCodes::CONCURRENCY_NOT_ALLOWED_FOR_DDL);
                    }
                }
                else if (record && txn_status == CnchTransactionStatus::Inactive)
                {
                    try
                    {
                        record->setCommitTs(context.getTimestamp());
                        catalog->rollbackTransaction(*record);
                    }
                    catch (...)
                    {
                        tryLogCurrentException(log, "Failed to set txn status for inactive transaction " + txn_id.toString());
                    }
                }
            }

            // reset conflicted intents
            std::vector<WriteIntent> old_intents;
            for (const auto & old_part : txn.second)
            {
                old_intents.emplace_back(txn_id, location, old_part);
                LOG_DEBUG(log, "Will reset old intent: {} {} , txn id {}\n", intent_prefix, old_part, txn_id);
            }

            watch.restart();

            if (!catalog->tryResetIntents(intent_prefix, old_intents, txn_record.txnID(), txn_record.location()))
                throw Exception(
                    "Cannot reset intents for conflicted txn. The lock might be acquired by another transaction now. conflicted txn_id: " + txn_id.toString()
                        + ". Current txn_id: " + txn_record.txnID().toString(),
                    ErrorCodes::CONCURRENCY_NOT_ALLOWED_FOR_DDL);
            else
                ProfileEvents::increment(ProfileEvents::IntentLockWriteIntentPreemption);
        }

        locked = true;
        ProfileEvents::increment(ProfileEvents::IntentLockWriteIntentElapsedMilliseconds, watch.elapsed());
        ProfileEvents::increment(ProfileEvents::IntentLockAcquiredSuccess);
    }
}

void IntentLock::removeIntents()
{
    if (locked && valid)
    {
        auto catalog = context.getCnchCatalog();
        auto intents = createWriteIntents();
        catalog->clearIntents(intent_prefix, intents);
        locked = false;
        LOG_DEBUG(log, "IntentLock " + intent_prefix + "-" + lock_entity.getNameForLogs() + " is removed");
    }
    else
    {
        LOG_DEBUG(log, "IntentLock " + intent_prefix + "-" + lock_entity.getNameForLogs() + " is not locked. Intents removed");
    }
}

void IntentLock::lockImpl()
{
    ProfileEvents::increment(ProfileEvents::IntentLockAcquiredAttempt);
    Stopwatch watch;

    if (lock_entity.hasUUID())
    {
        auto & coordinator = context.getCnchTransactionCoordinator();
        auto & tsCacheManager = coordinator.getTsCacheManager();

        auto table_guard = tsCacheManager.getTimestampCacheTableGuard(lock_entity.uuid);
        auto & tsCache = tsCacheManager.getTimestampCacheUnlocked(lock_entity.uuid);

        if (auto last_updated_ts = tsCache->lookup(intent_names); last_updated_ts > txn_record.txnID())
        {
            ProfileEvents::increment(ProfileEvents::TsCacheCheckFailed);
            throw Exception(
                "TsCache check failed. Intents has been updated by a later transaction: " + last_updated_ts.toString()
                    + ". Current txn_id: " + txn_record.txnID().toString(),
                ErrorCodes::CNCH_TRANSACTION_TSCACHE_CHECK_FAILED);
        }

        ProfileEvents::increment(ProfileEvents::TsCacheCheckSuccess);
        // write intents under table_guard mutex
        writeIntents();
    }
    else
        writeIntents();

    ProfileEvents::increment(ProfileEvents::IntentLockAcquiredElapsedMilliseconds, watch.elapsedMilliseconds());
}

void IntentLock::unlock()
{
    removeIntents();
}

std::vector<WriteIntent> IntentLock::createWriteIntents()
{
    std::vector<WriteIntent> intents;
    intents.reserve(intent_names.size());
    for (const auto & intent_name : intent_names)
        intents.emplace_back(txn_record.txnID(), txn_record.location(), intent_name);
    return intents;
}

}
