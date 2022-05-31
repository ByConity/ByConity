#pragma once

#include <atomic>
#include <map>
#include <optional>

#include <Core/UUIDHelpers.h>
#include <Interpreters/StorageID.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TxnTimestamp.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>

namespace DB
{
class Context;

struct LockEntity
{
    String database_name;
    String table_name;
    UUID uuid = UUIDHelpers::Nil;

    LockEntity() = default;

    LockEntity(String database, String table = String(), UUID uuid_ = UUIDHelpers::Nil)
        : database_name(std::move(database)), table_name(std::move(table)), uuid(uuid_)
    {
    }

    LockEntity(const StorageID & storage_id)
        : database_name(storage_id.database_name), table_name(storage_id.table_name), uuid(storage_id.uuid)
    {
    }

    bool hasUUID() const { return uuid != UUIDHelpers::Nil; }
    String getDatabase_name() const { return database_name; }
    String getTableName() const { return table_name; }

    String getNameForLogs() const
    {
        return (database_name.empty() ? "" : database_name + ".") + table_name
            + (hasUUID() ? " (UUID " + toString(uuid) + ")" : "");
    }
};

class IntentLock
{
private:
    static constexpr const char * TB_LOCK_PREFIX = "TB_LOCK";
    static constexpr const char * DB_LOCK_PREFIX = "DB_LOCK";

public:
    IntentLock(const Context & context_, TransactionRecord txn_record_, LockEntity entity, Strings intent_names_ = {})
        : context(context_)
        , txn_record(std::move(txn_record_))
        , lock_entity(std::move(entity))
        , intent_names(std::move(intent_names_))
        , log(&Poco::Logger::get("IntentLock"))
    {
        if (intent_names.empty())
        {
            if (lock_entity.table_name.empty())
            {
                intent_prefix = DB_LOCK_PREFIX;
                intent_names = {lock_entity.database_name};
            }
            else
            {
                intent_prefix = TB_LOCK_PREFIX;
                intent_names = {lock_entity.database_name + "-" + lock_entity.table_name};
            }
        }
        else
        {
            intent_prefix = UUIDHelpers::UUIDToString(lock_entity.uuid);
        }
    }

    IntentLock(const IntentLock &) = delete;
    IntentLock & operator=(const IntentLock &) = delete;

    ~IntentLock()
    {
        try
        {
            unlock();
        }
        catch (...)
        {
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    bool tryLock();
    void lock();
    void unlock();
    bool isLocked() { return locked.load(std::memory_order_relaxed); }

    const LockEntity & getLockEntity() const { return lock_entity; }

private:
    const Context & context;
    TransactionRecord txn_record;
    LockEntity lock_entity;
    String intent_prefix;
    Strings intent_names;

    static constexpr size_t lock_retry = 3;
    std::atomic<bool> locked{false};
    bool valid{true};
    Poco::Logger * log;

private:
    void lockImpl();
    void writeIntents();
    void removeIntents();
    std::optional<TransactionRecord> tryGetTransactionRecord(const TxnTimestamp & txnID);
    std::vector<WriteIntent> createWriteIntents();
};

using IntentLockPtr = std::unique_ptr<IntentLock>;
using IntentLockPtrs = std::vector<IntentLockPtr>;

}
