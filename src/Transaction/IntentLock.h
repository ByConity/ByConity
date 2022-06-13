#pragma once

#include <atomic>
#include <map>
#include <optional>
#include <boost/noncopyable.hpp>

#include <Core/UUIDHelpers.h>
#include <Interpreters/StorageID.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TxnTimestamp.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>

namespace DB
{
class Context;

class IntentLock : boost::noncopyable
{
public:
    static constexpr auto TB_LOCK_PREFIX = "TB_LOCK";
    static constexpr auto DB_LOCK_PREFIX = "DB_LOCK";

    IntentLock(const Context & context_, TransactionRecord txn_record_, String lock_prefix_, Strings intent_names_ = {})
        : context(context_)
        , txn_record(std::move(txn_record_))
        , lock_prefix(lock_prefix_)
        , intent_names(std::move(intent_names_))
        , log(&Poco::Logger::get("IntentLock"))
    {
    }

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

private:
    static constexpr size_t lock_retry = 3;

    const Context & context;
    TransactionRecord txn_record;
    String lock_prefix;
    Strings intent_names;

    bool locked{false};
    bool valid{true};
    Poco::Logger * log;

    void lockImpl();
    void writeIntents();
    void removeIntents();
    std::optional<TransactionRecord> tryGetTransactionRecord(const TxnTimestamp & txnID);
    std::vector<WriteIntent> createWriteIntents();
};

using IntentLockPtr = std::unique_ptr<IntentLock>;
using IntentLockPtrs = std::vector<IntentLockPtr>;

}
