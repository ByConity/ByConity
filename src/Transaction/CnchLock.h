#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/StorageID.h>
#include <Transaction/LockRequest.h>
#include <boost/core/noncopyable.hpp>
#include <Transaction/TxnTimestamp.h>

namespace DB
{
class Context;
class ICnchTransaction;

class CnchLockHolder : private boost::noncopyable, WithContext
{
public:
    friend class ICnchTransaction;
    explicit CnchLockHolder(const ContextPtr & context_, std::vector<LockInfoPtr> && elems);
    explicit CnchLockHolder(const ContextPtr & context_, LockInfoPtr && elem);

    ~CnchLockHolder();

    [[nodiscard]] bool tryLock();
    void lock();
    void unlock();
    void assertLockAcquired() const;
    void setLockExpireDuration(std::chrono::milliseconds expire_duration) { lock_expire_duration = expire_duration; }

private:
    void reportLockHeartBeatTask();
    void reportLockHeartBeat();

    TxnTimestamp txn_id;
    BackgroundSchedulePool::TaskHolder report_lock_heartbeat_task;

    class CnchLock;
    std::vector<std::unique_ptr<CnchLock>> cnch_locks;

    std::chrono::milliseconds lock_expire_duration{30000};
};

using CnchLockHolderPtr = std::shared_ptr<CnchLockHolder>;
using CnchLockHolderPtrs = std::vector<CnchLockHolderPtr>;

}
