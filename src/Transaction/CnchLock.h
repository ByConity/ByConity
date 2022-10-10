#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/StorageID.h>
#include <Transaction/LockRequest.h>
#include <boost/core/noncopyable.hpp>
#include <Transaction/TxnTimestamp.h>

namespace DB
{
class Context;

class CnchLockHolder : private boost::noncopyable
{
public:
    explicit CnchLockHolder(const Context & global_context_, std::vector<LockInfoPtr> && elems);
    ~CnchLockHolder();

    void lock();
    void unlock();
    void setLockExpireDuration(std::chrono::milliseconds expire_duration) { lock_expire_duration = expire_duration; }

private:
    void reportLockHeartBeatTask();
    void reportLockHeartBeat();

    const Context & global_context;
    TxnTimestamp txn_id;
    BackgroundSchedulePool::TaskHolder report_lock_heartbeat_task;

    class CnchLock;
    std::vector<std::unique_ptr<CnchLock>> cnch_locks;

    std::chrono::milliseconds lock_expire_duration{30000};
};

}
