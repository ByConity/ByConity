#include <Transaction/CnchLock.h>

#include <CloudServices/CnchServerClient.h>
#include <CloudServices/CnchWorkerClientPools.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <Transaction/LockDefines.h>
#include <Transaction/LockManager.h>
#include <Common/Exception.h>
#include <Common/serverLocality.h>
#include <Poco/Logger.h>

#include <atomic>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CNCH_LOCK_ACQUIRE_FAILED;
}

class CnchLockHolder::CnchLock
{
public:
    explicit CnchLock(const Context & context_, LockInfoPtr info) : context(context_), lock_info(std::move(info)) { }

    ~CnchLock()
    {
        try
        {
            unlock();
        }
        catch (...)
        {
        }
    }

    CnchLock(const CnchLock &) = delete;
    CnchLock & operator=(const CnchLock &) = delete;

    void lock()
    {
        auto server = context.getCnchTopologyMaster()->getTargetServer(UUIDHelpers::UUIDToString(lock_info->table_uuid), false);
        lock_info->lock_id = context.getTimestamp();
        String host_with_rpc = server.getRPCAddress();

        bool is_local = isLocalServer(host_with_rpc, std::to_string(context.getRPCPort()));
        LOG_DEBUG(
            &Poco::Logger::get("CnchLockManagerClient"),
            "try lock {}, target server: {}", lock_info->toDebugString(), (is_local ? "local" : host_with_rpc));

        if (is_local)
        {
            LockManager::instance().lock(lock_info, context);
        }
        else
        {
            client = context.getCnchServerClientPool().get(host_with_rpc);
            client->acquireLock(lock_info);
        }

        locked = (lock_info->status == LockStatus::LOCK_OK);
    }

    void unlock()
    {
        if (locked)
        {
            if (client)
                client->releaseLock(lock_info);
            else
                LockManager::instance().unlock(lock_info);

            locked = false;
        }
    }

    const Context & context;
    bool locked{false};
    LockInfoPtr lock_info;
    CnchServerClientPtr client;
};

CnchLockHolder::CnchLockHolder(const Context & global_context_, std::vector<LockInfoPtr> && elems) : global_context(global_context_)
{
    assert(!elems.empty());
    txn_id = elems.front()->txn_id;
    assert(txn_id);
    for (const auto & info : elems)
    {
        assert(txn_id == info->txn_id);
        cnch_locks.push_back(std::make_unique<CnchLock>(global_context, info));
    }
}

CnchLockHolder::~CnchLockHolder()
{
    if (report_lock_heartbeat_task)
        report_lock_heartbeat_task->deactivate();

    unlock();
}

void CnchLockHolder::lock()
{
    Stopwatch watch;
    SCOPE_EXIT({ LOG_DEBUG(&Poco::Logger::get("CnchLock"), "acquire {} locks in {} ms", cnch_locks.size(), watch.elapsedMilliseconds()); });

    for (const auto & lock : cnch_locks)
    {
        lock->lock();
    }

    /// init heartbeat task if needed
    if (!report_lock_heartbeat_task)
    {
        report_lock_heartbeat_task
            = global_context.getSchedulePool().createTask("reportLockHeartBeat", [this]() { reportLockHeartBeatTask(); });
        report_lock_heartbeat_task->activateAndSchedule();
    }
}

void CnchLockHolder::unlock()
{
    for (const auto & lock : cnch_locks)
        lock->unlock();
}

void CnchLockHolder::reportLockHeartBeat()
{
    std::set<CnchServerClient *> clients;
    bool update_local_lock_manager = false;

    for (const auto & cnch_lock : cnch_locks)
    {
        if (!cnch_lock->locked)
            continue;

        if (cnch_lock->client)
            clients.emplace(cnch_lock->client.get());
        else
            update_local_lock_manager = true;
    }

    if (update_local_lock_manager)
        LockManager::instance().updateExpireTime(txn_id, LockManager::Clock::now() + lock_expire_duration);

    for (auto * client : clients)
    {
        client->reportCnchLockHeartBeat(txn_id, lock_expire_duration.count());
    }
}

static constexpr UInt64 heartbeat_interval = 5000;

void CnchLockHolder::reportLockHeartBeatTask()
{
    try
    {
        reportLockHeartBeat();
        report_lock_heartbeat_task->scheduleAfter(heartbeat_interval);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        report_lock_heartbeat_task->scheduleAfter(heartbeat_interval);
    }
}

}
