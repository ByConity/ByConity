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
#include "Interpreters/Context_fwd.h"
#include <Poco/Logger.h>

#include <atomic>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CNCH_LOCK_ACQUIRE_FAILED;
}

class CnchLockHolder::CnchLock : WithContext
{
friend class CnchLockHolder;
public:
    explicit CnchLock(const ContextPtr & context_, LockInfoPtr info) : WithContext(context_), lock_info(std::move(info)) { }

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

    bool tryLock()
    {
        auto client = getTargetServer();

        LOG_DEBUG(
            getLogger("CnchLockManagerClient"),
            "try lock {}, target server: {}", lock_info->toDebugString(), (client.has_value() ? (*client)->getRPCAddress() : "local"));

        if (!client)
        {
            auto context = getContext();
            LockManager::instance().lock(lock_info, *context);
        }
        else
        {
            server_client = *client;
            server_client->acquireLock(lock_info);
        }

        locked = (lock_info->status == LockStatus::LOCK_OK);
        return locked;
    }

    void unlock()
    {
        if (locked)
        {
            LOG_DEBUG(
                getLogger("CnchLockManagerClient"),
                "unlock lock {}, target server: {}", lock_info->toDebugString(), (server_client ? server_client->getRPCAddress() : "local"));

            if (server_client)
                server_client->releaseLock(lock_info);
            else
                LockManager::instance().unlock(lock_info);

            locked = false;
        }
    }

    void assertLockAcquired() const
    {
        if (!locked)
            return;

        auto client = getTargetServer();
        if (client)
        {
            (*client)->assertLockAcquired(lock_info);
        }
        else
        {
            LockManager::instance().assertLockAcquired(lock_info->txn_id, lock_info->lock_id);
        }
    }

private:
    std::optional<CnchServerClientPtr> getTargetServer() const
    {
        auto context = getContext();
        auto server = context->getCnchTopologyMaster()->getTargetServer(lock_info->table_uuid_with_prefix, DEFAULT_SERVER_VW_NAME, false);
        String host_with_rpc = server.getRPCAddress();

        bool is_local = isLocalServer(host_with_rpc, std::to_string(context->getRPCPort()));

        if (is_local)
        {
            return {};
        }
        else
        {
            return getContext()->getCnchServerClientPool().get(host_with_rpc);
        }
    }

    bool locked{false};
    LockInfoPtr lock_info;
    CnchServerClientPtr server_client;
};

CnchLockHolder::CnchLockHolder(const ContextPtr & context_, LockInfoPtr && elem) : WithContext(context_)
{
    txn_id = elem->txn_id;
    assert(txn_id);
    elem->setLockID(context_->getTimestamp());
    cnch_locks.push_back(std::make_unique<CnchLock>(context_, elem));
}

CnchLockHolder::CnchLockHolder(const ContextPtr & context_, std::vector<LockInfoPtr> && elems) : WithContext(context_)
{
    assert(!elems.empty());
    txn_id = elems.front()->txn_id;
    assert(txn_id);
    for (const auto & info : elems)
    {
        assert(txn_id == info->txn_id);
        /// assign lock id;
        info->setLockID(context_->getTimestamp());
        cnch_locks.push_back(std::make_unique<CnchLock>(context_, info));
    }
}

CnchLockHolder::~CnchLockHolder()
{
    try
    {
        if (report_lock_heartbeat_task)
            report_lock_heartbeat_task->deactivate();

        unlock();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

bool CnchLockHolder::tryLock()
{
    Stopwatch watch;
    SCOPE_EXIT({ LOG_DEBUG(getLogger("CnchLock"), "acquire {} locks in {} ms", cnch_locks.size(), watch.elapsedMilliseconds()); });

    /// Init heartbeat task if needed
    /// We need to start the heartbeat process in advance, otherwise txn may be aborted due to expiration time
    if (!report_lock_heartbeat_task)
    {
        report_lock_heartbeat_task
            = getContext()->getSchedulePool().createTask("reportLockHeartBeat", [this]() { reportLockHeartBeatTask(); });
        report_lock_heartbeat_task->activateAndSchedule();
    }

    for (const auto & lock : cnch_locks)
    {
        if (!lock->tryLock())
            return false;
    }
    return true;
}

void CnchLockHolder::lock()
{
    if (!tryLock())
        throw Exception("Unable to acquired lock", ErrorCodes::CNCH_LOCK_ACQUIRE_FAILED);
}

void CnchLockHolder::unlock()
{
    for (const auto & lock : cnch_locks)
        lock->unlock();
}

void CnchLockHolder::assertLockAcquired() const
{
    for (const auto & cnch_lock : cnch_locks)
    {
        cnch_lock->assertLockAcquired();
    }
}

void CnchLockHolder::reportLockHeartBeat()
{
    std::set<CnchServerClient *> clients;
    bool update_local_lock_manager = false;

    for (const auto & cnch_lock : cnch_locks)
    {
        /// We need to start the heartbeat process in advance, otherwise txn may be aborted due to expiration time
        if (cnch_lock->server_client)
            clients.emplace(cnch_lock->server_client.get());
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
