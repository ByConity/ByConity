#include <MergeTreeCommon/CnchServerLeader.h>

#include <Common/Exception.h>
#include <Catalog/Catalog.h>
#include <Storages/PartCacheManager.h>
#include <ServiceDiscovery/IServiceDiscovery.h>

namespace DB
{

CnchServerLeader::CnchServerLeader(ContextPtr context_) : WithContext(context_)
{
    auto task_func = [this] (String task_name, std::function<bool ()> func, UInt64 interval, std::atomic<UInt64> & last_time, BackgroundSchedulePool::TaskHolder & task)
    {
        /// Check schedule delay
        UInt64 start_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        if (last_time && start_time > last_time && (start_time - last_time) > (1000 + interval))
            LOG_WARNING(log, "{} schedules over {}ms. Last finish time: {}, current time: {}", task_name, (1000 + interval), last_time, start_time);

        bool success = false;
        try
        {
            success = func();
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
        /// Check execution time
        UInt64 finish_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        if (finish_time > start_time && finish_time - start_time > 1000)
            LOG_WARNING(log, "{} executed over 1000ms. Start time: {}, current time: {}", task_name, start_time, finish_time);

        last_time = finish_time;
        auto schedule_delay = success ? interval
                                      : getContext()->getSettingsRef().topology_retry_interval_ms.totalMilliseconds();
        task->scheduleAfter(schedule_delay);
    };

    async_query_status_check_task = getContext()->getTopologySchedulePool().createTask("AsyncQueryStatusChecker", [this, task_func](){
        UInt64 async_query_status_check_interval = getContext()->getRootConfig().async_query_status_check_period * 1000;
        task_func("AsyncQueryStatusChecker"
            , [this]() -> bool { return checkAsyncQueryStatus(); }
            , async_query_status_check_interval
            , async_query_status_check_time
            , async_query_status_check_task);
    });

    /// For leader election
    const auto & conf = getContext()->getRootConfig();
    auto refresh_interval_ms = conf.service_discovery_kv.server_leader_refresh_interval_ms.value;
    auto expired_interval_ms = conf.service_discovery_kv.server_leader_expired_interval_ms.value;
    auto prefix = conf.service_discovery_kv.election_prefix.value;
    auto election_path = prefix + conf.service_discovery_kv.server_leader_host_path.value;
    auto host = getContext()->getHostWithPorts();
    auto metastore_ptr = getContext()->getCnchCatalog()->getMetastore();

    elector = std::make_shared<StorageElector>(
        std::make_shared<ServerManagerKvStorage>(metastore_ptr),
        refresh_interval_ms,
        expired_interval_ms,
        host,
        election_path,
        [&](const HostWithPorts *) { return onLeader(); },
        [&](const HostWithPorts *) { return onFollower(); }
    );
}

CnchServerLeader::~CnchServerLeader()
{
    try
    {
        shutDown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

bool CnchServerLeader::isLeader() const
{
    return elector->isLeader();
}

std::optional<HostWithPorts> CnchServerLeader::getCurrentLeader() const
{
    return elector->getLeaderInfo();
}

/// Callback by StorageElector. Need to gurantee no exception thrown in this method.
bool CnchServerLeader::onLeader()
{
    auto current_address = getContext()->getHostWithPorts().getRPCAddress();

    try
    {
        async_query_status_check_task->activateAndSchedule();
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to set leader status when current node becoming leader.");
        partialShutdown();
        return false;
    }

    LOG_DEBUG(log, "Current node {} become leader", current_address);
    return true;
}

bool CnchServerLeader::onFollower()
{
    try
    {
        partialShutdown();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        return false;
    }

    return true;
}

bool CnchServerLeader::checkAsyncQueryStatus()
{
    /// Mark inactive jobs to failed.
    try
    {
        auto statuses = getContext()->getCnchCatalog()->getIntermidiateAsyncQueryStatuses();
        std::vector<Protos::AsyncQueryStatus> to_expire;
        for (const auto & status : statuses)
        {
            /// Find the expired statuses.
            UInt64 start_time = static_cast<UInt64>(status.start_time());
            UInt64 execution_time = static_cast<UInt64>(status.max_execution_time());
            /// TODO(WangTao): We could have more accurate ways to expire status whose execution time is unlimited, like check its real status from host server.
            if (execution_time == 0)
                execution_time = getContext()->getRootConfig().async_query_expire_time;
            if (time(nullptr) - start_time > execution_time)
            {
                to_expire.push_back(std::move(status));
            }
        }

        if (!to_expire.empty())
        {
            LOG_INFO(log, "Mark {} async queries to failed.", to_expire.size());
            getContext()->getCnchCatalog()->markBatchAsyncQueryStatusFailed(to_expire, "Status expired");
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return false;
    }

    return true;
}

void CnchServerLeader::shutDown()
{
    try
    {
        async_query_status_check_task->deactivate();
        elector->stop();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

/// call me when election is expired
void CnchServerLeader::partialShutdown()
{
    try
    {
        leader_initialized = false;

        async_query_status_check_task->deactivate();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}


}
