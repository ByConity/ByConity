#pragma once

#include <Catalog/IMetastore.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context_fwd.h>
#include <MergeTreeCommon/CnchServerTopology.h>
#include <Common/StorageElection/KvStorage.h>
#include <Common/StorageElection/StorageElector.h>

namespace DB
{

/***
 * CnchServerLeader is responsible for manage cluster-unique (among all servers) services.
 *
 * Leader election is required to make sure there only one thread that can execute functions in a cluster at a time.
 */
class CnchServerLeader : public WithContext
{
public:
    explicit CnchServerLeader(ContextPtr context_);

    ~CnchServerLeader();

    bool isLeader() const;
    std::optional<HostWithPorts> getCurrentLeader() const;

    void shutDown();
    void partialShutdown();

private:
    /// Logger
    LoggerPtr log = getLogger("CnchServerLeader");
    /// Leader election related.
    bool onLeader();
    bool onFollower();
    std::shared_ptr<StorageElector> elector;
    std::atomic_bool leader_initialized{false};

    /// User-defined logic.
    bool checkAsyncQueryStatus();
    BackgroundSchedulePool::TaskHolder async_query_status_check_task;
    std::atomic<UInt64> async_query_status_check_time{0};
};

using CnchServerLeaderPtr = std::shared_ptr<CnchServerLeader>;

}
