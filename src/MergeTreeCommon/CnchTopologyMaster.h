#pragma once

#include <Interpreters/Context.h>
#include <MergeTreeCommon/CnchServerTopology.h>
#include <Core/BackgroundSchedulePool.h>

namespace DB
{
/***
 * This class help to supply a unified view of servers' topology in cnch cluster.
 * A background task will periodically fetch topology from metastore to ensure the cached topology
 * keeps up to date.
 */
class CnchTopologyMaster
{
public:
    CnchTopologyMaster(Context & context_);

    ~CnchTopologyMaster();

    std::list<CnchServerTopology> getCurrentTopology();

    /// Get target server for table with current timestamp.
    HostWithPorts getTargetServer(const String & table_uuid, bool allow_empty_result, bool allow_tso_unavailable = false);

    /// Get target server with provided timestamp.
    HostWithPorts getTargetServer(const String & table_uuid, const UInt64 ts,  bool allow_empty_result, bool allow_tso_unavailable = false);

    void shutDown();
private:

    HostWithPorts getTargetServerImpl(
        const String & table_uuid,
        std::list<CnchServerTopology> & current_topology,
        const UInt64 current_ts,
        bool allow_empty_result,
        bool allow_tso_unavailable);

    Poco::Logger * log = &Poco::Logger::get("CnchTopologyMaster");
    Context & context;
    BackgroundSchedulePool::TaskHolder topology_fetcher;
    std::list<CnchServerTopology> topologies;
    mutable std::mutex mutex;
};

using CnchTopologyMasterPtr = std::shared_ptr<CnchTopologyMaster>;

}
