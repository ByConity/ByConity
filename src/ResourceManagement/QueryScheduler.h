#pragma once
#include <Common/HostWithPorts.h>
#include <Core/Types.h>

#include <boost/noncopyable.hpp>
#include <common/logger_useful.h>
#include <memory>
#include <ResourceManagement/CommonData.h>
#include <ResourceManagement/VWScheduleAlgo.h>
#include <ResourceManagement/WorkerNode.h>

namespace DB::ResourceManagement
{
class VirtualWarehouse;
using VirtualWarehousePtr = std::shared_ptr<VirtualWarehouse>;
class IWorkerGroup;
using WorkerGroupPtr = std::shared_ptr<IWorkerGroup>;

/// A worker group's metrics can be updated in runtime,
/// so we need to copy both group ptr and metrics to avoid too many lock contentions.
using WorkerGroupAndMetrics = std::pair<WorkerGroupPtr, WorkerGroupMetrics>;


class QueryScheduler : boost::noncopyable
{
    using VWScheduleAlgo = ResourceManagement::VWScheduleAlgo;
    using Requirement = ResourceManagement::ResourceRequirement;
public:
    explicit QueryScheduler(VirtualWarehouse & vw_);

    ~QueryScheduler() = default;

    WorkerGroupPtr pickWorkerGroup(const VWScheduleAlgo & algo = VWScheduleAlgo::GlobalRoundRobin, const Requirement & requirement = {});

    HostWithPorts pickWorker(const VWScheduleAlgo & algo = VWScheduleAlgo::Random, const Requirement & requirement = {});

private:
    void filterGroup(const Requirement & requirement, std::vector<WorkerGroupAndMetrics> & out_available_groups) const;
    WorkerGroupPtr selectGroup(const VWScheduleAlgo & algo, const std::vector<WorkerGroupAndMetrics> & available_groups);

    void filterWorker(const Requirement & requirement, std::vector<WorkerNodePtr> & out_available_workers);
    HostWithPorts selectWorker(const VWScheduleAlgo & algo, const std::vector<WorkerNodePtr> & available_workers);


    VirtualWarehouse & vw;
    Poco::Logger * log;

    /// TODO: (zuochuang.zema) With ResourceRequirement, RoundRobin might not be a good strategy anymore as the available workers (groups) changes dynamically.
    std::atomic<size_t> pick_group_sequence = 0;   /// round-robin index for pickWorkerGroup.
    std::atomic<size_t> pick_worker_sequence = 0;  /// round-robin index for pickWorker.
};

}
