#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <ResourceManagement/IWorkerGroup.h>
#include <map>


namespace DB::ResourceManagement
{
class PhysicalWorkerGroup : public IWorkerGroup
{
public:
    PhysicalWorkerGroup(ContextPtr context, String id_, UUID vw_uuid_, String psm_ = {})
        : IWorkerGroup(WorkerGroupType::Physical, std::move(id_), vw_uuid_), psm(psm_)
        , aggregated_metrics(WorkerGroupMetrics(id))
        , refresh_metrics_task(context->getSchedulePool().createTask("WorkerGroupRefreshMetrics", [&]() { refreshAggregatedMetrics(); }))
    {
        refresh_metrics_task->activateAndSchedule();
    }

    ~PhysicalWorkerGroup() override { }

    size_t getNumWorkers() const override;
    std::map<String, WorkerNodePtr> getWorkers() const override;
    WorkerGroupData getData(bool with_metrics = false, bool only_running_state = true) const override;
    void refreshAggregatedMetrics() override;
    WorkerGroupMetrics getAggregatedMetrics() const override;

    void registerNode(const WorkerNodePtr & node) override;
    void removeNode(const String & worker_id) override;

    void addLentGroupDestID(const String & group_id);

    void removeLentGroupDestID(const String & group_id);

    void clearLentGroups();

    std::unordered_set<String> getLentGroupsDestIDs() const;

    bool empty() const override
    {
        std::lock_guard lock(state_mutex);
        return workers.empty();
    }

    std::vector<WorkerNodePtr> randomWorkers(const size_t n, const std::unordered_set<String> & blocklist) const override;

private:
    std::map<String, WorkerNodePtr> getWorkersImpl(std::lock_guard<std::mutex> & lock) const;

    const String psm;
    std::map<String, WorkerNodePtr> workers;
    WorkerGroupMetrics aggregated_metrics;
    BackgroundSchedulePool::TaskHolder refresh_metrics_task;
    std::unordered_set<String> lent_groups_dest_ids;

};

using PhysicalWorkerGroupPtr = std::shared_ptr<PhysicalWorkerGroup>;


}
