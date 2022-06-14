#pragma once
#include <ResourceManagement/IWorkerGroup.h>

namespace DB::ResourceManagement
{
class SharedWorkerGroup : public IWorkerGroup
{
public:
    SharedWorkerGroup(String id_, UUID vw_uuid_, String linked_id_)
        : IWorkerGroup(WorkerGroupType::Shared, std::move(id_), vw_uuid_), linked_id(std::move(linked_id_))
    {
    }

    size_t getNumWorkers() const override;
    std::map<String, WorkerNodePtr> getWorkers() const override;
    WorkerGroupData getData(bool with_metrics = false) const override;
    WorkerGroupMetrics getAggregatedMetrics() const override;

    void registerNode(const WorkerNodePtr &) override;
    void removeNode(const String &) override;

    bool empty() const override
    {
        return linked_group->empty();
    }

    WorkerNodePtr randomWorker() const override
    {
        return linked_group->randomWorker();
    }

    void setLinkedGroup(WorkerGroupPtr group);

private:
    std::map<String, WorkerNodePtr> getWorkersImpl(std::lock_guard<std::mutex> & lock) const;

    const String linked_id;
    WorkerGroupPtr linked_group;
};

using SharedWorkerGroupPtr = std::shared_ptr<SharedWorkerGroup>;

}
