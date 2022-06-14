#pragma once

#include <Core/UUID.h>
#include <ResourceManagement/WorkerNode.h>
#include <ResourceManagement/CommonData.h>

#include <map>
#include <mutex>
#include <boost/noncopyable.hpp>

namespace DB::ResourceManagement
{
class IWorkerGroup : private boost::noncopyable
{
public:
    IWorkerGroup(WorkerGroupType type_, String id_, UUID vw_uuid_) : type(type_), id(std::move(id_)), vw_uuid(vw_uuid_) { }
    virtual ~IWorkerGroup() { }

    WorkerGroupType getType() const { return type; }
    auto & getID() const { return id; }
    auto getVWUUID() const { return vw_uuid; }

    void setVWName(String name)
    {
        std::lock_guard lock(state_mutex);
        vw_name = std::move(name);
    }

    String getVWName() const
    {
        std::lock_guard lock(state_mutex);
        return vw_name;
    }


    virtual size_t getNumWorkers() const = 0;
    virtual std::map<String, WorkerNodePtr> getWorkers() const = 0;
    virtual WorkerGroupData getData(bool with_metrics = false) const = 0;

    /// Only physical worker groups need to refresh metrics.
    virtual void refreshAggregatedMetrics() {}

    virtual WorkerGroupMetrics getAggregatedMetrics() const = 0;

    virtual void registerNode(const WorkerNodePtr &) { }
    virtual void removeNode(const String &) { }

    virtual bool empty() const = 0;
    virtual WorkerNodePtr randomWorker() const = 0;

protected:
    const WorkerGroupType type;
    const String id;
    const UUID vw_uuid;

    mutable std::mutex state_mutex;
    std::string vw_name;
};

using WorkerGroupPtr = std::shared_ptr<IWorkerGroup>;

}
