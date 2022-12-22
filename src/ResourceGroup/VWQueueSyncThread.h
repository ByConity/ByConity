#pragma once

#include <thread>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Types.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ResourceManagement
{
    struct VirtualWarehouseData;
}

class IResourceGroup;
class VWResourceGroupManager;

using ResourceGroupPtr = std::shared_ptr<IResourceGroup>;
using VirtualWarehouseData = ResourceManagement::VirtualWarehouseData;


/** Server-side thread that synchronises server query queue info with Resource Manager
  * Retrieves total query queue info snapshot, and performs resource group deletion for unused groups
  */
class VWQueueSyncThread : protected WithContext
{
public:
    VWQueueSyncThread(UInt64 interval_, ContextPtr global_context_);
    ~VWQueueSyncThread();

    void start()
    {
        task->activateAndSchedule();
    }

    void stop()
    {
        task->deactivate();
    }

    UInt64 getLastSyncTime() const
    {
        return last_sync_time.load();
    }

private:
    using ResourceGroupDataPair = std::pair<VirtualWarehouseData, ResourceGroupPtr>;
    bool syncQueueDetails(VWResourceGroupManager * vw_resource_group_manager);
    bool syncResourceGroups(VWResourceGroupManager * vw_resource_group_manager);
    void run();

    UInt64 interval; /// in seconds;
    std::atomic<UInt64> last_sync_time{0};

    BackgroundSchedulePool::TaskHolder task;

    Poco::Logger * log;
};

}
