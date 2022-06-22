#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <boost/noncopyable.hpp>

namespace Poco
{
    class Logger;
}

namespace DB::ResourceManagement
{

class ResourceManagerController;
class VirtualWarehouse;
class IWorkerGroup;
using VirtualWarehousePtr = std::shared_ptr<VirtualWarehouse>;
using WorkerGroupPtr = std::shared_ptr<IWorkerGroup>;


/** This class is used to enable sharing of worker groups across VW.
  * This feature is only enabled if cnch_auto_enable_resource_sharing is configured to true.
  *
  */
class WorkerGroupResourceCoordinator : private boost::noncopyable
{
public:
    WorkerGroupResourceCoordinator(ResourceManagerController & rm_controller_);
    ~WorkerGroupResourceCoordinator();

    void start();

    void stop();

private:
    ResourceManagerController & rm_controller;

    Poco::Logger * log;

    BackgroundSchedulePool::TaskHolder background_task;

    UInt64 task_interval_ms;


    // Unlink ineligible worker groups linked by WorkerGroupResourceCoordinator.
    // Also updates last lend time for each linked worker group's parent VW
    void unlinkBusyAndOverlentGroups(const VirtualWarehousePtr & vw,
                                     std::lock_guard<std::mutex> * vw_lock,
                                     std::lock_guard<std::mutex> * wg_lock);
    void unlinkOverborrowedGroups(const VirtualWarehousePtr & vw,
                                  std::lock_guard<std::mutex> * vw_lock,
                                  std::lock_guard<std::mutex> * wg_lock);
    void unlinkIneligibleGroups(const std::unordered_map<String, VirtualWarehousePtr> & vws,
                                std::lock_guard<std::mutex> * vw_lock,
                                std::lock_guard<std::mutex> * wg_lock);

    void getEligibleGroups(std::unordered_map<String, VirtualWarehousePtr> & vws,
                            std::vector<VirtualWarehousePtr> & eligible_vw_borrowers,
                            std::vector<WorkerGroupPtr> & eligible_wg_lenders,
                            size_t & borrow_slots,
                            size_t & lend_slots);
    void linkEligibleGroups(std::unordered_map<String, VirtualWarehousePtr> & vws,
                            std::vector<VirtualWarehousePtr> & eligible_vw_borrowers,
                            std::vector<WorkerGroupPtr> & eligible_wg_lenders,
                            size_t & borrow_slots,
                            size_t & lend_slots,
                            std::lock_guard<std::mutex> * vw_lock,
                            std::lock_guard<std::mutex> * wg_lock);

    void run();

    static constexpr auto linked_group_infix = "_borrow_from_";

};

}
