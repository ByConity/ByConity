/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <Common/Logger.h>
#include <Core/BackgroundSchedulePool.h>

#include <bthread/mutex.h>
#include <boost/noncopyable.hpp>

namespace Poco
{
    class Logger;
}

namespace DB::ResourceManagement
{

enum class CoordinateMode
{
    None,
    Sharing,
    Scaling,
};

struct ResourceCoordinateDecision
{
    String type;
    String vw;
    String wg;
    size_t expected_wg_number{0};

    ResourceCoordinateDecision(const String & type_, const String & vw_, const String & wg_, size_t expected_wg_number_)
    : type(type_), vw(vw_), wg(wg_), expected_wg_number(expected_wg_number_) {} 

    inline std::string toDebugString() const
    {
        std::stringstream ss;
        ss << "resource_coordinate{type:" << type
           << ", vw:" << vw
           << ", wg:" << wg
           << ", expected_wg_number:" << expected_wg_number
           << "}";
        return ss.str();
    }
};

class ResourceManagerController;
class VirtualWarehouse;
class IWorkerGroup;
using VirtualWarehousePtr = std::shared_ptr<VirtualWarehouse>;
using WorkerGroupPtr = std::shared_ptr<IWorkerGroup>;


/** This class is used to enable resource coordinating for worker groups across VW.
  * There are two modes: 
  * 1. Sharing - auto create/drop a shared worker group link from a vw to a idle/busy worker group. 
  * 2. Scaling - auto create/drop a phyiscal worker group when the vw is busy/idle.
  * In Sharing mode, creat a shared worker group means scaling up, and drop a shared worker group means scaling down.
  * This feature is only enabled if resource_coordinate_mode is one of above modes (None to disable this feature).
  *
  */
class WorkerGroupResourceCoordinator : private boost::noncopyable
{
    using Decision = ResourceCoordinateDecision;
public:
    WorkerGroupResourceCoordinator(ResourceManagerController & rm_controller_);
    ~WorkerGroupResourceCoordinator();

    void setMode(const String & mode_);
    void start();
    void stop();
    std::vector<Decision> flushDecisions();

private:
    ResourceManagerController & rm_controller;
    LoggerPtr log;
    CoordinateMode mode{CoordinateMode::Sharing};
    BackgroundSchedulePool::TaskHolder background_task;
    UInt64 task_interval_ms;
    
    std::unordered_map<String, UInt64> last_scaleup_timestamps;
    std::unordered_map<String, UInt64> last_scaledown_timestamps;

    std::mutex decisions_mutex;
    std::vector<Decision> decisions;


    // Unlink ineligible worker groups linked by WorkerGroupResourceCoordinator.
    // Also updates last lend time for each linked worker group's parent VW
    void unlinkBusyAndOverlentGroups(const VirtualWarehousePtr & vw,
                                     std::lock_guard<bthread::Mutex> * vw_lock,
                                     std::lock_guard<bthread::Mutex> * wg_lock);

    void unlinkOverborrowedGroups(const VirtualWarehousePtr & vw,
                                  std::lock_guard<bthread::Mutex> * vw_lock,
                                  std::lock_guard<bthread::Mutex> * wg_lock);

    void unlinkIneligibleGroups(const std::unordered_map<String, VirtualWarehousePtr> & vws,
                                std::lock_guard<bthread::Mutex> * vw_lock,
                                std::lock_guard<bthread::Mutex> * wg_lock);

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
                            std::lock_guard<bthread::Mutex> * vw_lock,
                            std::lock_guard<bthread::Mutex> * wg_lock);

    void run();
    void runAutoSharing();
    void runAutoScaling();
    void decideScaleup(const Decision & decision);
    void decideScaledown(const Decision & decision);

    static constexpr auto linked_group_infix = "_borrow_from_";

};

}
