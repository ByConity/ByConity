#pragma once

#include <ResourceManagement/VirtualWarehouse.h>
#include <Common/ConcurrentMapForCreating.h>

#include <common/logger_useful.h>
#include <ResourceManagement/CommonData.h>

namespace DB::ResourceManagement
{
class ResourceManagerController;

class VirtualWarehouseManager : protected ConcurrentMapForCreating<std::string, VirtualWarehouse>, private boost::noncopyable
{
public:
    VirtualWarehouseManager(ResourceManagerController & rm_controller_);

    void loadVirtualWarehouses();

    VirtualWarehousePtr createVirtualWarehouse(const std::string & name, const VirtualWarehouseSettings & settings, const bool if_not_exists);
    VirtualWarehousePtr tryGetVirtualWarehouse(const std::string & name);
    VirtualWarehousePtr getVirtualWarehouse(const std::string & name);
    void alterVirtualWarehouse(const std::string & name, const VirtualWarehouseAlterSettings & settings);
    void dropVirtualWarehouse(const std::string & name, const bool if_exists);

    auto getAllVirtualWarehouses() { return getAll(); }

    void clearVirtualWarehouses();

    void updateQueryQueueMap(const String & server_id, const VWQueryQueueMap & vw_query_queue_map, std::vector<String> & deleted_vw_list);

    AggQueryQueueMap getAggQueryQueueMap() const;

private:
    ResourceManagerController & rm_controller;
    Poco::Logger * log{nullptr};

    std::atomic_bool need_sync_with_catalog{false};
};
}
