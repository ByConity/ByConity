#include <ResourceManagement/VirtualWarehouseManager.h>

#include <Catalog/Catalog.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <ResourceManagement/ResourceManagerController.h>
#include <Catalog/Catalog.h>
#include <ResourceManagement/ResourceTracker.h>

namespace DB
{
namespace ErrorCodes
{
    const extern int BRPC_TIMEOUT;
    const extern int VIRTUAL_WAREHOUSE_ALREADY_EXISTS;
    const extern int VIRTUAL_WAREHOUSE_NOT_FOUND;
    const extern int VIRTUAL_WAREHOUSE_NOT_EMPTY;
}
}

namespace DB::ResourceManagement
{
VirtualWarehouseManager::VirtualWarehouseManager(ResourceManagerController & rm_controller_)
    : rm_controller(rm_controller_), log(&Poco::Logger::get("VirtualWarehouseManager"))
{
}

void VirtualWarehouseManager::loadVirtualWarehouses()
{
    auto catalog = rm_controller.getCnchCatalog();

    LOG_DEBUG(log, "Loading virtual warehouses...");

    auto vw_data_list = catalog->scanVirtualWarehouses();

    std::lock_guard lock(cells_mutex);
    for (auto & vw_data : vw_data_list)
    {
        auto vw = VirtualWarehouseFactory::create(vw_data.name, vw_data.uuid, vw_data.settings);
        cells.try_emplace(vw_data.name, vw);

        LOG_DEBUG(log, "Loaded virtual warehouse {}", vw_data.name);
    }

    LOG_INFO(log, "Loaded {} virtual warehouses.", cells.size());
}

VirtualWarehousePtr VirtualWarehouseManager::createVirtualWarehouse(const std::string & name, const VirtualWarehouseSettings & settings, const bool if_not_exists)
{
    if (if_not_exists)
    {
        auto vw = tryGetVirtualWarehouse(name);
        if (vw)
            return vw;
    }

    auto catalog = rm_controller.getCnchCatalog();

    auto uuid = UUIDHelpers::generateV4(); /// TODO: check deplication

    VirtualWarehouseData vw_data;
    vw_data.name = name;
    vw_data.uuid = uuid;
    vw_data.settings = settings;

    auto creator = [&] {
        try
        {
            catalog->createVirtualWarehouse(name, vw_data);

            LOG_DEBUG(log, "Created virtual warehouse {} in catalog", name);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::BRPC_TIMEOUT)
                need_sync_with_catalog.store(true, std::memory_order_relaxed);
            throw;
        }

        return VirtualWarehouseFactory::create(name, uuid, settings);
    };

    auto [vw, created] = getOrCreate(name, std::move(creator));
    if (!created)
        throw Exception("Virtual warehouse `" + name + "` already exists.", ErrorCodes::VIRTUAL_WAREHOUSE_ALREADY_EXISTS);

    return vw;
}

VirtualWarehousePtr VirtualWarehouseManager::tryGetVirtualWarehouse(const std::string & name)
{
    auto res = tryGet(name);
    if (!res && need_sync_with_catalog.load(std::memory_order_relaxed))
    {
        auto catalog = rm_controller.getCnchCatalog();

        VirtualWarehouseData vw_data;
        if (catalog->tryGetVirtualWarehouse(name, vw_data))
        {
            return getOrCreate(name, [&] { return VirtualWarehouseFactory::create(name, vw_data.uuid, vw_data.settings); }).first;
        }
    }
    return res;
}

VirtualWarehousePtr VirtualWarehouseManager::getVirtualWarehouse(const std::string & name)
{
    auto res = tryGetVirtualWarehouse(name);
    if (!res)
        throw Exception("Virtual warehouse `" + name + "` not found.", ErrorCodes::VIRTUAL_WAREHOUSE_NOT_FOUND);
    return res;
}

void VirtualWarehouseManager::alterVirtualWarehouse(const std::string & name, const VirtualWarehouseAlterSettings & settings)
{
    auto res = getVirtualWarehouse(name);
    auto catalog = rm_controller.getCnchCatalog();
    try
    {
        res->applySettings(settings, catalog);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::BRPC_TIMEOUT)
            need_sync_with_catalog.store(true, std::memory_order_relaxed);
        throw;
    }
}

void VirtualWarehouseManager::dropVirtualWarehouse(const std::string & name, const bool if_exists)
{
    auto res = tryGetVirtualWarehouse(name);

    if (!res)
    {
        if (if_exists)
            return;
        else
            throw Exception("Virtual Warehouse `" + name + "` not found", ErrorCodes::VIRTUAL_WAREHOUSE_NOT_FOUND);
    }

    if (res->getNumGroups() == 0)
    {
        auto catalog = rm_controller.getCnchCatalog();
        try
        {
            catalog->dropVirtualWarehouse(name);

            LOG_DEBUG(log, "Dropped virtual warehouse {} in catalog", name);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::BRPC_TIMEOUT)
                need_sync_with_catalog.store(true, std::memory_order_relaxed);
            throw;
        }

        erase(name);
    }
    else
    {
        throw Exception("Virtual warehouse `" + name + "` has existing groups", ErrorCodes::VIRTUAL_WAREHOUSE_NOT_EMPTY);
    }
}

void VirtualWarehouseManager::clearVirtualWarehouses()
{
    std::lock_guard lock(cells_mutex);
    cells.clear();
}

void VirtualWarehouseManager::updateQueryQueueMap(const String & server_id, const VWQueryQueueMap & vw_query_queue_map, std::vector<String> & deleted_vw_list)
{
    for (const auto & it : vw_query_queue_map)
    {
        const auto & vw_name = it.first;

        const auto vw_ptr = tryGet(vw_name);
        if (!vw_ptr)
        {
            LOG_DEBUG(log, "Resource Group's VW {} no longer exists, adding to delete list", vw_name);
            deleted_vw_list.emplace_back(vw_name);
            continue;
        }

        vw_ptr->updateQueueInfo(server_id, it.second);
    }

}

AggQueryQueueMap VirtualWarehouseManager::getAggQueryQueueMap() const
{
    const auto vws = getAll();

    // Process all vws
    AggQueryQueueMap res;
    for (const auto & [name, vw] : vws)
    {
        auto server_agg_queue_info = vw->getAggQueueInfo();
        res.emplace(name, server_agg_queue_info);
    }
    return res;
}

}
