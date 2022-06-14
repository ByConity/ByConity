#include <ResourceManagement/WorkerGroupManager.h>

#include <Catalog/Catalog.h>
#include <ResourceManagement/PhysicalWorkerGroup.h>
#include <ResourceManagement/ResourceManagerController.h>
#include <ResourceManagement/SharedWorkerGroup.h>
#include <ResourceManagement/VirtualWarehouseManager.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    const extern int LOGICAL_ERROR;
    const extern int BRPC_TIMEOUT;
    const extern int WORKER_GROUP_NOT_FOUND;
    const extern int WORKER_GROUP_ALREADY_EXISTS;
}
}

namespace DB::ResourceManagement
{
WorkerGroupManager::WorkerGroupManager(ResourceManagerController & rm_controller_)
    : rm_controller(rm_controller_), log(&Poco::Logger::get("WorkerGroupManager"))
{
}

void WorkerGroupManager::loadWorkerGroups()
{
    auto catalog = rm_controller.getCnchCatalog();

    LOG_DEBUG(log, "Loading worker groups...");

    auto data_vec = catalog->scanWorkerGroups();

    std::lock_guard lock(cells_mutex);

    for (auto & data : data_vec)
    {
        if (data.type != WorkerGroupType::Physical)
            continue;

        try
        {
            cells.try_emplace(data.id, createWorkerGroupObject(data, &lock));
            LOG_DEBUG(log, "Loaded physical worker group {}", data.id);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }

    for (auto & data : data_vec)
    {
        if (data.type != WorkerGroupType::Shared)
            continue;

        try
        {
            cells.try_emplace(data.id, createWorkerGroupObject(data, &lock));
            LOG_DEBUG(log, "Loaded shared worker group {}", data.id);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }

    LOG_INFO(log, "Loaded {} worker groups.", cells.size());
}

void WorkerGroupManager::clearWorkerGroups()
{
    std::lock_guard lock(cells_mutex);
    cells.clear();
}

WorkerGroupPtr WorkerGroupManager::createWorkerGroupObject(const WorkerGroupData & data, std::lock_guard<std::mutex> * lock)
{
    switch (data.type)
    {
        case WorkerGroupType::Physical:
            return std::make_shared<PhysicalWorkerGroup>(rm_controller.getContext(), data.id, data.vw_uuid, data.psm);

        case WorkerGroupType::Shared: {
            std::optional<std::lock_guard<std::mutex>> maybe_lock;
            if (!lock)
            {
                maybe_lock.emplace(cells_mutex);
                lock = &maybe_lock.value();
            }

            auto res = std::make_shared<SharedWorkerGroup>(data.id, data.vw_uuid, data.linked_id);

            if (auto linked_group = tryGetImpl(data.linked_id, *lock))
                res->setLinkedGroup(linked_group);

            return res;
        }

        default:
            return nullptr;
    }
}

WorkerGroupPtr WorkerGroupManager::createWorkerGroup(
    const std::string & id, bool if_not_exists, const std::string & vw_name, WorkerGroupData data)
{
    auto catalog = rm_controller.getCnchCatalog();

    VirtualWarehousePtr vw;
    if (!vw_name.empty())
    {
        vw = rm_controller.getVirtualWarehouseManager().getVirtualWarehouse(vw_name);
        data.vw_uuid = vw->getUUID();
    }

    auto creator = [&] {
        try
        {
            if (data.type == WorkerGroupType::Shared)
            {
                if (auto const & linked = tryGet(data.linked_id);
                    !linked || linked->getType() == WorkerGroupType::Shared)
                    throw Exception("Create shared worker group with error! linked_id: " + data.linked_id, ErrorCodes::LOGICAL_ERROR);
            }
            catalog->createWorkerGroup(id, data);
            LOG_DEBUG(log, "Created worker group {} in catalog", id);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::BRPC_TIMEOUT)
                need_sync_with_catalog.store(true, std::memory_order_relaxed);
            throw;
        }

        return createWorkerGroupObject(data);
    };

    auto [group, created] = getOrCreate(id, std::move(creator));
    if (!if_not_exists && !created)
        throw Exception("Worker group " + id + " already exists.", ErrorCodes::WORKER_GROUP_ALREADY_EXISTS);

    return group;
}

WorkerGroupPtr WorkerGroupManager::tryGetWorkerGroup(const std::string & id)
{
    auto res = tryGet(id);
    if (!res && need_sync_with_catalog.load(std::memory_order_relaxed))
    {
        auto catalog = rm_controller.getCnchCatalog();

        WorkerGroupData data;
        if (!catalog->tryGetWorkerGroup(id, data))
            return nullptr;

        res = getOrCreate(id, [&] { return createWorkerGroupObject(data); }).first;

        // TODO: Shift VW-related logic to RMController
        if (!data.vw_name.empty())
        {
            /// TODO: get by UUID when support RENAME VW
            auto vw = rm_controller.getVirtualWarehouseManager().getVirtualWarehouse(data.vw_name);
            vw->addWorkerGroup(res);
            res->setVWName(vw->getName());
        }
    }
    return res;
}

WorkerGroupPtr WorkerGroupManager::getWorkerGroup(const std::string & id)
{
    auto res = tryGetWorkerGroup(id);
    if (!res)
        throw Exception("Worker group `" + id + "` nod found", ErrorCodes::WORKER_GROUP_NOT_FOUND);
    return res;
}

void WorkerGroupManager::dropWorkerGroup(const std::string & id, bool if_exists)
{
    /// TODO: (zuochuang.zema, Derrick) the order issue.
    ///  up to down: vw_manager > wg_manager > catalog.
    auto group = tryGetWorkerGroup(id);
    if (!group)
    {
        if (if_exists)
            return;
        else
            throw Exception("Worker group `" + id + "` not found", ErrorCodes::WORKER_GROUP_NOT_FOUND);
    }

    // TODO: Shift VW-related logic to RMController
    auto vw_name = group->getVWName();
    if (!vw_name.empty())
    {
        auto vw = rm_controller.getVirtualWarehouseManager().getVirtualWarehouse(vw_name);
        vw->removeGroup(id);
    }

    auto catalog = rm_controller.getCnchCatalog();
    try
    {
        catalog->dropWorkerGroup(id);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::BRPC_TIMEOUT)
            need_sync_with_catalog.store(true, std::memory_order_relaxed);

        throw;
    }

    erase(id);
}
}
