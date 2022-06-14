#include <ResourceManagement/ResourceManagerController.h>

#include <Catalog/Catalog.h>
#include <Interpreters/Context.h>
#include <ResourceManagement/ResourceTracker.h>
#include <ResourceManagement/VirtualWarehouseManager.h>
#include <ResourceManagement/WorkerGroupManager.h>
#include <Core/UUIDHelpers.h>
#include <ResourceManagement/ElectionController.h>
#include <ResourceManagement/VirtualWarehouseType.h>

namespace DB::ErrorCodes
{
    extern const int KNOWN_WORKER_GROUP;
    extern const int RESOURCE_MANAGER_ILLEGAL_CONFIG;
    extern const int VIRTUAL_WAREHOUSE_NOT_INITIALIZED;
}

namespace DB::ResourceManagement
{
ResourceManagerController::ResourceManagerController(ContextPtr global_context_)
    : WithContext(global_context_), log(&Poco::Logger::get("ResourceManagerController"))
{
    resource_tracker = std::make_unique<ResourceTracker>(*this);
    vw_manager = std::make_unique<VirtualWarehouseManager>(*this);
    group_manager = std::make_unique<WorkerGroupManager>(*this);
    election_controller = std::make_unique<ElectionController>(*this);
}

ResourceManagerController::~ResourceManagerController()
{
}

Catalog::CatalogPtr ResourceManagerController::getCnchCatalog()
{
    return getContext()->getCnchCatalog();
}

void ResourceManagerController::createVWsFromConfig()
{
    Poco::Util::AbstractConfiguration::Keys config_keys;

    String prefix = "resource_manager.vws";
    config->keys(prefix, config_keys);
    String prefix_key;

    for (const String & key: config_keys)
    {
        prefix_key = prefix + "." + key;
        if (key.find("vw") == 0)
        {
            if (!config->has(prefix_key + ".name"))
            {
                LOG_WARNING(log, "Virtual Warehouse specified in config without name");
                continue;
            }
            String name = config->getString(prefix_key + ".name");
            if (!config->has(prefix_key + ".type"))
            {
                LOG_WARNING(log, "Virtual Warehouse " + name + " specified in config without type");
                continue;
            }
            if (!config->has(prefix_key + ".num_workers"))
            {
                LOG_WARNING(log, "Virtual Warehouse " + name + " specified in config without num_workers");
                continue;
            }
            LOG_DEBUG(log, "Found virtual warehouse " + name + " in config");
            if (vw_manager->tryGetVirtualWarehouse(name))
            {
                LOG_DEBUG(log, "Virtual warehouse " + name + " already exists, skipping creation");
                if (config->has(prefix_key + ".worker_groups"))
                    createWorkerGroupsFromConfig(prefix_key + ".worker_groups", name);
                continue;
            }
            VirtualWarehouseSettings vw_settings;
            auto type_str = config->getString(prefix_key + ".type", "Unknown");
            vw_settings.type = ResourceManagement::toVirtualWarehouseType(&type_str[0]);
            if (vw_settings.type == VirtualWarehouseType::Unknown)
            {
                LOG_WARNING(log, "Unknown virtual warehouse type " + type_str + ". Type should be of Read, Write, Task or Default");
                continue;
            }

            vw_settings.num_workers = config->getInt(prefix_key + ".num_workers", 0);
            vw_settings.min_worker_groups = config->getInt(prefix_key + ".min_worker_groups", 0);
            vw_settings.max_worker_groups = config->getInt(prefix_key + ".max_worker_groups", 0);
            vw_settings.max_concurrent_queries = config->getInt(prefix_key + ".max_concurrent_queries", 0);
            vw_settings.auto_suspend = config->getInt(prefix_key + ".auto_suspend", 0);
            vw_settings.auto_resume = config->getInt(prefix_key + ".auto_resume", 1);

            vw_manager->createVirtualWarehouse(name, vw_settings, false);
            if (config->has(prefix_key + ".worker_groups"))
                createWorkerGroupsFromConfig(prefix_key + ".worker_groups", name);
            LOG_DEBUG(log, "Created virtual warehouse " + name + " using config");
        }
    }

}

void ResourceManagerController::createWorkerGroupsFromConfig(const String & prefix, const String & vw_name)
{
    Poco::Util::AbstractConfiguration::Keys config_keys;

    config->keys(prefix, config_keys);
    String prefix_key;
    for (const String & key: config_keys)
    {
        prefix_key = prefix + "." + key;
        if (key.find("worker_group") == 0)
        {
            if (!config->has(prefix_key + ".name"))
            {
                LOG_WARNING(log, "Worker Group specified in config without name");
                continue;
            }
            String name = config->getString(prefix_key + ".name");
            if (group_manager->tryGetWorkerGroup(name))
            {
                LOG_DEBUG(log, "Worker group " + name + " already exists, skipping creation");
                continue;
            }
            if (!config->has(prefix_key + ".type"))
            {
                LOG_WARNING(log, "Worker Group " + name + " specified in config without type");
                continue;
            }
            WorkerGroupData group_data;
            group_data.id = name;
            auto type_str = config->getString(prefix_key + ".type", "Unknown");
            group_data.type = ResourceManagement::toWorkerGroupType(&type_str[0]);
            if (group_data.type == WorkerGroupType::Unknown)
            {
                LOG_WARNING(log, "Unknown worker group type " + type_str + ". Type should be of Physical, Shared or Composite");
                continue;
            }
            if (config->has(prefix_key + ".psm"))
                group_data.psm = config->getString(prefix_key + ".psm");
            if (config->has(prefix_key + ".shared_group"))
                group_data.linked_id = config->getString(prefix_key + ".shared_group");

            group_manager->createWorkerGroup(name, false, vw_name, group_data);
            LOG_DEBUG(log, "Created worker group " + name + " in Virtual Warehouse " + vw_name + " using config");
        }
    }
}

void ResourceManagerController::initialize()
{
    vw_manager->loadVirtualWarehouses();
    group_manager->loadWorkerGroups();
    createVWsFromConfig();

    auto vws = vw_manager->getAllVirtualWarehouses();
    auto groups = group_manager->getAllWorkerGroups();

    std::unordered_map<UUID, VirtualWarehousePtr> uuid_to_vw;
    for (auto & [_, vw] : vws)
        uuid_to_vw[vw->getUUID()] = vw;

    for (auto & [_, group] : groups)
    {
        auto vw_uuid = group->getVWUUID();

        if (vw_uuid == UUIDHelpers::Nil)
        {
            LOG_DEBUG(log, "Worker group {} doesn't belong to any virtual warehouse", group->getID());
            continue;
        }

        auto vw_it = uuid_to_vw.find(vw_uuid);
        if (vw_it != uuid_to_vw.end())
        {
            auto & vw = vw_it->second;
            vw->addWorkerGroup(group);
            group->setVWName(vw->getName());
            LOG_DEBUG(log, "Add worker group {} to virtual warehouse {}", group->getID(), vw->getName());
        }
        else
        {
            LOG_DEBUG(log, "Worker group {} belongs to a nonexistent virtual warehouse {}", group->getID(), toString(vw_uuid));
        }
    }
}

void ResourceManagerController::setConfig(const ConfigurationPtr & config_)
{
    config = config_;
}

void ResourceManagerController::registerWorkerNode(const WorkerNodeResourceData & data)
{
    if (data.worker_group_id.empty())
        throw Exception("The group_id of node must not be empty", ErrorCodes::KNOWN_WORKER_GROUP);

    auto [outdated, worker_node] = resource_tracker->registerNode(data);

    const auto & vw_name = worker_node->vw_name;
    const auto & worker_id = worker_node->getID();
    const auto & worker_group_id = worker_node->worker_group_id;

    auto group = group_manager->tryGetWorkerGroup(worker_group_id);
    if (!group)
    {
        auto vw = vw_manager->tryGetVirtualWarehouse(vw_name);
        if (!vw)
            throw Exception("Worker node's Virtual Warehouse `" + vw_name + "` has not been created", ErrorCodes::VIRTUAL_WAREHOUSE_NOT_INITIALIZED);

        // Create group if not found
        WorkerGroupData worker_group_data;
        worker_group_data.id = worker_group_id;
        worker_group_data.vw_name = vw_name;
        group = group_manager->createWorkerGroup(worker_group_id, false, vw_name, worker_group_data);
        vw->addWorkerGroup(group);
        group->setVWName(vw->getName());
    }

    if (outdated)
    {
        /// remove node from worker group first.
        LOG_WARNING(log, "Worker {} is outdated, Will remove it from WorkerGroup and ResourceTracker", worker_id);
        group->removeNode(worker_id);
        resource_tracker->removeNode(worker_id);

        /// re-register new node after removing outdated node.
        return registerWorkerNode(data);
    }
    else
    {
        group->registerNode(worker_node);
        LOG_DEBUG(log, "Node {} registered successfully in worker group {}", worker_node->getID(), group->getID());
    }

    worker_node->assigned = true;
}

void ResourceManagerController::removeWorkerNode(const std::string & worker_id, const std::string & vw_name, const std::string & group_id)
{
    if (group_id.empty() || vw_name.empty())
        throw Exception("The vw_name and group_id must not be empty.", ErrorCodes::LOGICAL_ERROR);

    auto group = group_manager->getWorkerGroup(group_id);
    group->removeNode(worker_id);
}

}
