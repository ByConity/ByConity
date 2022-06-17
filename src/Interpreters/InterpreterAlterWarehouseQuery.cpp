
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/InterpreterAlterWarehouseQuery.h>
#include <Parsers/ASTAlterWarehouseQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <ResourceManagement/CommonData.h>
#include <ResourceManagement/ResourceManagerClient.h>
#include <ResourceManagement/VirtualWarehouseType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_MANAGER_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int RESOURCE_MANAGER_UNKNOWN_SETTING;
    extern const int RESOURCE_MANAGER_INCOMPATIBLE_SETTINGS;
    extern const int RESOURCE_MANAGER_WRONG_VW_SCHEDULE_ALGO;

}

InterpreterAlterWarehouseQuery::InterpreterAlterWarehouseQuery(const ASTPtr & query_ptr_, ContextPtr context_)
    : WithContext(context_), query_ptr(query_ptr_) {}


BlockIO InterpreterAlterWarehouseQuery::execute()
{
    auto & alter = query_ptr->as<ASTAlterWarehouseQuery &>();

    if (!alter.rename_to.empty())
        throw Exception("Renaming VirtualWarehouse is currently unsupported.", ErrorCodes::LOGICAL_ERROR);

    ResourceManagement::VirtualWarehouseAlterSettings vw_alter_settings;

    if (alter.settings)
    {
        for (const auto & change : alter.settings->changes)
        {
            if (change.name == "type")
            {
                using RMType = ResourceManagement::VirtualWarehouseType;
                auto value = change.value.safeGet<std::string>();
                auto type = RMType(ResourceManagement::toVirtualWarehouseType(&value[0]));
                if (type == RMType::Unknown)
                    throw Exception("Unknown Virtual Warehouse type: " + value, ErrorCodes::RESOURCE_MANAGER_UNKNOWN_SETTING);
                vw_alter_settings.type = type;
            }
            else if (change.name == "auto_suspend")
            {
                vw_alter_settings.auto_suspend = change.value.safeGet<size_t>();
            }
            else if (change.name == "auto_resume")
            {
                vw_alter_settings.auto_resume = change.value.safeGet<size_t>();
            }
            else if (change.name == "num_workers")
            {
                vw_alter_settings.num_workers = change.value.safeGet<size_t>();
            }
            else if (change.name == "min_worker_groups")
            {
                vw_alter_settings.min_worker_groups = change.value.safeGet<size_t>();
            }
            else if (change.name == "max_worker_groups")
            {
                vw_alter_settings.max_worker_groups = change.value.safeGet<size_t>();
            }
            else if (change.name == "max_concurrent_queries")
            {
                vw_alter_settings.max_concurrent_queries = change.value.safeGet<size_t>();
            }
            else if (change.name == "max_queued_queries")
            {
                vw_alter_settings.max_queued_queries = change.value.safeGet<size_t>();
            }
            else if (change.name == "max_queued_waiting_ms")
            {
                vw_alter_settings.max_queued_waiting_ms = change.value.safeGet<size_t>();
            }
            else if (change.name == "vw_schedule_algo")
            {
                auto value = change.value.safeGet<std::string>();
                auto algo = ResourceManagement::toVWScheduleAlgo(&value[0]);
                if (algo == ResourceManagement::VWScheduleAlgo::Unknown)
                    throw Exception("Wrong vw_schedule_algo: " + value, ErrorCodes::RESOURCE_MANAGER_WRONG_VW_SCHEDULE_ALGO);
                vw_alter_settings.vw_schedule_algo = algo;
            }
            else
            {
                throw Exception("Unknown setting " + change.name, ErrorCodes::RESOURCE_MANAGER_UNKNOWN_SETTING);
            }
        }
    }

    if (vw_alter_settings.min_worker_groups && vw_alter_settings.max_worker_groups
        && vw_alter_settings.min_worker_groups > vw_alter_settings.max_worker_groups)
        throw Exception("min_worker_groups should be less than or equal to max_worker_groups", ErrorCodes::RESOURCE_MANAGER_INCOMPATIBLE_SETTINGS);

    // auto client = getContext()->getResourceManagerClient();
    // client->updateVirtualWarehouse(vw_name, vw_alter_settings);

    return {};
}

}
