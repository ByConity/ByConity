
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateWarehouseQuery.h>
#include <Parsers/ASTCreateWarehouseQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <ResourceManagement/CommonData.h>
#include <ResourceManagement/ResourceManagerClient.h>
#include <ResourceManagement/VirtualWarehouseType.h>



namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int RESOURCE_MANAGER_ERROR;
    extern const int RESOURCE_MANAGER_INCOMPATIBLE_SETTINGS;
    extern const int RESOURCE_MANAGER_UNKNOWN_SETTING;
    extern const int RESOURCE_MANAGER_WRONG_VW_SCHEDULE_ALGO;
}

InterpreterCreateWarehouseQuery::InterpreterCreateWarehouseQuery(const ASTPtr & query_ptr_, ContextPtr context_) 
    : WithContext(context_), query_ptr(query_ptr_) {}


BlockIO InterpreterCreateWarehouseQuery::execute()
{
    auto & create = query_ptr->as<ASTCreateWarehouseQuery &>();

    bool num_workers_in_settings(false);

    VirtualWarehouseSettings vw_settings;

    if (create.settings)
    {
        for (const auto & change : create.settings->changes)
        {
            if (change.name == "type")
            {
                auto value = change.value.safeGet<std::string>();
                vw_settings.type = ResourceManagement::toVirtualWarehouseType(&value[0]);
            }
            else if (change.name == "auto_suspend")
            {
                vw_settings.auto_suspend = change.value.safeGet<size_t>();
            }
            else if (change.name == "auto_resume")
            {
                vw_settings.auto_resume = change.value.safeGet<size_t>();
            }
            else if (change.name == "num_workers")
            {
                num_workers_in_settings = true;
                vw_settings.num_workers = change.value.safeGet<size_t>();
            }

            else if (change.name == "max_worker_groups")
            {
                vw_settings.max_worker_groups = change.value.safeGet<size_t>();
            }
            else if (change.name == "min_worker_groups")
            {
                vw_settings.min_worker_groups = change.value.safeGet<size_t>();
            }
            else if (change.name == "max_concurrent_queries")
            {
                vw_settings.max_concurrent_queries = change.value.safeGet<size_t>();
            }
            else if (change.name == "max_queued_queries")
            {
                vw_settings.max_queued_queries = change.value.safeGet<size_t>();
            }
            else if (change.name == "max_queued_waiting_ms")
            {
                vw_settings.max_queued_waiting_ms = change.value.safeGet<size_t>();
            }
            else if (change.name == "vw_schedule_algo")
            {
                auto value = change.value.safeGet<std::string>();
                auto algo = ResourceManagement::toVWScheduleAlgo(&value[0]);
                if (algo == ResourceManagement::VWScheduleAlgo::Unknown)
                    throw Exception("Wrong vw_schedule_algo: " + value, ErrorCodes::RESOURCE_MANAGER_WRONG_VW_SCHEDULE_ALGO);
                vw_settings.vw_schedule_algo = algo;
            }
            else
            {
                throw Exception("Unknown setting " + change.name, ErrorCodes::RESOURCE_MANAGER_UNKNOWN_SETTING);
            }
        }
    }

    if (!num_workers_in_settings)
    {
        throw Exception("Expected num_workers setting to be filled", ErrorCodes::SYNTAX_ERROR);
    }

    if (vw_settings.type == ResourceManagement::VirtualWarehouseType::Unknown)
        throw Exception("Unknown Virtual Warehouse type, expected type to be one of 'Read', 'Write', 'Task' or 'Default filled under SETTINGS'", ErrorCodes::RESOURCE_MANAGER_UNKNOWN_SETTING);

    if (vw_settings.min_worker_groups > vw_settings.max_worker_groups)
        throw Exception("min_worker_groups should be less than or equal to max_worker_groups", ErrorCodes::RESOURCE_MANAGER_INCOMPATIBLE_SETTINGS);

    auto client = getContext()->getResourceManagerClient();
    client->createVirtualWarehouse(create.name, vw_settings, create.if_not_exists);

    return {};
}

}
