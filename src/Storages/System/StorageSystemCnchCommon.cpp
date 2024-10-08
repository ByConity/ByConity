#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Protos/RPCHelpers.h>
#include <Storages/System/StorageSystemCnchCommon.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/Status.h>

std::optional<String> DB::filterAndStripDatabaseNameIfTenanted(const String & tenant_id, const String & database_name)
{
    std::optional<String> stripped_database_name;
    do
    {
    if (!tenant_id.empty())
    {
        if (startsWith(database_name, tenant_id + "."))
        {
            stripped_database_name.emplace(DB::getOriginalDatabaseName(database_name, tenant_id));
        }
        else
        {
            // Will skip database of other tenants and default user (without tenantid prefix)
            if (database_name.find(".") != std::string::npos)
                break;

            if (!DB::DatabaseCatalog::isDefaultVisibleSystemDatabase(database_name))
                break;
            stripped_database_name.emplace(database_name);
        }
    }
    else
    {
        stripped_database_name = database_name;
    }
    } while (false);

    return stripped_database_name;
}

std::vector<std::tuple<String, String, String>> DB::filterTables(const ContextPtr & context, const SelectQueryInfo & query_info)
{
    auto catalog = context->getCnchCatalog();
    auto table_models = catalog->getAllTables();

    Block block_to_filter;

    MutableColumnPtr database_fullname_column = ColumnString::create();
    MutableColumnPtr database_column = ColumnString::create();
    MutableColumnPtr table_name_column = ColumnString::create();
    MutableColumnPtr table_uuid_column = ColumnUUID::create();

    const String & tenant_id = context->getTenantId();

    for (const auto & table_model : table_models)
    {

        if (Status::isDeleted(table_model.status()))
            continue;
        const String & database_name = table_model.database();
        std::optional<String> stripped_database_name = filterAndStripDatabaseNameIfTenanted(tenant_id, database_name);
        if (!stripped_database_name.has_value())
            continue;
        database_fullname_column->insert(database_name);
        database_column->insert(std::move(*stripped_database_name));
        table_name_column->insert(table_model.name());
        table_uuid_column->insert(RPCHelpers::createUUID(table_model.uuid()));
    }
    block_to_filter.insert(ColumnWithTypeAndName(std::move(database_fullname_column), std::make_shared<DataTypeString>(), "database_fullname"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(database_column), std::make_shared<DataTypeString>(), "database"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(table_name_column), std::make_shared<DataTypeString>(), "table_name"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(table_uuid_column), std::make_shared<DataTypeUUID>(), "table_uuid"));

    VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);

    if (!block_to_filter.rows())
        return {};

    std::vector<std::tuple<String, String, String>> res;

    auto database_fullname_column_res = block_to_filter.getByName("database_fullname").column;
    auto database_column_res = block_to_filter.getByName("database").column;
    auto table_name_column_res = block_to_filter.getByName("table_name").column;
    for (size_t i = 0; i < database_column_res->size(); ++i)
        res.emplace_back((*database_fullname_column_res)[i].get<String>(), (*database_column_res)[i].get<String>(), (*table_name_column_res)[i].get<String>());

    LOG_DEBUG(getLogger("SystemCnchParts"), "Got {} tables from catalog after filter", res.size());
    return res;
}
