#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Protos/RPCHelpers.h>
#include <Storages/System/StorageSystemCnchCommon.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/Status.h>

std::vector<std::pair<String, String>> DB::filterTables(const ContextPtr & context, const SelectQueryInfo & query_info)
{
    auto catalog = context->getCnchCatalog();
    auto table_models = catalog->getAllTables();

    Block block_to_filter;

    MutableColumnPtr database_column = ColumnString::create();
    MutableColumnPtr table_name_column = ColumnString::create();
    MutableColumnPtr table_uuid_column = ColumnUUID::create();

    for (const auto & table_model : table_models)
    {
        if (Status::isDeleted(table_model.status()))
            continue;

        database_column->insert(table_model.database());
        table_name_column->insert(table_model.name());
        table_uuid_column->insert(RPCHelpers::createUUID(table_model.uuid()));
    }

    block_to_filter.insert(ColumnWithTypeAndName(std::move(database_column), std::make_shared<DataTypeString>(), "database"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(table_name_column), std::make_shared<DataTypeString>(), "table_name"));
    block_to_filter.insert(ColumnWithTypeAndName(std::move(table_uuid_column), std::make_shared<DataTypeUUID>(), "table_uuid"));

    VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);

    if (!block_to_filter.rows())
        return {};

    std::vector<std::pair<String, String>> res;

    auto database_column_res = block_to_filter.getByName("database").column;
    auto table_name_column_res = block_to_filter.getByName("table_name").column;
    for (size_t i = 0; i < database_column_res->size(); ++i)
        res.emplace_back((*database_column_res)[i].get<String>(), (*table_name_column_res)[i].get<String>());

    LOG_DEBUG(&Poco::Logger::get("SystemCnchParts"), "Got {} tables from catalog after filter", res.size());
    return res;
}
