#include <Catalog/Catalog.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSelectQuery.h>
#include <Protos/cnch_common.pb.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/System/CollectWhereClausePredicate.h>
#include <Storages/System/StorageSystemCnchAsyncQueries.h>
#include <common/logger_useful.h>


namespace DB
{

NamesAndTypesList StorageSystemCnchAsyncQueries::getNamesAndTypes()
{
    return {
        {"async_query_id", std::make_shared<DataTypeString>()},
        {"query_id", std::make_shared<DataTypeString>()},
        {"status", std::make_shared<DataTypeString>()},
        {"error_msg", std::make_shared<DataTypeString>()},
        {"start_time", std::make_shared<DataTypeDateTime>()},
        {"update_time", std::make_shared<DataTypeDateTime>()},
        {"max_execution_time", std::make_shared<DataTypeUInt64>()},
    };
}

void StorageSystemCnchAsyncQueries::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    ASTPtr where_expression = query_info.query->as<ASTSelectQuery>()->where();

    const std::vector<std::map<String, Field>> value_by_column_names = collectWhereORClausePredicate(where_expression, context);

    if (value_by_column_names.size() == 1)
    {
        const auto & value_by_column_name = value_by_column_names.at(0);
        auto it = value_by_column_name.find("async_query_id");
        if (it != value_by_column_name.end())
        {
            String selected_id = it->second.safeGet<String>();
            Protos::AsyncQueryStatus data;
            if (context->getCnchCatalog()->tryGetAsyncQueryStatus(selected_id, data))
            {
                size_t i = 0;
                res_columns[i++]->insert(data.id());
                res_columns[i++]->insert(data.query_id());
                res_columns[i++]->insert(AsyncQueryStatus_Status_Name(data.status()));
                res_columns[i++]->insert(data.error_msg());
                res_columns[i++]->insert(static_cast<UInt64>(data.start_time()));
                res_columns[i++]->insert(static_cast<UInt64>(data.update_time()));
                res_columns[i++]->insert(static_cast<UInt64>(data.max_execution_time()));
            }
            else
            {
                LOG_TRACE(getLogger(getName()), "return empty result with async_query_id {}", selected_id);
            }
        }
        else
        {
            LOG_TRACE(getLogger(getName()), "doesn't do any filtering");
        }
    }
}
}
