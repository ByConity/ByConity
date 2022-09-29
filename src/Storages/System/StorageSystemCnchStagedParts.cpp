#include <Storages/System/StorageSystemCnchStagedParts.h>

#include <Catalog/Catalog.h>
#include <CloudServices/CnchPartsHelper.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/System/CollectWhereClausePredicate.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}
NamesAndTypesList StorageSystemCnchStagedParts::getNamesAndTypes()
{
    return {
        {"partition", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"to_publish", std::make_shared<DataTypeUInt8>()},
        {"to_gc", std::make_shared<DataTypeUInt8>()},
        {"bytes_on_disk", std::make_shared<DataTypeUInt64>()},
        {"rows_count", std::make_shared<DataTypeUInt64>()},
        {"columns", std::make_shared<DataTypeString>()},
        {"marks_count", std::make_shared<DataTypeUInt64>()},
        {"ttl", std::make_shared<DataTypeString>()},
        {"commit_time", std::make_shared<DataTypeDateTime>()},
        {"columns_commit_time", std::make_shared<DataTypeDateTime>()},
        {"previous_version", std::make_shared<DataTypeUInt64>()},
        {"partition_id", std::make_shared<DataTypeString>()},
        {"bucket_number", std::make_shared<DataTypeInt64>()},
        {"hdfs_path", std::make_shared<DataTypeString>()},
    };
}

ColumnsDescription StorageSystemCnchStagedParts::getColumnsAndAlias()
{
    auto columns = ColumnsDescription(getNamesAndTypes());

    auto add_alias = [&](const String & alias_name, const String & column_name) {
        ColumnDescription column(alias_name, columns.get(column_name).type);
        column.default_desc.kind = ColumnDefaultKind::Alias;
        column.default_desc.expression = std::make_shared<ASTIdentifier>(column_name);
        columns.add(column);
    };

    /// Add aliases for column names for align with table system.parts.
    add_alias("bytes", "bytes_on_disk");
    add_alias("rows", "rows_count");

    return columns;
}

void StorageSystemCnchStagedParts::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    auto cnch_catalog = context->getCnchCatalog();
    if (context->getServerType() != ServerType::cnch_server || !cnch_catalog)
        throw Exception("Table system.cnch_staged_parts only support cnch_server", ErrorCodes::NOT_IMPLEMENTED);

    ASTPtr where_expression = query_info.query->as<ASTSelectQuery>()->where();
    std::map<String, String> column_to_value;

    DB::collectWhereClausePredicate(where_expression, column_to_value, context);
    // check for required structure of WHERE clause for cnch_staged_parts
    auto database_it = column_to_value.find("database");
    auto table_it = column_to_value.find("table");
    if (database_it == column_to_value.end() || table_it == column_to_value.end())
    {
        throw Exception(
            "Please check if the query has required columns and condition : 'database' AND 'table' .Eg. select * from "
            "system.cnch_staged_parts WHERE database = 'some_name' AND table = 'another_name'.... ",
            ErrorCodes::INCORRECT_QUERY);
    }

    TransactionCnchPtr cnch_txn = context->getCurrentTransaction();
    TxnTimestamp start_time = cnch_txn ? cnch_txn->getStartTime() : TxnTimestamp{context->getTimestamp()};

    DB::StoragePtr table = cnch_catalog->tryGetTable(*context, database_it->second, table_it->second, start_time);
    if (!table)
        return;

    auto * data = dynamic_cast<StorageCnchMergeTree *>(table.get());

    if (!data || !data->getInMemoryMetadataPtr()->hasUniqueKey())
        throw Exception("Table system.cnch_staged_parts only support CnchMergeTree unique engine", ErrorCodes::LOGICAL_ERROR);

    auto all_parts = CnchPartsHelper::toIMergeTreeDataPartsVector(cnch_catalog->getStagedParts(table, start_time));
    auto visible_parts = CnchPartsHelper::calcVisibleParts(all_parts, false);
    auto current_visible = visible_parts.cbegin();

    //get CNCH staged parts
    for (size_t i = 0, size = all_parts.size(); i != size; ++i)
    {
        if (all_parts[i]->deleted)
            continue;
        size_t col_num = 0;
        {
            WriteBufferFromOwnString out;
            all_parts[i]->partition.serializeText(*data, out, format_settings);
            res_columns[col_num++]->insert(out.str());
        }
        res_columns[col_num++]->insert(all_parts[i]->name);
        res_columns[col_num++]->insert(database_it->second);
        res_columns[col_num++]->insert(table_it->second);

        /// all_parts and visible_parts are both ordered by info.
        /// For each part within all_parts, we find the first part greater than or equal to it in visible_parts,
        /// which means the part is visible if they are equal to each other
        while (current_visible != visible_parts.cend() && (*current_visible)->info < all_parts[i]->info)
            ++current_visible;
        bool to_publish = current_visible != visible_parts.cend() && all_parts[i]->info == (*current_visible)->info;
        res_columns[col_num++]->insert(to_publish);
        res_columns[col_num++]->insert(!to_publish);

        res_columns[col_num++]->insert(all_parts[i]->getBytesOnDisk());
        res_columns[col_num++]->insert(all_parts[i]->rows_count);
        res_columns[col_num++]->insert(all_parts[i]->getColumnsPtr()->toString());
        res_columns[col_num++]->insert(all_parts[i]->getMarksCount());

        String ttl = std::to_string(all_parts[i]->ttl_infos.table_ttl.min) + "," + std::to_string(all_parts[i]->ttl_infos.table_ttl.max);
        res_columns[col_num++]->insert(ttl);

        // first 48 bits represent times
        res_columns[col_num++]->insert((all_parts[i]->commit_time.toUInt64() >> 18) / 1000);
        res_columns[col_num++]->insert((all_parts[i]->columns_commit_time.toUInt64() >> 18) / 1000);

        res_columns[col_num++]->insert(all_parts[i]->info.hint_mutation);
        res_columns[col_num++]->insert(all_parts[i]->info.partition_id);
        res_columns[col_num++]->insert(all_parts[i]->bucket_number);
        res_columns[col_num++]->insert(all_parts[i]->getFullPath());
    }
}
}
