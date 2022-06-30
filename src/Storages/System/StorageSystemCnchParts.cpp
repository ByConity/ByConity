#include <Catalog/Catalog.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSelectQuery.h>
#include <CloudServices/CnchPartsHelper.h>
#include <Storages/System/StorageSystemCnchParts.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Transaction/ICnchTransaction.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <TSO/TSOClient.h>
#include <fmt/format.h>
#include <map>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}
NamesAndTypesList StorageSystemCnchParts::getNamesAndTypes()
{
    return {
        {"partition", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"bytes_on_disk", std::make_shared<DataTypeUInt64>()},
        {"rows_count", std::make_shared<DataTypeUInt64>()},
        {"columns", std::make_shared<DataTypeString>()},
        {"marks_count", std::make_shared<DataTypeUInt64>()},
        {"ttl", std::make_shared<DataTypeString>()},
        {"commit_time", std::make_shared<DataTypeDateTime>()},
        {"kv_commit_time", std::make_shared<DataTypeDateTime>()},
        {"columns_commit_time", std::make_shared<DataTypeDateTime>()},
        {"mutation_commit_time", std::make_shared<DataTypeDateTime>()},
        {"previous_version", std::make_shared<DataTypeUInt64>()},
        {"partition_id", std::make_shared<DataTypeString>()},
        {"visible", std::make_shared<DataTypeUInt8>()},
        {"bucket_number", std::make_shared<DataTypeInt64>()},
        {"checksums_file_pos", std::make_shared<DataTypeString>()},
        {"index_file_pos", std::make_shared<DataTypeString>()},
    };
}

ColumnsDescription StorageSystemCnchParts::getColumnsAndAlias()
{
    auto columns = ColumnsDescription(getNamesAndTypes());

    auto add_alias = [&](const String & alias_name, const String & column_name)
    {
        ColumnDescription column(alias_name, columns.get(column_name).type);
        column.default_desc.kind = ColumnDefaultKind::Alias;
        column.default_desc.expression = std::make_shared<ASTIdentifier>(column_name);
        columns.add(column);
    };

    /// Add aliases for column names for align with table system.parts.
    add_alias("bytes", "bytes_on_disk");
    add_alias("rows", "rows_count");
    add_alias("active", "visible");

    return columns;
}

void StorageSystemCnchParts::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    Catalog::CatalogPtr cnch_catalog = context->getCnchCatalog();
    if (context->getServerType() != ServerType::cnch_server || !cnch_catalog)
        throw Exception("Table system.cnch_parts only support cnch_server", ErrorCodes::NOT_IMPLEMENTED);

    ASTPtr where_expression = query_info.query->as<ASTSelectQuery>()->where();
    std::map<String, String> columnToValue;

    DB::collectWhereClausePredicate(where_expression, columnToValue);
    // check for required structure of WHERE clause for cnch_parts
    auto database_it = columnToValue.find("database");
    auto table_it = columnToValue.find("table");
    if (database_it == columnToValue.end() || table_it == columnToValue.end())
    {
        throw Exception(
            "Please check if the query has required columns and condition : 'database' AND 'table' .Eg. select * from "
            "cnch_parts WHERE database = 'some_name' AND table = 'another_name'.... ", ErrorCodes::INCORRECT_QUERY);
    }

    TransactionCnchPtr cnch_txn = context->getCurrentTransaction();
    TxnTimestamp start_time = cnch_txn ? cnch_txn->getStartTime() : TxnTimestamp{context->getTimestamp()};

    DB::StoragePtr table = cnch_catalog->tryGetTable(*context, database_it->second, table_it->second, start_time);
    if (!table)
        return;

    auto * data = dynamic_cast<StorageCnchMergeTree *>(table.get());

    if (!data)
        throw Exception("Table system.cnch_parts only support CnchMergeTree engine", ErrorCodes::LOGICAL_ERROR);

    auto all_parts = cnch_catalog->getAllServerDataParts(table, start_time, nullptr);
    auto visible_parts = CnchPartsHelper::calcVisibleParts(all_parts, false);
    auto current_visible = visible_parts.cbegin();

    //get CNCH parts
    for (size_t i = 0, size = all_parts.size(); i != size; ++i)
    {
        size_t col_num = 0;
        {
            WriteBufferFromOwnString out;
            /// FIXME: if StorageCnchMergetree derived from MergeTreeMetaBase
            // all_parts[i]->partition().serializeText(*data, out, format_settings);
            res_columns[col_num++]->insert(out.str());
        }
        res_columns[col_num++]->insert(all_parts[i]->name());
        res_columns[col_num++]->insert(database_it->second);
        res_columns[col_num++]->insert(table_it->second);
        res_columns[col_num++]->insert(all_parts[i]->part_model().size());
        res_columns[col_num++]->insert(all_parts[i]->part_model().rows_count());
        res_columns[col_num++]->insert(all_parts[i]->part_model().columns());
        res_columns[col_num++]->insert(all_parts[i]->part_model().marks_count());

        res_columns[col_num++]->insert("");

        // first 48 bits represent times
        res_columns[col_num++]->insert((all_parts[i]->getCommitTime() >> 18) / 1000);
        res_columns[col_num++]->insert((all_parts[i]->part_model().commit_time() >> 18) / 1000); 
        res_columns[col_num++]->insert((UInt64(all_parts[i]->part_model().columns_commit_time()) >> 18) / 1000);
        res_columns[col_num++]->insert(TxnTimestamp(all_parts[i]->part_model().mutation_commit_time()).toSecond());

        res_columns[col_num++]->insert(all_parts[i]->info().hint_mutation);
        res_columns[col_num++]->insert(all_parts[i]->info().partition_id);

        /// all_parts and visible_parts are both ordered by info.
        /// For each part within all_parts, we find the first part greater than or equal to it in visible_parts,
        /// which means the part is visible if they are equal to each other
        while (current_visible != visible_parts.cend() && (*current_visible)->info() < all_parts[i]->info()) ++current_visible;
        res_columns[col_num++]->insert(current_visible != visible_parts.cend() && all_parts[i]->info() == (*current_visible)->info());

        res_columns[col_num++]->insert(all_parts[i]->part_model().bucket_number());

        off_t checksums_file_offset = 0; size_t checksums_file_size = 0;
        off_t index_file_offset = 0; size_t index_file_size = 0;
        if (all_parts[i]->part_model().has_meta_files_pos())
        {
            const auto & meta_files_pos = all_parts[i]->part_model().meta_files_pos();
            checksums_file_offset = meta_files_pos.checksums_file_offset();
            checksums_file_size = meta_files_pos.checksums_file_size();
            index_file_offset = meta_files_pos.index_file_offset();
            index_file_offset = meta_files_pos.index_file_size();
        }
        res_columns[col_num++]->insert(fmt::format("offset:{}, size:{}", checksums_file_offset, checksums_file_size));
        res_columns[col_num++]->insert(fmt::format("offset:{}, size:{}", index_file_offset, index_file_size));

    }
}

}
