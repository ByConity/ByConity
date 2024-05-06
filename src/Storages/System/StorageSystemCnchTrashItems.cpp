#include <Catalog/Catalog.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/System/CollectWhereClausePredicate.h>
#include <Storages/System/StorageSystemCnchCommon.h>
#include <Storages/System/StorageSystemCnchTrashItems.h>

namespace DB
{

NamesAndTypesList StorageSystemCnchTrashItems::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeString>()},
        {"partition_id", std::make_shared<DataTypeString>()},
        {"type", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"bytes_on_disk", std::make_shared<DataTypeUInt64>()},
        {"commit_time", std::make_shared<DataTypeDateTime>()},
        {"commit_ts", std::make_shared<DataTypeUInt64>()},
        {"end_ts", std::make_shared<DataTypeUInt64>()},
    };
}

void StorageSystemCnchTrashItems::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    auto cnch_catalog = context->getCnchCatalog();

    ASTPtr where_expression = query_info.query->as<ASTSelectQuery>()->where();
    std::map<String, String> column_to_value;

    collectWhereClausePredicate(where_expression, column_to_value);

    /// Check if table is specified.
    std::vector<StoragePtr> request_storages;
    std::vector<std::pair<String, String>> tables;
    auto database_it = column_to_value.find("database");
    auto table_it = column_to_value.find("table");
    auto uuid_it = column_to_value.find("uuid");

    TransactionCnchPtr cnch_txn = context->getCurrentTransaction();
    TxnTimestamp start_time = cnch_txn ? cnch_txn->getStartTime() : TxnTimestamp{context->getTimestamp()};

    /// Filter out the table we need in one way or another.
    if (uuid_it != column_to_value.end())
    {
        request_storages.push_back(cnch_catalog->tryGetTableByUUID(*context, uuid_it->second, start_time));
    }
    else if (database_it != column_to_value.end() && table_it != column_to_value.end())
    {
        request_storages.push_back(cnch_catalog->tryGetTable(*context, database_it->second, table_it->second, start_time));
    }
    else
    {
        auto filtered_tables = filterTables(context, query_info);
        for (const auto & [database_fullname, database_name, table_name] : filtered_tables)
            request_storages.push_back(cnch_catalog->tryGetTable(*context, database_fullname, table_name, start_time));
    }

    /// Add one item into final results.
    auto add_item = [&](const StoragePtr & storage, const ServerDataPartPtr & part, const DeleteBitmapMetaPtr & delete_bitmap) {
        size_t col_num = 0;
        res_columns[col_num++]->insert(storage->getDatabaseName());
        res_columns[col_num++]->insert(storage->getTableName());
        res_columns[col_num++]->insert(UUIDHelpers::UUIDToString(storage->getStorageUUID()));
        if (part)
        {
            res_columns[col_num++]->insert(part->info().partition_id);
            res_columns[col_num++]->insert("PART");
            res_columns[col_num++]->insert(part->name());
            res_columns[col_num++]->insert(part->size());
            res_columns[col_num++]->insert(TxnTimestamp(part->getCommitTime()).toSecond());
            res_columns[col_num++]->insert(part->getCommitTime());
            res_columns[col_num++]->insert(part->getEndTime());
        }
        else if (delete_bitmap)
        {
            auto model = delete_bitmap->getModel();
            res_columns[col_num++]->insert(model->partition_id());
            res_columns[col_num++]->insert("DELETE_BITMAP");
            res_columns[col_num++]->insert(delete_bitmap->getNameForLogs());
            res_columns[col_num++]->insert(model->file_size());
            res_columns[col_num++]->insert(TxnTimestamp(delete_bitmap->getCommitTime()).toSecond());
            res_columns[col_num++]->insert(delete_bitmap->getCommitTime());
            res_columns[col_num++]->insert(delete_bitmap->getEndTime());
        }
    };

    /// Loop every table requested, retrieve all trashed item in them.
    for (const auto & storage : request_storages)
    {
        if (!storage)
            continue;


        if (auto * cnch_merge_tree = dynamic_cast<StorageCnchMergeTree *>(storage.get()))
        {
            auto trash_items = cnch_catalog->getDataItemsInTrash(storage);

            if (trash_items.empty())
                continue;

            for (const auto & part : trash_items.data_parts)
                add_item(storage, part, nullptr);

            for (const auto & delete_bitmap : trash_items.delete_bitmaps)
                add_item(storage, nullptr, delete_bitmap);
        }
        else if (!context->getSettingsRef().enable_skip_non_cnch_tables_for_cnch_trash_items)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Table system.cnch_trash_itesm only support CnchMergeTree engine, but got `{}`. "
                "Consider enable `enable_skip_non_cnch_tables_for_cnch_trash_items` to skip non CnchMergeTree engine.",
                storage->getName());
    }
}

}
