#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/System/StorageSystemCnchDetachedParts.h>
#include <Storages/MergeTree/CnchAttachProcessor.h>
#include <Storages/System/CollectWhereClausePredicate.h>
#include <Storages/System/StorageSystemCnchCommon.h>

namespace DB
{

NamesAndTypesList StorageSystemCnchDetachedParts::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"partition_id", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"name", std::make_shared<DataTypeString>()},
        {"part_id", std::make_shared<DataTypeUUID>()},
        {"disk", std::make_shared<DataTypeString>()},
        {"min_block_number", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"max_block_number", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"level", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>())}
    };
}

void StorageSystemCnchDetachedParts::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{

   auto query_context = Context::createCopy(context);

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
    auto add_item = [&](const StorageCnchMergeTree *storage, const MutableMergeTreeDataPartCNCHPtr part) 
    {
        const FormatSettings format_settings;
        DiskType::Type remote_disk_type = storage->getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk()->getType();

        size_t col_num = 0;
        res_columns[col_num++]->insert(storage->getDatabaseName());
        res_columns[col_num++]->insert(storage->getTableName());
        {
            WriteBufferFromOwnString out;
            part->get_partition().serializeText(*storage, out, format_settings);
            res_columns[col_num++]->insert(out.str());
        }
        res_columns[col_num++]->insert(part->get_name());
        res_columns[col_num++]->insert(part->get_uuid());
        res_columns[col_num++]->insert(DiskType::toString(remote_disk_type));
        res_columns[col_num++]->insert(part->get_info().min_block);
        res_columns[col_num++]->insert(part->get_info().max_block);
        res_columns[col_num++]->insert(part->get_info().level);
    };

    /// Loop every table requested, retrieve all detached item in them.
    for (const auto & storage : request_storages)
    {
        if (!storage)
            continue;

        if (auto * cnch_merge_tree = dynamic_cast<StorageCnchMergeTree *>(storage.get()))
        {
            PartitionCommand cmd;
            CnchAttachProcessor processor(*cnch_merge_tree, cmd, query_context);
            auto detached_parts = processor.getDetachedParts(AttachFilter::createPartsFilter());
            for (const auto & parts : detached_parts)
            {
                for (const auto & part : parts)
                {
                    add_item(cnch_merge_tree, part);
                }
            }
        }
    }
}

}
