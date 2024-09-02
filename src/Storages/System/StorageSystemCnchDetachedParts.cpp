#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeEnum.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/System/StorageSystemCnchDetachedParts.h>
#include <Storages/MergeTree/CnchAttachProcessor.h>
#include <Storages/System/CollectWhereClausePredicate.h>
#include <Storages/System/StorageSystemCnchCommon.h>

namespace DB
{

NamesAndTypesList StorageSystemCnchDetachedParts::getNamesAndTypes()
{
    auto type_enum = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"VisiblePart", static_cast<Int8>(PartType::VisiblePart)},
        {"InvisiblePart", static_cast<Int8>(PartType::InvisiblePart)}
    });

    return {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"table_uuid", std::make_shared<DataTypeUUID>()},
        {"partition_id", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"name", std::make_shared<DataTypeString>()},
        {"part_id", std::make_shared<DataTypeUUID>()},
        {"part_type", std::move(type_enum)},
        {"previous_version", std::make_shared<DataTypeUInt64>()},
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
    std::vector<std::tuple<String, String, String>> tables;

    const std::vector<std::map<String,Field>> value_by_column_names = collectWhereORClausePredicate(where_expression, context);
    bool enable_filter_by_table = false;
    bool enable_filter_by_partition = false;
    String only_selected_db;
    String only_selected_table;
    String only_selected_partition_id;

    if (value_by_column_names.size() == 1)
    {
        const auto & value_by_column_name = value_by_column_names.at(0);
        auto db_it = value_by_column_name.find("database");
        auto table_it = value_by_column_name.find("table");
        auto partition_it = value_by_column_name.find("partition_id");
        if ((db_it != value_by_column_name.end()) && (table_it != value_by_column_name.end()))
        {
            only_selected_db = db_it->second.getType() == Field::Types::String ? db_it->second.get<String>() : "";
            only_selected_table = table_it->second.getType() == Field::Types::String ? table_it->second.get<String>() : "";
            enable_filter_by_table = true;

            LOG_TRACE(&Poco::Logger::get("StorageSystemCnchDetachedParts"),
                    "filtering from catalog by table with db name {} and table name {}",
                    only_selected_db, only_selected_table);
        }

        if (partition_it != value_by_column_name.end())
        {
            only_selected_partition_id = partition_it->second.getType() == Field::Types::String ? partition_it->second.get<String>() : "";
            enable_filter_by_partition = true;

            LOG_TRACE(&Poco::Logger::get("StorageSystemCnchDetachedParts"),
                    "filtering from catalog by partition with partition name {}",
                    only_selected_partition_id);
        }
    }

    if (!(enable_filter_by_partition || enable_filter_by_table))
        LOG_TRACE(&Poco::Logger::get("StorageSystemCnchDetachedParts"), "No explicitly table and partition provided in where expression");

    // check for required structure of WHERE clause for cnch_parts
    if (!enable_filter_by_table)
    {
        if (!context->getSettingsRef().enable_multiple_tables_for_cnch_parts)
            throw Exception(
                "You should specify database and table in where cluster or set enable_multiple_tables_for_cnch_parts to enable visit "
                "multiple "
                "tables",
                ErrorCodes::BAD_ARGUMENTS);
        tables = filterTables(context, query_info);
    }
    else
    {
        const String & tenant_id = context->getTenantId();
        String only_selected_db_full = only_selected_db;
        if (!tenant_id.empty())
        {
            if (!DB::DatabaseCatalog::isDefaultVisibleSystemDatabase(only_selected_db))
            {
                only_selected_db_full = tenant_id + "." + only_selected_db;
            }
        }
        tables.emplace_back(
                only_selected_db_full,
                only_selected_db,
                only_selected_table
            );
    }

    /// Add one item into final results.
    auto add_item = [&](const String& striped_database, const StorageCnchMergeTree *storage, const MutableMergeTreeDataPartCNCHPtr part)
    {
        const FormatSettings format_settings;
        DiskType::Type remote_disk_type = storage->getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk()->getType();

        auto type = PartType::VisiblePart;
        for (IMergeTreeDataPartPtr curr_part = part; curr_part; curr_part = curr_part->tryGetPreviousPart())
        {
            size_t col_num = 0;
            res_columns[col_num++]->insert(striped_database);
            res_columns[col_num++]->insert(storage->getTableName());
            res_columns[col_num++]->insert(storage->getStorageUUID());
            {
                WriteBufferFromOwnString out;
                curr_part->get_partition().serializeText(*storage, out, format_settings);
                res_columns[col_num++]->insert(out.str());
            }
            res_columns[col_num++]->insert(curr_part->get_name());
            res_columns[col_num++]->insert(curr_part->get_uuid());
            res_columns[col_num++]->insert(static_cast<Int8>(type));
            res_columns[col_num++]->insert(curr_part->get_info().hint_mutation);
            res_columns[col_num++]->insert(DiskType::toString(remote_disk_type));
            res_columns[col_num++]->insert(curr_part->get_info().min_block);
            res_columns[col_num++]->insert(curr_part->get_info().max_block);
            res_columns[col_num++]->insert(curr_part->get_info().level);
            if (type == PartType::VisiblePart)
                type = PartType::InvisiblePart;
        }



    };

    TransactionCnchPtr cnch_txn = context->getCurrentTransaction();
    TxnTimestamp start_time = cnch_txn ? cnch_txn->getStartTime() : TxnTimestamp{context->getTimestamp()};

    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    for (const auto & [database_fullname, database_name, table_name] : tables)
    {
        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_fullname);
        if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_fullname, table_name))
            continue;

        auto table = cnch_catalog->tryGetTable(*context, database_fullname, table_name, start_time);

        /// Skip not exist table
        if (!table)
            continue;

        if (auto * cnch_merge_tree = dynamic_cast<StorageCnchMergeTree *>(table.get()))
        {
            PartitionCommand cmd;
            CnchAttachProcessor processor(*cnch_merge_tree, cmd, query_context);
            auto detached_parts = processor.getDetachedParts(AttachFilter::createPartsFilter());
            for (const auto & parts : detached_parts)
            {
                for (const auto & part : parts)
                {
                    add_item(database_name, cnch_merge_tree, part);
                }
            }
        }
    }
}

}
