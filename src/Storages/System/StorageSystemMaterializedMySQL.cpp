#include <Storages/System/StorageSystemMaterializedMySQL.h>
#if USE_MYSQL

#include <Columns/ColumnString.h>
#include <Core/Names.h>
#include <CloudServices/CnchBGThreadsMap.h>
#include <Databases/MySQL/DatabaseCnchMaterializedMySQL.h>
#include <Databases/MySQL/MaterializedMySQLSyncThreadManager.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/System/TenantController.h>

namespace DB
{

StorageSystemMaterializedMySQL::StorageSystemMaterializedMySQL(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({
        { "mysql_info",                 std::make_shared<DataTypeString>() },
        { "mysql_database",             std::make_shared<DataTypeString>()},
        { "database",                   std::make_shared<DataTypeString>() },
        { "uuid",                       std::make_shared<DataTypeString>() },
        { "sync_type",                  std::make_shared<DataTypeString>() },
        { "include_tables",             std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "exclude_tables",             std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "resync_tables",              std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "total_position",             std::make_shared<DataTypeString>() },

        { "sync_threads_number",        std::make_shared<DataTypeUInt32>() },
        { "sync_tables_list",           std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "sync_thread_names",          std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "sync_thread_clients",        std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "sync_thread_binlog",         std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "broken_sync_threads",        std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },

        { "last_exception",             std::make_shared<DataTypeString>() },
        { "sync_failed_tables",         std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "skipped_unsupported_tables", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
    }));
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemMaterializedMySQL::read(
    const Names & /* column_names */,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    DISABLE_VISIT_FOR_TENANTS();
    auto strings_2_array = [] (Strings & elem_list) -> Array {
        Array arr;
        for (const auto & elem : elem_list)
            arr.emplace_back(elem);
        return arr;
    };

    auto nameset_2_array = [] (NameSet & elem_list) -> Array {
        Array arr;
        for (const auto & elem : elem_list)
            arr.emplace_back(elem);
        return arr;
    };

    auto catalog_client = context->getCnchCatalog();
    if (context->getServerType() != ServerType::cnch_server || !catalog_client)
        throw Exception("Table system.materialized_mysql is only supported in server", ErrorCodes::LOGICAL_ERROR);

    auto bg_threads = context->getCnchBGThreadsMap(CnchBGThreadType::MaterializedMySQL)->getAll();

    MutableColumnPtr col_database_mut = ColumnString::create();

    for (const auto & thread : bg_threads)
    {
        const auto & storage_id = thread.second->getStorageID();
        col_database_mut->insert(storage_id.database_name);
    }

    ColumnPtr col_database(std::move(col_database_mut));
    /// Determine what tables are needed by the conditions in the query.
    {
        Block filtered_block
            {
                { col_database, std::make_shared<DataTypeString>(), "database" },
            };

        VirtualColumnUtils::filterBlockWithQuery(query_info.query, filtered_block, context);

        if (!filtered_block.rows())
            return Pipe();

        col_database = filtered_block.getByName("database").column;
    }

    MutableColumns res_columns = getInMemoryMetadataPtr()->getSampleBlock().cloneEmptyColumns();

    for (size_t i = 0; i < col_database->size(); ++i)
    {
        String database = (*col_database)[i].safeGet<String>();
        auto database_ptr = DatabaseCatalog::instance().tryGetDatabase(database, context);
        if (!database_ptr)
            continue;

        if (auto * materialized_mysql = dynamic_cast<DatabaseCnchMaterializedMySQL*>(database_ptr.get()))
        {
            auto thread = bg_threads.find(materialized_mysql->getStorageID().uuid);
            if (thread == bg_threads.end())
                continue;

            auto * manager = dynamic_cast<MaterializedMySQLSyncThreadManager*>(thread->second.get());
            if (!manager)
                continue;

            auto status_info = manager->getSyncStatusInfo();

            size_t col_num = 0;
            res_columns[col_num++]->insert(status_info.mysql_info);
            res_columns[col_num++]->insert(status_info.mysql_database);
            res_columns[col_num++]->insert(database);
            res_columns[col_num++]->insert(UUIDHelpers::UUIDToString(materialized_mysql->getStorageID().uuid));
            res_columns[col_num++]->insert(status_info.sync_type);
            res_columns[col_num++]->insert(nameset_2_array(status_info.include_tables));
            res_columns[col_num++]->insert(nameset_2_array(status_info.exclude_tables));
            res_columns[col_num++]->insert(nameset_2_array(status_info.resync_tables));
            res_columns[col_num++]->insert(status_info.total_position);
            res_columns[col_num++]->insert(status_info.threads_num);
            if (status_info.threads_num > 0)
            {
                res_columns[col_num++]->insert(strings_2_array(status_info.materialized_tables));
                res_columns[col_num++]->insert(strings_2_array(status_info.sync_thread_names));
                res_columns[col_num++]->insert(strings_2_array(status_info.sync_thread_clients));
                res_columns[col_num++]->insert(strings_2_array(status_info.binlog_infos));
                res_columns[col_num++]->insert(strings_2_array(status_info.broken_threads));
            }
            else
            {
                res_columns[col_num++]->insertDefault();
                res_columns[col_num++]->insertDefault();
                res_columns[col_num++]->insertDefault();
                res_columns[col_num++]->insertDefault();
                res_columns[col_num++]->insertDefault();
            }

            res_columns[col_num++]->insert(status_info.last_exception);
            res_columns[col_num++]->insert(nameset_2_array(status_info.sync_failed_tables));
            res_columns[col_num++]->insert(nameset_2_array(status_info.skipped_unsupported_tables));
        }
    }
    Block res = getInMemoryMetadataPtr()->getSampleBlock().cloneWithoutColumns();
    if (!res_columns.empty())
    {
        size_t col_num = 0;
        size_t num_columns = res.columns();
        while (col_num < num_columns)
        {
            res.getByPosition(col_num).column = std::move(res_columns[col_num]);
            ++col_num;
        }
    }

    return Pipe(std::make_shared<SourceFromInputStream>(std::make_shared<OneBlockInputStream>(res)));
}

} /// namespace DB

#endif
