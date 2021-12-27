#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemBitEngine.h>
#include <Storages/VirtualColumnUtils.h>
#include <Interpreters/Context.h>
#include <Access/ContextAccess.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageHaMergeTree.h>
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDictionaryManager.h>

namespace DB
{


NamesAndTypesList StorageSystemBitEngine::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"encoded_columns", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"encoded_columns_size", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
        {"version", std::make_shared<DataTypeUInt64>()},
        {"is_valid", std::make_shared<DataTypeUInt64>()},
        {"shard_id", std::make_shared<DataTypeUInt64>()},
        {"shard_base_offset", std::make_shared<DataTypeUInt64>()}
    };
}

void StorageSystemBitEngine::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_DATABASES);
    std::map<String, std::map<String, StoragePtr>> tables;  // <database, <table, StoragePtr > >

    const auto databases = DatabaseCatalog::instance().getDatabases();

    for (const auto & [database_name, database] : databases)
    {
        if (check_access_for_databases && !access->isGranted(AccessType::SHOW_DATABASES, database_name))
            continue;
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue;

        for (auto tables_it = database->getTablesIterator(context); tables_it->isValid(); tables_it->next())
        {
            const StorageMergeTree * merge_tree = dynamic_cast<const StorageMergeTree *>(tables_it->table().get());
            const StorageHaMergeTree * ha_merge_tree = dynamic_cast<const StorageHaMergeTree *>(tables_it->table().get());
            if (merge_tree || ha_merge_tree)
                tables[database_name][tables_it->name()] = tables_it->table();
        }
    }

    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_table_mut = ColumnString::create();

    for (auto & db : tables)
    {
        for (auto & table : db.second)
        {
            col_database_mut->insert(db.first);
            col_table_mut->insert(table.first);
        }
    }

    ColumnPtr col_database(std::move(col_database_mut));
    ColumnPtr col_table(std::move(col_table_mut));

    /// Determine what tables are needed by the conditions in the query.
    {
        Block filtered_block
            {
                { col_database, std::make_shared<DataTypeString>(), "database" },
                { col_table, std::make_shared<DataTypeString>(), "table" },
            };

        VirtualColumnUtils::filterBlockWithQuery(query_info.query, filtered_block, context);

        if (!filtered_block.rows())
            return;

        col_database = filtered_block.getByName("database").column;
        col_table = filtered_block.getByName("table").column;
    }

    for (size_t i = 0; i < col_database->size(); ++i)
    {
        auto database = (*col_database)[i].safeGet<const String &>();
        auto table = (*col_table)[i].safeGet<const String &>();
        auto & explicit_table = tables[database][table];
        auto merge_tree = dynamic_cast<MergeTreeData*>(explicit_table.get());

        if (!merge_tree)
            continue;

        {
            auto bitengine_dictionary_manager = dynamic_cast<BitEngineDictionaryManager *>(merge_tree->bitengine_dictionary_manager.get());

            if (!bitengine_dictionary_manager)
                continue;
            auto status = bitengine_dictionary_manager->getStatus();
            Array encoded_columns_array;
            for (auto & column : status.encoded_columns)
                encoded_columns_array.push_back(column);
            Array encoded_columns_size_array;
            for (auto & column_size : status.encoded_columns_size)
                encoded_columns_size_array.push_back(column_size);

            size_t col_num = 0;
            res_columns[col_num++]->insert(database);
            res_columns[col_num++]->insert(table);
            res_columns[col_num++]->insert(encoded_columns_array);
            res_columns[col_num++]->insert(encoded_columns_size_array);
            res_columns[col_num++]->insert(status.version);
            res_columns[col_num++]->insert(status.is_valid);
            res_columns[col_num++]->insert(status.shard_id);
            res_columns[col_num++]->insert(status.shard_base_offset);
        }
    }
}

}
