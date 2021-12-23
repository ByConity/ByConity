#include <Storages/System/StorageSystemUniqueLogs.h>

#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/StorageHaUniqueMergeTree.h>
#include <Storages/VirtualColumnUtils.h>

namespace DB
{

NamesAndTypesList StorageSystemUniqueLogs::getNamesAndTypes()
{
    return {
        /// Table properties.
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"replica_name", std::make_shared<DataTypeString>()},
        /// Log entry fields
        {"type", std::make_shared<DataTypeString>()},
        {"version", std::make_shared<DataTypeUInt64>()},
        {"prev_version", std::make_shared<DataTypeUInt64>()},
        {"create_time", std::make_shared<DataTypeDateTime>()},
        {"source_replica", std::make_shared<DataTypeString>()},
        {"added_parts", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"updated_parts", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"removed_parts", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"target_version", std::make_shared<DataTypeUInt64>()},
        /// whether version <= committed version
        {"committed", std::make_shared<DataTypeUInt8>()},
        {"metadata_str", std::make_shared<DataTypeString>()},
        {"columns_str", std::make_shared<DataTypeString>()}
    };
}

void StorageSystemUniqueLogs::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);
    std::map<String, std::map<String, StoragePtr>> unique_tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases())
    {
        /// Check if database can contain replicated tables
        if (!db.second->canContainMergeTreeTables())
            continue;
        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, db.first);
        for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            const auto & table = iterator->table();
            if (!table)
                continue;

            if (!dynamic_cast<const StorageHaUniqueMergeTree *>(table.get()))
                continue;
            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, db.first, iterator->name()))
                continue;
            unique_tables[db.first][iterator->name()] = table;
        }
    }

    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_table_mut = ColumnString::create();
    for (auto & db : unique_tables)
    {
        for (auto & table : db.second)
        {
            col_database_mut->insert(db.first);
            col_table_mut->insert(table.first);
        }
    }

    ColumnPtr col_database_to_filter = std::move(col_database_mut);
    ColumnPtr col_table_to_filter = std::move(col_table_mut);
    /// Determine what tables are needed by the conditions in the query.
    {
        Block filtered_block
            {
                { col_database_to_filter, std::make_shared<DataTypeString>(), "database" },
                { col_table_to_filter, std::make_shared<DataTypeString>(), "table" },
            };

        VirtualColumnUtils::filterBlockWithQuery(query_info.query, filtered_block, context);

        if (!filtered_block.rows())
            return;

        col_database_to_filter = filtered_block.getByName("database").column;
        col_table_to_filter = filtered_block.getByName("table").column;
    }

    auto names_to_array = [](const Names & names) -> Array {
        Array res;
        res.reserve(names.size());
        for (auto & name : names)
            res.push_back(name);
        return res;
    };

    for (size_t i = 0, tables_size = col_database_to_filter->size(); i < tables_size; ++i)
    {
        String database = (*col_database_to_filter)[i].safeGet<const String &>();
        String table = (*col_table_to_filter)[i].safeGet<const String &>();
        auto & storage = dynamic_cast<StorageHaUniqueMergeTree &>(*unique_tables[database][table]);
        String replica_name = storage.replicaName();
        UInt64 commit_version = 0;
        auto logs = storage.getManifestLogs(commit_version);

        for (size_t j = 0; j < logs.size(); ++j)
        {
            auto & entry = logs[j];
            size_t col_num = 0;
            res_columns[col_num++]->insert(database);
            res_columns[col_num++]->insert(table);
            res_columns[col_num++]->insert(replica_name);
            res_columns[col_num++]->insert(ManifestLogEntry::typeToString(entry.type));
            res_columns[col_num++]->insert(entry.version);
            res_columns[col_num++]->insert(entry.prev_version);
            res_columns[col_num++]->insert(entry.create_time);
            res_columns[col_num++]->insert(entry.source_replica);
            res_columns[col_num++]->insert(names_to_array(entry.added_parts));
            res_columns[col_num++]->insert(names_to_array(entry.updated_parts));
            res_columns[col_num++]->insert(names_to_array(entry.removed_parts));
            res_columns[col_num++]->insert(entry.target_version);
            res_columns[col_num++]->insert(static_cast<bool>(entry.version <= commit_version));
            res_columns[col_num++]->insert(entry.metadata_str);
            res_columns[col_num++]->insert(entry.columns_str);
        }
    }
}

}
