#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemBitmapIndex.h>
#include <Storages/MergeTree/MergeTreeBitmapIndex.h>
#include <Storages/VirtualColumnUtils.h>
#include <Interpreters/Context.h>
#include <Storages/StorageMergeTree.h>
#include <Access/ContextAccess.h>


namespace DB
{


NamesAndTypesList StorageSystemBitmapIndex::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"part", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemBitmapIndex::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_DATABASES);

    std::map<String, std::map<String, StoragePtr>> bitmap_index_tables;

    Databases databases = DatabaseCatalog::instance().getDatabases();

    for (const auto & [database_name, database] : databases)
    {
        if (check_access_for_databases && !access->isGranted(AccessType::SHOW_DATABASES, database_name))
            continue;

        for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            const StorageMergeTree * merge_tree = dynamic_cast<const StorageMergeTree *>(iterator->table().get());

            if (merge_tree)
                bitmap_index_tables[database_name][iterator->name()] = iterator->table();
        }
    }

    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_table_mut = ColumnString::create();

    for (auto & db : bitmap_index_tables)
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
        MergeTreeBitmapIndexStatus status;

        auto database = (*col_database)[i].safeGet<const String &>();
        auto table = (*col_table)[i].safeGet<const String &>();
        auto & explicit_table = bitmap_index_tables[database][table];

        if (!explicit_table)
            continue;

        auto lock = explicit_table->lockForShare(context->getCurrentQueryId(), context->getSettingsRef().lock_acquire_timeout);

        auto merge_tree = dynamic_cast<MergeTreeData*>(explicit_table.get());

        if (merge_tree && merge_tree->getMergeTreeBitmapIndex())
            status = merge_tree->getMergeTreeBitmapIndex()->getMergeTreeBitmapIndexStatus();
        else
            continue;

        Names parts = status.parts;

        for (const auto & name : parts)
        {
            size_t col_num = 0;
            res_columns[col_num++]->insert(database);
            res_columns[col_num++]->insert(table);
            res_columns[col_num++]->insert(name);
        }
    }
}

}
