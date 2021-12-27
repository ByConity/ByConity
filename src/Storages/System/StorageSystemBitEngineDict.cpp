#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemBitEngineDict.h>
#include <Storages/VirtualColumnUtils.h>
#include <Interpreters/Context.h>
#include <Access/ContextAccess.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageHaMergeTree.h>
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDictionaryManager.h>

namespace DB
{


NamesAndTypesList StorageSystemBitEngineDict::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"column", std::make_shared<DataTypeString>()},
        {"source", std::make_shared<DataTypeUInt64>()},
        {"encode", std::make_shared<DataTypeUInt64>()},
    };
}

void StorageSystemBitEngineDict::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_DATABASES);
    std::map<String, std::map<String, StoragePtr>> tables_to_search;  // <database, <table, StoragePtr > >

    const auto databases = DatabaseCatalog::instance().getDatabases();

    for (const auto & [database_name, database] : databases)
    {
        if (check_access_for_databases && !access->isGranted(AccessType::SHOW_DATABASES, database_name))
            continue;
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue;

        for (auto tables_it = database->getTablesIterator(context); tables_it->isValid(); tables_it->next())
        {
            if (auto * table = dynamic_cast<MergeTreeData *>(tables_it->table().get()); table)
            {
                if (table->isBitEngineMode())
                    tables_to_search[database_name][tables_it->name()] = tables_it->table();
            }
        }
    }

    MutableColumnPtr database_column_mut = ColumnString::create();
    MutableColumnPtr table_column_mut = ColumnString::create();
    MutableColumnPtr column_column_mut = ColumnString::create();

    for (auto & db_tbl : tables_to_search)
    {
        for (auto & table_name_ptr : db_tbl.second)
        {
            auto & columns = table_name_ptr.second->getInMemoryMetadataPtr()->getColumns();
            for (auto name_and_type : columns.getAllPhysical())
            {
                if (isBitmap64(name_and_type.type) && name_and_type.type->isBitEngineEncode())
                {
                    database_column_mut->insert(db_tbl.first);
                    table_column_mut->insert(table_name_ptr.first);
                    column_column_mut->insert(name_and_type.name);
                }
            }
        }
    }

    /// Determine what tables are needed by the conditions in the query.
    ColumnPtr col_database_to_filter = std::move(database_column_mut);
    ColumnPtr col_table_to_filter = std::move(table_column_mut);
    ColumnPtr col_column_to_filter = std::move(column_column_mut);
    {
        Block filtered_block
            {
                { col_database_to_filter, std::make_shared<DataTypeString>(), "database" },
                { col_table_to_filter, std::make_shared<DataTypeString>(), "table" },
                { col_column_to_filter, std::make_shared<DataTypeString>(), "column" }
            };

        VirtualColumnUtils::filterBlockWithQuery(query_info.query, filtered_block, context);
        if (!filtered_block.rows())
            return;

        col_database_to_filter = filtered_block.getByName("database").column;
        col_table_to_filter = filtered_block.getByName("table").column;
        col_column_to_filter = filtered_block.getByName("column").column;
    }

    auto fillDataFromSnapshot = [&res_columns](BitEngineDictionarySnapshot & dict_snapshot,
                                               String & database, String & table, String & column)
    {
        if (!dict_snapshot.empty())
        {
            size_t dict_size = dict_snapshot.size();
            size_t shard_base_offset = dict_snapshot.getOffset();
            BitEngineDictioanryColumnPtr key_column_ptr = dict_snapshot.getKeyColumn();
            auto * encoded_column = dynamic_cast<ColumnUInt64 *>(res_columns[4].get());

            for (size_t index = 0; index < dict_size; ++index)
            {
                res_columns[0]->insert(database);
                res_columns[1]->insert(table);
                res_columns[2]->insert(column);
                encoded_column->insertValue(shard_base_offset + index);  // res_columns[4]
            }
            res_columns[3]->insertRangeFrom(*key_column_ptr, 0, dict_size);
        }
    };

    for (size_t i = 0, rows = col_database_to_filter->size(); i < rows; ++i)
    {
        String database = (*col_database_to_filter)[i].safeGet<const String &>();
        String table = (*col_table_to_filter)[i].safeGet<const String &>();
        String column = (*col_column_to_filter)[i].safeGet<const String &>();

        auto target_bitmap_table = tables_to_search[database][table];
        auto * merge_tree = dynamic_cast<MergeTreeData*>(target_bitmap_table.get());
        if (!merge_tree)
            continue;

        if (!merge_tree->isBitEngineEncodeColumn(column))
            continue;

        {
            auto * bitengine_dictionary_manager = dynamic_cast<BitEngineDictionaryManager *>(merge_tree->bitengine_dictionary_manager.get());
            if (!bitengine_dictionary_manager)
                continue;
            auto dict_snapshot = bitengine_dictionary_manager->getDictSnapshotPtr(column);
            fillDataFromSnapshot(*dict_snapshot, database, table, column);
        }
    }
}

}
