#include <Storages/BitEngine/BitEngineDictionaryManager.h>
// #include <Storages/StorageDictCloudMergeTree.h>
// #include <Storages/BitEngineEncodePartitionHelper.h>
#include <Columns/ColumnBitMap64.h>
#include "Storages/MergeTree/MergeTreeSuffix.h"

namespace DB
{
////////////////////////////    StartOf BitEngineDictionaryManager
BitEngineDictionaryManager::BitEngineDictionaryManager(
    const String & db_tbl_, const ContextPtr & context_,
    ExpressionActionsPtr cluster_by_key_expr_)
    : global_context(context_)
    , bitengine_table_cluster_by_key_expr(cluster_by_key_expr_)
    , log(&Poco::Logger::get("BitEngineDictionaryManager (" + db_tbl_ + ")"))
{
}

void BitEngineDictionaryManager::addBitEngineDictionary(
    const String & bitengine_field_name, BitEngineHelper::DictionaryDatabaseAndTable & underlying_dict_table)
{
    LOG_DEBUG(log, "======== addBitEngineDictionary {}", bitengine_field_name);
    auto [_, inserted] = field_dictionary_mapping.try_emplace(bitengine_field_name, underlying_dict_table);
    if (!inserted)
        throw Exception("BitEngineDictionaryManager: Duplicated bitengine_file_name assigned: " + bitengine_field_name,
            ErrorCodes::LOGICAL_ERROR);
    else
        LOG_DEBUG(log, "Add column {} to manager, its type: {} and the underlying dictionary table: {}.{}",
            bitengine_field_name, BitEngineHelper::keyTypeToString(underlying_dict_table.dict_key_type),
            underlying_dict_table.database, underlying_dict_table.table);
}

bool BitEngineDictionaryManager::isKeyStringDictioanry(const String & bitengine_field_name)
{
    auto it = field_dictionary_mapping.find(bitengine_field_name);
    return it != field_dictionary_mapping.end()
        && it->second.dict_key_type == BitEngineHelper::KeyType::KEY_STRING;
}

bool BitEngineDictionaryManager::isKeyIntDictioanry(const String & bitengine_field_name)
{
    auto it = field_dictionary_mapping.find(bitengine_field_name);
    return it != field_dictionary_mapping.end()
           && it->second.dict_key_type == BitEngineHelper::KeyType::KEY_INTEGER;
}

ColumnWithTypeAndName BitEngineDictionaryManager::encodeColumn(
    const ColumnWithTypeAndName & data_column,
    [[maybe_unused]] const String & dict_field_name,
    [[maybe_unused]] const ContextPtr & query_context,
    [[maybe_unused]] const BitengineWriteSettings & encoding_setting)
{
    /// TODO
    auto res = data_column;
    res.name += BITENGINE_COLUMN_EXTENSION;
    return res;

#if 0
    auto it = field_dictionary_mapping.at(dict_field_name);
    StoragePtr dict_storage = query_context.getTable(it.database, it.table);
    StorageDictCloudMergeTree * dict_cloud_storage = dynamic_cast<StorageDictCloudMergeTree *>(dict_storage.get());

    if (!dict_cloud_storage)
    {
        throw Exception(
            fmt::format("BitEngine dict should be StorageDictCloudMergeTree, but the table <`{}`.`{}`>"\
                " is {}", it.database, it.table, dict_storage->getName()),
            ErrorCodes::BAD_ARGUMENTS);
    }

    LOG_DEBUG(log, "==== dict table name: " << dict_cloud_storage->getDatabaseName() << "." << dict_cloud_storage->getTableName());

    // we loaded dict after the parts is loaded, no need to load it anymore
    // dict_cloud_storage->loadDict(query_context, MemoryDictMode::ENCODE);

    Block b{{data_column}};
    BitEngineHelper::generateBlockForEncode(b, 0ULL);
    return dict_cloud_storage->encode(b, query_context);
#endif
}


ColumnWithTypeAndName BitEngineDictionaryManager::encodeColumn(
    const ColumnWithTypeAndName & data_column,
    [[maybe_unused]] const String & dict_field_name,
    [[maybe_unused]] Int64 bucket_number,
    [[maybe_unused]] const ContextPtr & query_context,
    [[maybe_unused]] const BitengineWriteSettings & encoding_setting)
{
    /// TODO
#if 0
    auto it = field_dictionary_mapping.at(dict_field_name);

    bool debug = false;
    if (debug)
    {
        StoragePtr dict_storage = query_context.getTable(it.database, it.table);
        StorageDictCloudMergeTree * dict_cloud_storage = dynamic_cast<StorageDictCloudMergeTree *>(dict_storage.get());

        if (!dict_cloud_storage) {
            throw Exception(
                fmt::format("BitEngine dict should be StorageDictCloudMergeTree, but the table <`{}`.`{}`>"\
                    " is {}", it.database, it.table, dict_storage->getName()),
                ErrorCodes::BAD_ARGUMENTS);
        }

        LOG_DEBUG(log, "==== dict table name: " << dict_cloud_storage->getDatabaseName() << "."
            << dict_cloud_storage->getTableName());

        Block b{{data_column}};
        BitEngineHelper::generateBlockForEncode(b, 0ULL);
        dict_cloud_storage->encode(b, 0, 1, bucket_number, query_context);
        return b.getByPosition(1);
    }
    else
#else
    /// TODO this is just a demo
    {
        ColumnWithTypeAndName column_encoded {
            data_column.type,
            data_column.name + BITENGINE_COLUMN_EXTENSION
        };

        const ColumnBitMap64 * bitmap_column = checkAndGetColumn<ColumnBitMap64>(data_column.column.get());
        auto new_column = ColumnBitMap64::create();

        for (size_t i = 0; i < bitmap_column->size(); ++i)
        {
            auto bitmap = bitmap_column->getBitMapAt(i);
            BitMap64 new_bitmap;
            for (auto it = bitmap.begin(); it != bitmap.end(); ++it)
            {
                new_bitmap.add(*it + 100);
            }
            new_column->insert(new_bitmap);
        }
        column_encoded.column = std::move(new_column);
        return column_encoded;
    }
#endif
}

ColumnPtr BitEngineDictionaryManager::decodeColumn(
    const ColumnWithTypeAndName & data_column,
    [[maybe_unused]] const String & dict_field_name,
    [[maybe_unused]] const ContextPtr & query_context,
    [[maybe_unused]] const BitengineWriteSettings & decoding_setting)
{
    if (hasBitEngineDictionary(dict_field_name))
    {
        /// TODO
        return data_column.column;

#if 0
        auto it = field_dictionary_mapping.at(dict_field_name);
        StoragePtr dict_storage = query_context.getTable(it.database, it.table);
        auto * dict_cloud_storage = dynamic_cast<StorageDictCloudMergeTree *>(dict_storage.get());
        if (!dict_cloud_storage)
            throw Exception(
                fmt::format("In BitEngine decoding, cannot get StorageDictCloudMergeTree, db.tbl=`{}`.`{}`, EngineType={}",
                    it.database, it.table, dict_storage->getName()),
                ErrorCodes::LOGICAL_ERROR);

        Block b{{data_column}};
        return dict_cloud_storage->decode(b, query_context);
#endif
    }
    else
        throw Exception("Dictionary " + dict_field_name + " not found!", ErrorCodes::LOGICAL_ERROR);
}


}
