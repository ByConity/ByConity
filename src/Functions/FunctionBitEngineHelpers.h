#pragma once
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDictionaryManager.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageHaMergeTree.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>

namespace DB
{
#define COMMA ,

static NameSet bitmap_aggregate_functions{
    "bitmapextract",
    "bitmapextractbysplit",
    "bitmapcolumnor",
    "bitmapcolumnxor",
    "bitmapcolumnand"
};

String tryGetDictName(const String & column_name, StoragePtr storage)
{
    String name_lowercase  = Poco::toLower(column_name);
    String inner_func_name = name_lowercase.substr(0, name_lowercase.find_first_of('('));
    if (!bitmap_aggregate_functions.count(inner_func_name))
        throw Exception("A function " + inner_func_name + " is found but we cannot parse bitmap field as dict name from it."
                            + "You should make sure bitmap field exists in the first argument. Or you can try DecodeBitmap", ErrorCodes::LOGICAL_ERROR);

    auto check = [&](auto & merge_tree, auto & name)
    {
        if (merge_tree->bitengine_dictionary_manager)
            return merge_tree->bitengine_dictionary_manager->hasBitEngineDictionary(name);
        return false;
    };

    auto * merge_tree = dynamic_cast<MergeTreeData *>(storage.get());

    size_t pos_begin = column_name.find_last_of('(');
    size_t pos_end = column_name.find_last_of(')');
    if (pos_begin == std::string::npos || pos_end == std::string::npos)
        return "";

    Names values;
    size_t pre_index = pos_end;
    for (size_t i = pos_begin + 1; i <= pos_end; ++i)
    {
        char c = column_name[i];
        if (c == ' ' || c == ','
            || c == '(' || c == ')')
        {
            if (pre_index != pos_end)
            {
                values.push_back(column_name.substr(pre_index, i - pre_index));
                pre_index = pos_end;
            }
        }
        else if (pre_index == pos_end)
            pre_index = i;
    }

    String res;
    for (const auto & value : values)
    {
        String tmp_name;
        if (merge_tree && check(merge_tree, value))
            tmp_name = value;
        if (!res.empty() && !tmp_name.empty())
            throw Exception("BitEngine cannot decode multiple column in single function", ErrorCodes::LOGICAL_ERROR);
        else if (!tmp_name.empty())
            res = tmp_name;
    }
    return res;
}

inline bool checkDataTypeForBitEngineDecode(const DataTypePtr & data_type)
{
    return WhichDataType(data_type).isNativeUInt();
}

bool checkDataTypeForBitEngineEncode(const DataTypePtr & data_type)
{
    return isNativeInteger<DataTypePtr>(data_type);
}

inline StoragePtr getStorage(String & database, String & table, ContextPtr context)
{
    auto database_ptr = DatabaseCatalog::instance().getDatabase(database);
    StoragePtr storage = database_ptr->tryGetTable(table, context);
    if (!storage)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist.", database, table);

    if (auto * distributed = dynamic_cast<StorageDistributed *>(storage.get()); distributed)
    {
        storage = database_ptr->tryGetTable(distributed->getRemoteTableName(), context); // try find the local table
    }

    return storage;
}

inline String getValidDictName(StoragePtr & storage, String & input_dict_name)
{
    auto * merge_tree = dynamic_cast<MergeTreeData *>(storage.get());

    if (!merge_tree)
        throw Exception("Cannot decode table, we expect MergeTree family storage", ErrorCodes::LOGICAL_ERROR);

    String res = input_dict_name;
    if (!merge_tree->isBitEngineEncodeColumn(input_dict_name))
    {
        res = tryGetDictName(input_dict_name, storage);
        if (res.empty())
            throw Exception("In the first argument: " + input_dict_name + ", we cannot find a bitmap field as dict name. "
                                + "You should make sure bitmap field exists in the first argument. Or you can try DecodeBitmap", ErrorCodes::LOGICAL_ERROR);
    }

    auto name_and_type = merge_tree->getInMemoryMetadataPtr()->getColumns().getAllPhysical().tryGetByName(res);
    if ( !name_and_type.has_value() || !name_and_type.value().type->isBitEngineEncode())
        throw Exception("The type of column " + res + " (type: " + name_and_type.value().type->getName() + ") is not BitMap64",
                        ErrorCodes::LOGICAL_ERROR);

    return res;
}

std::tuple<String, String, String> getDictionaryPrerequisite(const ColumnsWithTypeAndName & arguments, const String & func_name)
{
    const auto * database_column = checkAndGetColumnEvenIfConst<ColumnString>(arguments[1].column.get());
    const auto * table_column = checkAndGetColumnEvenIfConst<ColumnString>(arguments[2].column.get());
    if (!database_column || !table_column)
        throw Exception("Function " + func_name + " has illegal argument of database or table", ErrorCodes::LOGICAL_ERROR);

    String database = database_column->getDataAt(0).toString();
    String table = table_column->getDataAt(0).toString();

    String dict_name;
    if (arguments.size() >= 4)
    {
        const auto * dict_column = checkAndGetColumnEvenIfConst<ColumnString>(arguments[3].column.get());
        if (!dict_column)
            throw Exception("Function " + func_name + " has illegal argument of dictionary", ErrorCodes::LOGICAL_ERROR);
        dict_name = dict_column->getDataAt(0).toString();
    }
    return std::make_tuple(database, table, dict_name);
}

/// This function is used by HaMergeTree/MergeTree to get BitEngineDictionary to encode/decode data
/// Cautious that if BitEngineDictionary will be changed in HaMergeTree, you should get LOCK first !!!
inline BitEngineDictionaryPtr getBitEngineDictionaryPtr(const MergeTreeData & data, String & dict_name)
{
    auto dict_manager = data.bitengine_dictionary_manager;
    if (!dict_manager)
        throw Exception("Cannot find BitEngineDictionaryManager", ErrorCodes::LOGICAL_ERROR);

    return dynamic_cast<BitEngineDictionaryManager *>(dict_manager.get())->getBitEngineDictPtr(dict_name);
}

/// `DISCARD` here means those keys not-found are discarded by BitEngineDictionary
/// As for user-input data, there are two ways:
/// 1. for bitmap, those keys not-found is discard in the result, so the result bitmap size may be smaller
/// 2. for non BitEngine column (like UInt64), those keys are replaced by a default value in result column
/// In both two ways we keep the column size unchanged
ColumnPtr encodeColumnDiscardUnknown(
    ColumnWithTypeAndName & column,
    String & database,
    String & table,
    String & dict_name,
    const ContextPtr & context)
{
    if (!isBitmap64(column.type) && !checkDataTypeForBitEngineEncode(column.type))
        throw Exception("BitEngine cannot encode column " + column.name + ", since it's type is not Integer or Bitmap64", ErrorCodes::LOGICAL_ERROR);

    auto storage = getStorage(database, table, context);
    auto * merge_tree = dynamic_cast<MergeTreeData *>(storage.get());
    if (!merge_tree)
        throw Exception("Cannot decode table, we expect MergeTree family storage", ErrorCodes::LOGICAL_ERROR);

    ColumnWithTypeAndName column_encoded;
    BitEngineEncodeSettings settings(context->getSettingsRef(), merge_tree->getSettings());

    if (dynamic_cast<StorageMergeTree *>(storage.get()) ||
        dynamic_cast<StorageHaMergeTree *>(storage.get()))
    {

        auto dict_ptr = getBitEngineDictionaryPtr(*merge_tree, dict_name);
        if (!dict_ptr->isValid())
            throw Exception("BitEngine cannot encode column " + column.name + ", since the dict is invalid", ErrorCodes::LOGICAL_ERROR);

        if (isBitmap64(column.type))
        {

            column_encoded = dict_ptr->encodeColumn(column, settings.bitengineEncodeWithoutLock(true));
        }
        else // Integer type
        {
#define ENCODE_DISCARD(type) dict_ptr->encodeNonBitEngineColumn<type>(column COMMA false COMMA settings.getLossRate())

            switch (column.type->getTypeId())
            {
                case TypeIndex::UInt8:      column_encoded = ENCODE_DISCARD(UInt8); break;
                case TypeIndex::UInt16:     column_encoded = ENCODE_DISCARD(UInt16); break;
                case TypeIndex::UInt32:     column_encoded = ENCODE_DISCARD(UInt32); break;
                case TypeIndex::UInt64:     column_encoded = ENCODE_DISCARD(UInt64); break;
                case TypeIndex::Int8:       column_encoded = ENCODE_DISCARD(Int8); break;
                case TypeIndex::Int16:      column_encoded = ENCODE_DISCARD(Int16); break;
                case TypeIndex::Int32:      column_encoded = ENCODE_DISCARD(Int32); break;
                case TypeIndex::Int64:      column_encoded = ENCODE_DISCARD(Int64); break;
                default:
                    break;
            }
        }
    }
    else
        throw Exception("BitEngine doesn't work in the table engine, input is: " + database + "." + table, ErrorCodes::LOGICAL_ERROR);

    return column_encoded.column;
}

/// `ADD` means keys not found in dict will be encoded
ColumnPtr encodeColumnAddUnknown(
    ColumnWithTypeAndName & column,
    String & database,
    String & table,
    String & dict_name,
    const ContextPtr & context)
{
    if (!isBitmap64(column.type) && !checkDataTypeForBitEngineEncode(column.type))
        throw Exception("BitEngine cannot encode column " + column.name + ", since it's type is not under-64 bit integer or Bitmap64",
                        ErrorCodes::LOGICAL_ERROR);

    StoragePtr storage = getStorage(database, table, context);
    auto * merge_tree = dynamic_cast<MergeTreeData *>(storage.get());
    if (!merge_tree)
        throw Exception("Cannot decode table, we expect MergeTree family storage", ErrorCodes::LOGICAL_ERROR);

    ColumnWithTypeAndName column_encoded;
    bool need_update = false;
    BitEngineEncodeSettings settings(context->getSettingsRef(), merge_tree->getSettings());

    /// Below: for MergeTree/HaMergeTree
    auto dict_ptr = getBitEngineDictionaryPtr(*merge_tree, dict_name);
    if (!dict_ptr->isValid())
        throw Exception("BitEngine cannot encode column " + column.name + ", since the dict is invalid", ErrorCodes::LOGICAL_ERROR);

    BitEngineDictionaryManagerPtr dict_manager = merge_tree->bitengine_dictionary_manager;

    auto encodeColumnData = [&] {
        try
        {
            if (isBitmap64(column.type))
                column_encoded = dict_ptr->encodeColumn(column, settings.bitengineEncodeWithoutLock(false));
            else // Integer type
            {
#define ENCODE(type) dict_ptr->encodeNonBitEngineColumn<type>(column COMMA true COMMA settings.getLossRate())

                switch (column.type->getTypeId())
                {
                    case TypeIndex::UInt8:      column_encoded = ENCODE(UInt8); break;
                    case TypeIndex::UInt16:     column_encoded = ENCODE(UInt16); break;
                    case TypeIndex::UInt32:     column_encoded = ENCODE(UInt32); break;
                    case TypeIndex::UInt64:     column_encoded = ENCODE(UInt64); break;
                    case TypeIndex::Int8:       column_encoded = ENCODE(Int8); break;
                    case TypeIndex::Int16:      column_encoded = ENCODE(Int16); break;
                    case TypeIndex::Int32:      column_encoded = ENCODE(Int32); break;
                    case TypeIndex::Int64:      column_encoded = ENCODE(Int64); break;
                    default:
                        break;
                }
            }

            need_update |= dict_ptr->updated();
        }
        catch(...)
        {
            if (dict_manager && need_update)
            {
                LOG_DEBUG(&Poco::Logger::get("encodeColumnAddUnknown"), "Will update dictionary version in catch");
                dict_manager->updateVersion();
                dict_manager->flushDict();
            }
        }

        if (dict_manager && need_update)
        {
            LOG_DEBUG(&Poco::Logger::get("encodeColumnAddUnknown"), "Will update version after encode column");
            // update local version
            dict_manager->updateVersion();
            // reduce the frequency of flush disk
            dict_manager->lightFlush();
        }

    };

    if (dynamic_cast<StorageMergeTree *>(storage.get()))
    {
        encodeColumnData();
    }
    else if (auto * ha_merge_tree = dynamic_cast<StorageHaMergeTree *>(storage.get()); ha_merge_tree)
    {
        if (auto * dict_ha_manager = ha_merge_tree->getBitEngineDictionaryHaManager(); dict_ha_manager)
        {
            // version in zk is updated when releasing the BitEngineLock
            auto bitengine_lock = dict_ha_manager->tryGetLock();
            dict_ha_manager->tryUpdateDict();
            // double-check the status of bitengine manager if the storage is shutdown when it was waitting for the lock
            if (!dict_ha_manager || dict_ha_manager->isStopped())
            {
                LOG_DEBUG(&Poco::Logger::get("encodeColumnAddUnknown"), "The HaMergeTree Engine doesn't work well now, encoding column failed.");
                return column_encoded.column;
            }
            encodeColumnData();
        }
        else
            throw Exception("BitEngineDictionaryHaManager is not found in " + table + ", encoding column failed."
                            ,ErrorCodes::LOGICAL_ERROR);
    }
    else
    {
        throw Exception("BitEngine doesn't work in the table engine, input is: " + database + "." + table ,ErrorCodes::LOGICAL_ERROR);
    }

    return column_encoded.column;
}

ColumnPtr decodeColumn(
    ColumnWithTypeAndName & column,
    String & database,
    String & table,
    String & dict_name,
    bool try_parse_dict_name,
    const ContextPtr & context)
{
    if (!isBitmap64(column.type) && !checkDataTypeForBitEngineDecode(column.type))
        throw Exception("BitEngine cannot encode column " + column.name + ", since it's type is not UInt or Bitmap64",
                        ErrorCodes::LOGICAL_ERROR);

    StoragePtr storage = getStorage(database, table, context);
    auto * merge_tree = dynamic_cast<MergeTreeData *>(storage.get());
    if (!merge_tree)
        throw Exception("Cannot decode table, we expect MergeTree family storage", ErrorCodes::LOGICAL_ERROR);

    ColumnPtr column_decoded;
    String valid_dict_name = try_parse_dict_name ? getValidDictName(storage, dict_name) : dict_name;

    /// Below: for MergeTree/HaMergeTree
    auto * dict_manager = dynamic_cast<BitEngineDictionaryManager *>((merge_tree->bitengine_dictionary_manager).get());
    if (!dict_manager)
        throw Exception("BitEngine cannot decode column " + column.name + ", since the dictionary is invalid",
                        ErrorCodes::LOGICAL_ERROR);

    if (isBitmap64(column.type))
        column_decoded = dict_manager->decodeColumn(*column.column, valid_dict_name);
    else // UInt64
        column_decoded = dict_manager->decodeNonBitEngineColumn(*column.column, valid_dict_name);

    return column_decoded;
}

}
