#pragma once
#include <CloudServices/CnchWorkerResource.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/BitEngine/BitEngineDictionaryManager.h>
#include <Storages/MemoryDict.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/StorageDictCloudMergeTree.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BITENGINE_DICT_EXCEPTION;
}

static NameSet bitmap_aggregate_functions{
    "bitmapextract", "bitmapextractbysplit", "bitmapcolumnor", "bitmapcolumnxor", "bitmapcolumnand", "bitmapextractv2"};

struct BitEngineDictionaryEncodeInfo
{
    String database;
    String table;
    String dict_name;
    bool add_new;
    bool tolerant_loss;
};

String tryGetDictName(const String & column_name, StorageCloudMergeTree * merge_tree)
{
    String name_lowercase = Poco::toLower(column_name);
    size_t pos = name_lowercase.find_first_of('(');

    // if we enter tryGetDictName, that means string column_name is not
    // a legal filed of table, and if there's no '(' in it. It may be a
    // wrong dict_name, return empty string to throw exception.
    if (pos == std::string::npos)
        return "";

    String inner_func_name = name_lowercase.substr(0, name_lowercase.find_first_of('('));

    if (!bitmap_aggregate_functions.count(inner_func_name) && name_lowercase.find("bitmap") == std::string::npos)
        return "";

    auto check = [&](auto & storage_bitengine_cloud, auto & name) {
        if (auto dict_manager = storage_bitengine_cloud->getBitEngineDictionaryManager())
            return dict_manager->hasBitEngineDictionary(name);
        return false;
    };

    size_t pos_begin = column_name.find_last_of('(');
    size_t pos_end = column_name.find_last_of(')');
    if (pos_begin == std::string::npos || pos_end == std::string::npos)
        return "";

    Names values;
    size_t pre_index = pos_end;
    for (size_t i = pos_begin + 1; i <= pos_end; ++i)
    {
        char c = column_name[i];
        if (c == ' ' || c == ',' || c == '(' || c == ')')
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
            throw Exception("BitEngine cannot decode multiple column in single function", ErrorCodes::BITENGINE_DICT_EXCEPTION);
        else if (!tmp_name.empty())
            res = tmp_name;
    }
    return res;
}

inline bool checkDataTypeForBitEngineDecode(const DataTypePtr & data_type)
{
    return isBitmap64(data_type) || WhichDataType(data_type).isNativeUInt();
}

bool checkDataTypeForBitEngineEncode(const DataTypePtr & data_type)
{
    return isBitmap64(data_type) || isNativeInteger(data_type) || isArrayOfString(data_type) || isArrayOfUInt64(data_type)
        || isString(data_type);
}

StorageCloudMergeTree * loadDictsForCnchServer(
    StorageCnchMergeTree * storage_bitengine_cnch,
    const String & encode_database,
    const String & encode_table,
    [[maybe_unused]] MemoryDictMode mode,
    const ContextPtr & local_context)
{
    if (!storage_bitengine_cnch->isBitEngineTable())
        throw Exception(
            fmt::format("Table <`{}`.`{}`> is not a BitEngine table", encode_database, encode_table), ErrorCodes::BITENGINE_DICT_EXCEPTION);

    auto query_context = local_context->getQueryContext();
    /// init and get WorkerResource, then create cloud table for BitEngine, as well as load parts
    if (!query_context->tryGetCnchWorkerResource())
        query_context->initCnchWorkerResource();
    auto worker_resource = query_context->getCnchWorkerResource();

    auto lock = worker_resource->getBitEngineDictLoadLock();

    auto bitengine_tables_in_query = query_context->getBitEngineTables();
    if (!bitengine_tables_in_query)
        throw Exception(
            ErrorCodes::BITENGINE_DICT_EXCEPTION,
            "Not found bitengine tables info in context! node type: {}",
            query_context->getServerTypeString());

    auto it = bitengine_tables_in_query->find(storage_bitengine_cnch->getStorageUUID());

    BitEngineDictionaryTableMapping underlying_dictionary_table_cloud;
    auto cloud_table_name = storage_bitengine_cnch->getCloudTableName(local_context);
    if (!it->second.cloud_table_created_on_server)
    {
        /// create cloud table for underlying dict tables
        /// NOTE: load parts are note implemented yet
        const auto & dicts_mapping = storage_bitengine_cnch->getUnderlyDictionaryTables();
        for (const auto & entry : dicts_mapping)
        {
            StorageID dict_table_id{entry.second.first, entry.second.second};
            auto storage_underlying_dict
                = DatabaseCatalog::instance().tryGetTable(dict_table_id, local_context);
            StorageCnchMergeTree * storage_underlying_dict_cnch = dynamic_cast<StorageCnchMergeTree *>(storage_underlying_dict.get());
            if (storage_underlying_dict_cnch)
            {
                auto dict_table_name_cloud = storage_underlying_dict_cnch->getCloudTableName(local_context);
                underlying_dictionary_table_cloud.emplace(entry.first, std::make_pair(entry.second.first, dict_table_name_cloud));

                auto create_table_query = storage_underlying_dict_cnch->getCreateQueryForCloudTable(
                    storage_underlying_dict_cnch->getCreateTableSql(),
                    dict_table_name_cloud,
                    local_context,
                    false,
                    std::nullopt,
                    {},
                    {},
                    WorkerEngineType::DICT);

                /// try find dict_cloud_table first, maybe it's created already, like insert into select DecodeBitmap()
                auto storage_underlying_dict_cloud
                    = worker_resource->getTable(StorageID{dict_table_id.getDatabaseName(), dict_table_name_cloud});

                bool dict_cloud_already_exists{true};
                if (!storage_underlying_dict_cloud)
                {
                    worker_resource->executeCreateQuery(local_context->getQueryContext(), create_table_query, /* skip_if_exists */ true);
                    dict_cloud_already_exists = false;
                }

                /// after dict_cloud_table created, now get and load parts
                storage_underlying_dict_cloud
                    = worker_resource->getTable(StorageID{dict_table_id.getDatabaseName(), dict_table_name_cloud});
                auto * underlying_dict_cloud_table = dynamic_cast<StorageDictCloudMergeTree *>(storage_underlying_dict_cloud.get());

                if (!underlying_dict_cloud_table)
                {
                    throw Exception(
                        fmt::format(
                            "In decoding, cannot get DictCloudMergeTree for table:<`{}`.`{}`>",
                            dict_table_id.getDatabaseName(),
                            dict_table_name_cloud),
                        ErrorCodes::UNKNOWN_TABLE);
                }

                if (!dict_cloud_already_exists)
                {
                    auto server_parts = storage_underlying_dict_cnch->getAllPartsWithDBM(local_context).first;
                    MergeTreeDataPartTypeHelper::MutableDataPartsVector parts;
                    for (auto & part : server_parts)
                    {
                        parts.emplace_back(part->toCNCHDataPart(*storage_underlying_dict_cnch));
                    }

                    underlying_dict_cloud_table->loadDataParts(parts);
                }
            }
        }

        /// create cloud table for bitengine table
        auto create_table_query = storage_bitengine_cnch->getCreateQueryForCloudTable(
            storage_bitengine_cnch->getCreateTableSql(),
            cloud_table_name,
            local_context,
            false,
            std::nullopt,
            {},
            {},
            WorkerEngineType::CLOUD,
            underlying_dictionary_table_cloud);

        worker_resource->executeCreateQuery(local_context->getQueryContext(), create_table_query, /* skip_if_exists */ true);
        LOG_DEBUG(
            &Poco::Logger::get("FunctionsBitEngineHelper"),
            "Created table {} on {}",
            storage_bitengine_cnch->getStorageID().getFullTableName(),
            local_context->getServerTypeString());
        it->second.cloud_table_created_on_server = true;
        it->second.dict_loaded_on_server = true;
    }

    auto storage_bitengine = worker_resource->getTable(StorageID{storage_bitengine_cnch->getDatabaseName(), cloud_table_name});

    auto * cloud_table = dynamic_cast<StorageCloudMergeTree *>(storage_bitengine.get());

    if (!cloud_table)
    {
        throw Exception(
            fmt::format(
                "Cannot parse and get a CloudMergeTree from the argument of decode function, "
                "which is <`{}`.`{}`>, and the cloud table name is <`{}`.`{}`>",
                encode_database,
                encode_table,
                storage_bitengine_cnch->getDatabaseName(),
                cloud_table_name),
            ErrorCodes::UNKNOWN_TABLE);
    }

    return cloud_table;
}

inline StorageCloudMergeTree *
getCloudTable(const String & database, const String & table, [[maybe_unused]] MemoryDictMode mode, const ContextPtr & local_context)
{
    StoragePtr storage = DatabaseCatalog::instance().getTable(StorageID{database, table}, local_context);
    auto * cloud_table = dynamic_cast<StorageCloudMergeTree *>(storage.get());
    if (!cloud_table)
    {
        auto * storage_bitengine_cnch = dynamic_cast<StorageCnchMergeTree *>(storage.get());
        if (!storage_bitengine_cnch)
        {
            throw Exception(
                fmt::format("`{}`.`{}` is not a StorageCnchMergeTree table, check the arguments", database, table),
                ErrorCodes::UNKNOWN_TABLE);
        }

        cloud_table = loadDictsForCnchServer(storage_bitengine_cnch, database, table, mode, local_context);

        if (!cloud_table)
            throw Exception(
                fmt::format(
                    "BitEngine dict manager is not initialized for table `{}`.`{}`"
                    ", or it's not a BitEngine table",
                    cloud_table->getDatabaseName(),
                    cloud_table->getTableName()),
                ErrorCodes::UNKNOWN_TABLE);
    }

    if (!cloud_table->isBitEngineMode())
        throw Exception(
            fmt::format(
                "BitEngine dict manager is not initialized for table `{}`.`{}`"
                ", or it's not a BitEngine table",
                cloud_table->getDatabaseName(),
                cloud_table->getTableName()),
            ErrorCodes::BITENGINE_DICT_EXCEPTION);

    return cloud_table;
}

inline String getValidDictName(StorageCloudMergeTree * storage_bitengine_cloud, String & input_dict_name)
{
    String res = input_dict_name;

    if (!storage_bitengine_cloud->isBitEngineEncodeColumn(input_dict_name))
    {
        res = tryGetDictName(input_dict_name, storage_bitengine_cloud);
        if (res.empty())
            throw Exception(
                "In the first argument: " + input_dict_name + ", we cannot find dict name(which is a bitmap field from table, same name). "
                    + "You should make sure a physical bitmap field exists in the first argument. Or you can try DecodeBitmap",
                ErrorCodes::BITENGINE_DICT_EXCEPTION);
    }

    auto name_and_type = storage_bitengine_cloud->getInMemoryMetadataPtr()->getColumns().tryGetPhysical(res);
    if (!name_and_type.has_value() || !name_and_type.value().type->isBitEngineEncode())
        throw Exception(
            "The type of column " + res + " (type: " + name_and_type.value().type->getName() + ") is not BitMap64",
            ErrorCodes::BITENGINE_DICT_EXCEPTION);

    return res;
}

void checkDictionaryPrerequisiteTypes(const DataTypes & arguments, const String & func_name)
{
    /// first check dictionary info: db, tbl, bitmap_filed
    /// they are all String
    for (size_t i = 1; i < 4; ++i)
    {
        if (!checkAndGetDataType<DataTypeString>(arguments[i].get()))
            throw Exception(
                fmt::format(
                    "The {} argument type of function {} is not right, received={}, expected={}",
                    argPositionToSequence(i + 1),
                    func_name,
                    arguments[i]->getName(),
                    "String"),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    /// Until now, Bool switches are not used in CNCH, do not throw exception
    /// when enabling optimizer (1 will be assigned Int8, not UInt8)
    for (size_t i = 4; i < arguments.size(); ++i)
    {
        if (!checkAndGetDataType<DataTypeUInt8>(arguments[i].get()))
            throw Exception(
                fmt::format(
                    "The {} argument type of function {} is not right, recived={}, expected={}",
                    argPositionToSequence(i + 1),
                    func_name,
                    arguments[i]->getName(),
                    "UInt8"),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
}

BitEngineDictionaryEncodeInfo getDictionaryPrerequisite(const ColumnsWithTypeAndName & arguments, const String & func_name)
{
    const auto * database_column = checkAndGetColumnEvenIfConst<ColumnString>(arguments[1].column.get());
    const auto * table_column = checkAndGetColumnEvenIfConst<ColumnString>(arguments[2].column.get());
    if (!database_column || !table_column)
        throw Exception("Function " + func_name + " has illegal argument of database or table", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    String database = database_column->getDataAt(0).toString();
    String table = table_column->getDataAt(0).toString();

    String dict_name;
    bool add_new{false};
    bool tolerant_loss{false};
    if (arguments.size() >= 4)
    {
        const auto * dict_column = checkAndGetColumnEvenIfConst<ColumnString>(arguments[3].column.get());
        if (!dict_column)
            throw Exception("Function " + func_name + " has illegal argument of dictionary", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        dict_name = dict_column->getDataAt(0).toString();
    }
    if (arguments.size() >= 5)
    {
        const auto * setting_column = checkAndGetColumnEvenIfConst<ColumnUInt8>(arguments[4].column.get());
        add_new = setting_column->getBool(0);
    }
    if (arguments.size() >= 6)
    {
        const auto * tolerant_column = checkAndGetColumnEvenIfConst<ColumnUInt8>(arguments[5].column.get());
        tolerant_loss = tolerant_column->getBool(0);
    }

    return {database, table, dict_name, add_new, tolerant_loss};
}

void checkDataTypeAndDictKeyType(const DataTypePtr & data_type)
{
    if (!checkDataTypeForBitEngineEncode(data_type))
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "BitEngine cannot encode column, it's type: {}. BitEngine can encode type: "
            "UInt64, BitMap64, Array(Integer), Array(String), String",
            data_type->getName());
    }
}

/// `DISCARD` here means those keys not-found are discarded by BitEngineDictionary
/// As for user-input data, there are two ways:
/// 1. for bitmap, those keys not-found is discard in the result, so the result bitmap size may be smaller
/// 2. for non BitEngine column (like UInt64), those keys are replaced by a default value in result column
/// In both two ways we keep the column size unchanged
ColumnPtr
encodeColumnDiscardUnknown(ColumnWithTypeAndName & column, BitEngineDictionaryEncodeInfo & encode_info, const ContextPtr & local_context)
{
    checkDataTypeAndDictKeyType(column.type);

    auto * storage_bitengine_cloud = getCloudTable(encode_info.database, encode_info.table, MemoryDictMode::ENCODE, local_context);

    BitEngineEncodeSettings encode_settings
        = BitEngineEncodeSettings(local_context->getSettingsRef(), storage_bitengine_cloud->getSettings())
              .bitengineEncodeWithoutLock(true)
              .bitengineTolerantLoss(encode_info.tolerant_loss);

    ColumnWithTypeAndName column_encoded = storage_bitengine_cloud->getBitEngineDictionaryManager()->encodeColumn(
        column, encode_info.dict_name, local_context, encode_settings);

    return column_encoded.column;
}

/// `ADD` means keys not found in dict will be encoded
ColumnPtr
encodeColumnAddUnknown(ColumnWithTypeAndName & column, BitEngineDictionaryEncodeInfo & encode_info, const ContextPtr & local_context)
{
    checkDataTypeAndDictKeyType(column.type);

    auto * storage_bitengine_cloud = getCloudTable(encode_info.database, encode_info.table, MemoryDictMode::ENCODE, local_context);

    auto column_encoded
        = storage_bitengine_cloud->getBitEngineDictionaryManager()->encodeColumn(column, encode_info.dict_name, local_context, {});

    return column_encoded.column;
}

template <typename ResultType>
ColumnPtr decodeColumn(
    ColumnWithTypeAndName & column,
    BitEngineDictionaryEncodeInfo & encode_info,
    bool try_parse_dict_name,
    const KeyType & expected_key_type,
    const ContextPtr & local_context)
{
    if (!checkDataTypeForBitEngineDecode(column.type))
        throw Exception(
            "BitEngine cannot decode column " + column.name + ", since it's type is not UInt64 or Bitmap64",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    auto * storage_bitengine_cloud = getCloudTable(encode_info.database, encode_info.table, MemoryDictMode::ENCODE, local_context);

    String valid_dict_name = try_parse_dict_name ? getValidDictName(storage_bitengine_cloud, encode_info.dict_name) : encode_info.dict_name;

    auto dict_manager = storage_bitengine_cloud->getBitEngineDictionaryManager();

    if (expected_key_type == KeyType::KEY_STRING && !dict_manager->isKeyStringDictionary(valid_dict_name))
    {
        throw Exception(
            "You expect BitEngine KeyType::STRING, but the dict " + valid_dict_name + " is not this type.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    else if (expected_key_type == KeyType::KEY_INTEGER && dict_manager->isKeyStringDictionary(valid_dict_name))
    {
        throw Exception(
            "You expect BitEngine KeyType::INTEGER, but the dict " + valid_dict_name + " is not this type.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    ColumnPtr column_decoded = dict_manager->decodeColumn<ResultType>(column, valid_dict_name, local_context, {});
    return column_decoded;
}

}
