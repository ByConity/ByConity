#pragma once

#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/BitEngine/BitEngineHelper.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>

#include <unordered_map>

namespace DB
{
class BitEngineEncodePartitionParam;
class StorageCloudMergeTree;
class BitEngineDictionaryManager
{
public:
    BitEngineDictionaryManager(const String & db_tbl_, const ContextPtr & context_, ExpressionActionsPtr cluster_by_key_expr_);
    ~BitEngineDictionaryManager() = default;
    BitEngineDictionaryManager(BitEngineDictionaryManager &) = delete;
    BitEngineDictionaryManager(BitEngineDictionaryManager &&) = delete;

    void addBitEngineDictionary(const String & bitengine_field_name, BitEngineHelper::DictionaryDatabaseAndTable & underlying_dict_table);

    bool isKeyStringDictioanry(const String & bitengine_field_name);

    bool isKeyIntDictioanry(const String & bitengine_field_name);

    inline bool hasBitEngineDictionary(const String & bitengine_field_name)
    {
        return field_dictionary_mapping.count(bitengine_field_name);
    }

    ColumnWithTypeAndName encodeColumn(
        const ColumnWithTypeAndName & data_column,
        const String & dict_field_name,
        const ContextPtr & query_context,
        const BitengineWriteSettings & encoding_setting);

    ColumnWithTypeAndName encodeColumn(
        const ColumnWithTypeAndName & data_column,
        const String & dict_field_name,
        Int64 bucket_number,
        const ContextPtr & query_context,
        const BitengineWriteSettings & encoding_setting);

    ColumnPtr decodeColumn(
        const ColumnWithTypeAndName & data_column,
        const String & dict_field_name,
        const ContextPtr & query_context,
        [[maybe_unused]] const BitengineWriteSettings & decoding_setting);

    BitEngineHelper::DictionaryDatabaseAndTable getDictTable(const String & name) { return field_dictionary_mapping[name]; }

    /// TODO
#if 0
    MergeTreeMetaBase::MutableDataPartsVector
    encodeParts(
        const StorageCloudMergeTree & storage,
        const MergeTreeMetaBase::DataPartsVector & parts,
        const BitEngineEncodePartitionParam & params,
        const Context & context);
#endif

private:
    ContextPtr global_context;
    std::unordered_map<String, BitEngineHelper::DictionaryDatabaseAndTable> field_dictionary_mapping;
    ExpressionActionsPtr bitengine_table_cluster_by_key_expr;

    Poco::Logger * log;
};
}
