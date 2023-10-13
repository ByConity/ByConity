#pragma once

#include <Core/Types.h>
#include <Core/Block.h>
#include <Storages/IStorage.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Common/HashTable/Hash.h>

namespace DB {

namespace BitEngineHelper {
    enum KeyType : UInt8 {KEY_INTEGER, KEY_STRING,};
    String keyTypeToString(const KeyType & key_type);

    struct DictionaryDatabaseAndTable : DatabaseAndTableWithAlias
    {
        KeyType dict_key_type;

        DictionaryDatabaseAndTable(
            const String & dict_db, const String & dict_table,
            const KeyType dict_key_type_, UUID dict_table_uuid = UUIDHelpers::Nil)
        : dict_key_type(dict_key_type_)
        {
            database = dict_db;
            table = dict_table;
            uuid = dict_table_uuid;
        }

        DictionaryDatabaseAndTable(const String & dict_db, const String & dict_table)
        : dict_key_type(KeyType::KEY_INTEGER)
        {
            database = dict_db;
            table = dict_table;
        }

        DictionaryDatabaseAndTable() = default;
    };

    /// each BitEngine column has a dictionary table, so its a map in JSON,
    /// like: {"uids1": "db.tbl1", "uids2": "db.tbl2"}
    std::map<String,String> parseUnderlyingDictionarySetting(const String & mapping_str);

    void generateBlockForEncode(Block & block, size_t argument);
    ColumnWithTypeAndName generateColumnForEncode(const ColumnWithTypeAndName & column);

    /// all encode/decode function names in lower case
    const std::set<String> BITENGINE_ENCODE_DECODE_FUNCTIONS{
        "encodebitmap",
        "decodenonbitenginecolumn",
        "arraytobitmapwithencode",
        "encodenonbitenginecolumn",
        "decodebitmap",
        "bitmaptoarraywithdecode",
        "bitenginedecode"
    };

    /// check where the func_name is a BitEngine encode/decode function
    /// func_name should be lower case
    bool isBitEngineEncodeDecodeFunction(const String & func_name);
    bool isBitEngineEncodeDecodeFunction(const ASTPtr & node);

    class BitEngineDictInfo
    {
    public:
        UUID uuid = UUIDHelpers::Nil;
        DictionaryDatabaseAndTable cnch_bitengine_table;
        DictionaryDatabaseAndTable cloud_bitengine_table;

        bool operator==(const BitEngineDictInfo & info) const
        {
            return uuid == info.uuid;
        }
    };

    class BitEngineDictInfoHash
    {
    private:
        UInt128Hash hasher;
    public:
        size_t operator()(const BitEngineDictInfo & info) const
        {
            return hasher(info.uuid);
        }
    };

    struct BitEngineDictInfoSet
    {
        std::unordered_set<BitEngineDictInfo, BitEngineHelper::BitEngineDictInfoHash> tables;

        bool empty() const { return tables.empty(); }
        void emplace(const BitEngineDictInfo & info) { tables.emplace(info); }
        void emplaceCnchTable(const UUID & dict_table_uuid, const String & cnch_dict_db, const String & cnch_dict_table);
        void emplaceCloudTable(const UUID & dict_table_uuid, const String & cloud_dict_db, const String & cloud_dict_table);
    };

    ExpressionActionsPtr generateBitEngineProjectionActions(NamesWithAliases bitengine_projection, const Block & header, const ContextPtr & context);
    
    std::pair<ExpressionActionsPtr, bool> generateBitEngineProjection(
        const ASTSelectQuery * select,
        const ContextPtr & context,
        const Block & header,
        const Names & columns_to_project);

    bool rewriteBitEngineFunctionRelatedTables(
        ASTPtr & node,
        const ContextPtr & context,
        BitEngineDictInfoSet & bitengine_dictionary_info,
        const StorageID & table_id);
}

using KeyType = BitEngineHelper::KeyType;
using BitEngineDictionaryTableMapping = std::unordered_map<String, std::pair<String, String>>;
using BitEngineDictInfo = BitEngineHelper::BitEngineDictInfo;
using BitEngineDictInfoSet = BitEngineHelper::BitEngineDictInfoSet;

}
