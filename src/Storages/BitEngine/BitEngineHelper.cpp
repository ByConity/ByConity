#include <Columns/ColumnBitMap64.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTIdentifier.h>
#include <Poco/String.h>
#include <Storages/BitEngine/BitEngineHelper.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Storages/StorageCnchMergeTree.h>
#include <common/logger_useful.h>
#include "Interpreters/ActionsDAG.h"
#include <Interpreters/StorageID.h>

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_QUERY;
}

namespace BitEngineHelper
{
    String keyTypeToString(const KeyType & key_type)
    {
        switch (key_type)
        {
            case KeyType::KEY_STRING:
                return "KEY_STRING";
            case KeyType::KEY_INTEGER:
                return "KEY_INTEGER";
            default:
                return "ILLEGAL";
        }
    }

    std::map<String,String> parseUnderlyingDictionarySetting(const String & mapping_str)
    {
        std::map<String, String> res_map;
        using namespace rapidjson;
        Document document;
        ParseResult ok = document.Parse(mapping_str.data(), mapping_str.length());
        if (!ok)
            throw Exception(
                String("JSON parse error ") + GetParseError_En(ok.Code()) + " " + toString(ok.Offset()), ErrorCodes::BAD_ARGUMENTS);

        for (auto & member : document.GetObject())
        {
            if (!member.value.IsString())
                continue;
            auto && column = member.name.GetString();
            auto && table = member.value.GetString();
            LOG_TRACE(&Poco::Logger::get("BitEngineHelper"), "[UnderlyingDictionaryTables] {} : {}", column, table);
            res_map[column] = table;
        }
        return res_map;
    }

    void generateBlockForEncode(Block & block, size_t argument)
    {
        auto & column_to_encode = block.getByPosition(argument);

        block.insert(ColumnWithTypeAndName{
            column_to_encode.type->createColumn(),
            column_to_encode.type,
            column_to_encode.name + BITENGINE_COLUMN_EXTENSION
        });
    }

    ColumnWithTypeAndName generateColumnForEncode(const ColumnWithTypeAndName & column)
    {
        return ColumnWithTypeAndName{
            column.type->createColumn(),
            column.type,
            column.name + BITENGINE_COLUMN_EXTENSION
        };
    }

    bool isBitEngineEncodeDecodeFunction(const String & func_name)
    {
        return BITENGINE_ENCODE_DECODE_FUNCTIONS.count(func_name);
    }

    bool isBitEngineEncodeDecodeFunction(const ASTPtr & node)
    {
        if (!node)
            return false;

        if (const auto * func = node->as<ASTFunction>())
        {
            String func_name = Poco::toLower(func->name);
            if (!BitEngineHelper::isBitEngineEncodeDecodeFunction(func_name))
            {
                bool is_bitengine_func = false;
                for (const auto & child : func->children)
                    is_bitengine_func |= BitEngineHelper::isBitEngineEncodeDecodeFunction(child);
                return is_bitengine_func;
            }
            else
                return true;
        }
        else
        {
            bool is_bitengine_func = false;
            for (auto & child : node->children)
                is_bitengine_func |= BitEngineHelper::isBitEngineEncodeDecodeFunction(child);
            return is_bitengine_func;
        }
    }

    void BitEngineDictInfoSet::emplaceCnchTable(
        const UUID & dict_table_uuid, const String & cnch_dict_db, const String & cnch_dict_table)
    {
        BitEngineDictInfo info;
        info.uuid = dict_table_uuid;
        info.cnch_bitengine_table = DictionaryDatabaseAndTable{cnch_dict_db, cnch_dict_table};
        emplace(info);
    }

    void BitEngineDictInfoSet::emplaceCloudTable(
        const UUID & dict_table_uuid, const String & cloud_dict_db, const String & cloud_dict_table)
    {
        BitEngineDictInfo info;
        info.uuid = dict_table_uuid;

        auto it = tables.find(info);
        if (it == tables.end())
            throw Exception("Not found UUID: " + toString(dict_table_uuid), ErrorCodes::LOGICAL_ERROR);

        auto node = tables.extract(info);
        auto & target = node.value().cloud_bitengine_table;
        target.database = cloud_dict_db;
        target.table = cloud_dict_table;

        tables.insert(std::move(node));
    }

    ExpressionActionsPtr generateBitEngineProjectionActions(NamesWithAliases bitengine_projection, const Block & header, [[maybe_unused]]const ContextPtr & context)
    {
        if (bitengine_projection.empty())
            return nullptr;
        else
        {
            auto actions = std::make_shared<ActionsDAG>(header.getColumnsWithTypeAndName());
            actions->project(bitengine_projection);
            return std::make_shared<ExpressionActions>(actions);
        }
    }

    std::pair<ExpressionActionsPtr, bool> generateBitEngineProjection(
        const ASTSelectQuery * select,
        const ContextPtr & context,
        const Block & header,
        const Names & columns_to_project)
    {
        if (!select)
            return {};

        bool need_projection = false;
        bool has_bitengine_func = false;
        NamesWithAliases bitengine_projection;
        Block projected_header = header;

        for (size_t i = 0; i < select->select()->children.size(); ++i)
        {
            auto & child = select->select()->children[i];
            auto * function = child->as<ASTFunction>();
            if (!function)
            {
                bitengine_projection.emplace_back(child->getColumnName(), "");
                continue;
            }

            /// If it is not BitEngine func, we will not rewrite it, so getColumnName is safe.
            if (!BitEngineHelper::isBitEngineEncodeDecodeFunction(child))
            {
                bitengine_projection.emplace_back(child->getColumnName(), "");
                continue;
            }
            
            String column_name = columns_to_project[i];
            has_bitengine_func = true;
            /**
            * if we cannot find column_name in the header or find alias in projected_header, 
            * it means a projection is done in worker node, 
            * so that bitengine projection is unnecessary. 
            */
            if (!projected_header.has(column_name) || projected_header.has(function->tryGetAlias()))
            {
                bitengine_projection.emplace_back(function->getAliasOrColumnName(), "");
            }
            else
            {
                need_projection = true;
                bitengine_projection.emplace_back(function->getColumnName(), column_name);
                auto & column = projected_header.getByName(column_name);
                column.name = function->getColumnName();
            }
        }

        if (need_projection)
        {
            LOG_DEBUG(&Poco::Logger::get("CnchStorageCommonHelper"), "Generate projection for BitEngineFunctions: " + projected_header.dumpStructure());
            return {BitEngineHelper::generateBitEngineProjectionActions(bitengine_projection, projected_header, context), has_bitengine_func};
        }
        else
            return {nullptr, has_bitengine_func};
    }

    bool rewriteBitEngineFunctionRelatedTables(
        ASTPtr & node,
        const ContextPtr & context,
        BitEngineDictInfoSet & bitengine_dictionary_info,
        const StorageID & table_id)
    {
        if (!node)
            return false;

        auto get_field_from_function_ast = [](const ASTFunction * func_ast, size_t pos) -> Field
        {
            if (auto * literal = func_ast->arguments->children[pos]->as<ASTLiteral>())
                return literal->value;
            else
                throw Exception("BitEngineFunctions only accept literal", ErrorCodes::LOGICAL_ERROR);
        };

        auto replace_bit_engine_table_to_cloud = [](ASTFunction * func_ast, String & new_value)
        {
            func_ast->arguments->children[2]->as<ASTLiteral>()->value = Field(new_value);
        };

        if (auto * function = node->as<ASTFunction>())
        {
            String func_name = Poco::toLower(function->name);
            if (!BitEngineHelper::isBitEngineEncodeDecodeFunction(func_name))
            {
                bool rewrited = false;
                for (auto & child : function->children)
                {
                    rewrited |= rewriteBitEngineFunctionRelatedTables(child, context, bitengine_dictionary_info, table_id);
                }
                return rewrited;
            }
            
            /// if it's a bitengine encode/decode function
            String bitengine_db = get_field_from_function_ast(function, 1).safeGet<String>();
            String bitengine_tbl = get_field_from_function_ast(function, 2).safeGet<String>();


            auto storage = DatabaseCatalog::instance().tryGetTable(StorageID{bitengine_db, bitengine_tbl}, context);
            auto * storage_cnch = dynamic_cast<StorageCnchMergeTree *>(storage.get());
            if (!storage_cnch->isBitEngineTable())
            {
                throw Exception(fmt::format("Table specified {}.{} in function {} is "\
                    "not a BitEngine table", bitengine_db, bitengine_tbl, function->name),
                    ErrorCodes::LOGICAL_ERROR);
            }

            String bitengine_tbl_cloud = storage_cnch->getCloudTableName(context);
            replace_bit_engine_table_to_cloud(function, bitengine_tbl_cloud);
        
            auto uuid = storage->getStorageUUID();
            if (uuid == table_id.uuid)
                return true;

            bitengine_dictionary_info.emplaceCnchTable(uuid, bitengine_db, bitengine_tbl);
            bitengine_dictionary_info.emplaceCloudTable(uuid, bitengine_db, bitengine_tbl_cloud);
            return true;
        }
        else
        {
            bool rewrited = false;
            for (auto & child : node->children)
            {
                rewrited |= rewriteBitEngineFunctionRelatedTables(child, context, bitengine_dictionary_info, table_id);
            }
            return rewrited;
        }
    }

}

}
