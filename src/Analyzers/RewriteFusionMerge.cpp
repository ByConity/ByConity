#include "Analyzers/RewriteFusionMerge.h"
#include "Common/Exception.h"
#include "Common/typeid_cast.h"
#include "common/logger_useful.h"
#include "Core/Names.h"
#include "Core/SettingsEnums.h"
#include "DataTypes/DataTypeDateTime.h"
#include "DataTypes/IDataType.h"
#include "Interpreters/StorageID.h"
#include "Interpreters/evaluateConstantExpression.h"
#include "Parsers/ASTAsterisk.h"
#include "Parsers/ASTExpressionList.h"
#include "Parsers/ASTFunction.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTLiteral.h"
#include "Parsers/ASTSelectQuery.h"
#include "Parsers/ASTSelectWithUnionQuery.h"
#include "Parsers/ASTSubquery.h"
#include "Parsers/ASTTablesInSelectQuery.h"
#include "Parsers/IAST_fwd.h"
#include "Parsers/formatAST.h"
#include "Storages/IStorage.h"
#include "Storages/MergeTree/KeyCondition.h"
#include "boost/lexical_cast.hpp"

#include <algorithm>
#include <memory>
#include <string>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNRECOGNIZED_ARGUMENTS;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int SYNTAX_ERROR;
}

namespace
{
    Range convertToServeTimeRange(String str_server_time)
    {
        if (str_server_time.empty())
            throw Exception("Error when parse server time range: " + str_server_time, ErrorCodes::UNRECOGNIZED_ARGUMENTS);

        str_server_time.erase(std::remove(str_server_time.begin(), str_server_time.end(), ' '), str_server_time.end());
        size_t len = str_server_time.size();
        if (len < 2)
            throw Exception("Error when parse server time range: " + str_server_time, ErrorCodes::UNRECOGNIZED_ARGUMENTS);

        if (str_server_time[0] != '[' && str_server_time[0] != '(')
            throw Exception("Error when parse server time range: " + str_server_time, ErrorCodes::UNRECOGNIZED_ARGUMENTS);

        if (str_server_time[len - 1] != ']' && str_server_time[len - 1] != ')')
            throw Exception("Error when parse server time range: " + str_server_time, ErrorCodes::UNRECOGNIZED_ARGUMENTS);

        Range range = Range::createWholeUniverseWithoutNull();
        if (len == 2)
            return range;

        size_t spliter_pos = str_server_time.find(',');
        if (spliter_pos == String::npos)
            throw Exception("Error when parse server time range: " + str_server_time, ErrorCodes::UNRECOGNIZED_ARGUMENTS);

        if (spliter_pos != 1)
        {
            String left = str_server_time.substr(1, spliter_pos - 1);
            range.left = Field(boost::lexical_cast<UInt64>(left));
            if (str_server_time[0] == '[')
                range.left_included = true;
        }

        if (spliter_pos + 2 != len)
        {
            String right = str_server_time.substr(spliter_pos + 1, len - spliter_pos - 2);
            range.right = Field(boost::lexical_cast<UInt64>(right));
            if (str_server_time[len - 1] == ']')
                range.right_included = true;
        }

        return range;
    }

    ASTPtr makeDateLiteral(const Field & timestamp)
    {
        const static UInt64 mills_test = 2000000000ULL;
        UInt64 field_time = timestamp.safeGet<UInt64>();
        if (field_time > mills_test)
        {
            String date = DateLUT::serverTimezoneInstance().dateToString(field_time / 1000);
            return std::make_shared<ASTLiteral>(Field(date));
        }
        else
        {
            String date = DateLUT::serverTimezoneInstance().dateToString(field_time);
            return std::make_shared<ASTLiteral>(Field(date));
        }
    }

    ASTPtr makeFusionCondition(
        const String & event_date_name,
        const String & server_time_expr_name,
        const IDataType * server_time_type,
        const Range & server_time_interval)
    {
        ASTPtr fusion_expression = nullptr;
        if (!server_time_interval.left.isNull() || !server_time_interval.right.isNull())
        {
            ASTPtr event_date_identifier;
            ASTPtr server_time_expr;
            ASTPtr to_event_date;
            ASTPtr to_server_time;
            ASTPtr server_time_function;
            ASTPtr event_date_function;
            if (!server_time_interval.left.isNull())
            {
                event_date_identifier = std::make_shared<ASTIdentifier>(event_date_name);
                server_time_expr = std::make_shared<ASTIdentifier>(server_time_expr_name);
                to_event_date = makeDateLiteral(server_time_interval.left);
                if (typeid_cast<const DataTypeDateTime *>(server_time_type))
                    to_server_time = makeASTFunction("toDateTime", std::make_shared<ASTLiteral>(server_time_interval.left));
                else
                    to_server_time = std::make_shared<ASTLiteral>(server_time_interval.left);

                event_date_function = makeASTFunction("greaterOrEquals", event_date_identifier, to_event_date);
                if (server_time_interval.left_included)
                    server_time_function = makeASTFunction("greaterOrEquals", server_time_expr, to_server_time);
                else
                    server_time_function = makeASTFunction("greater", server_time_expr, to_server_time);

                if (fusion_expression)
                    fusion_expression = makeASTFunction("and", ASTs{fusion_expression, event_date_function, server_time_function});
                else
                    fusion_expression = makeASTFunction("and", ASTs{event_date_function, server_time_function});
            }
            if (!server_time_interval.right.isNull())
            {
                event_date_identifier = std::make_shared<ASTIdentifier>(event_date_name);
                server_time_expr = std::make_shared<ASTIdentifier>(server_time_expr_name);
                to_event_date = makeDateLiteral(server_time_interval.right);
                if (typeid_cast<const DataTypeDateTime *>(server_time_type))
                    to_server_time = makeASTFunction("toDateTime", std::make_shared<ASTLiteral>(server_time_interval.right));
                else
                    to_server_time = std::make_shared<ASTLiteral>(server_time_interval.right);

                event_date_function = makeASTFunction("lessOrEquals", event_date_identifier, to_event_date);
                if (server_time_interval.right_included)
                    server_time_function = makeASTFunction("lessOrEquals", server_time_expr, to_server_time);
                else
                    server_time_function = makeASTFunction("less", server_time_expr, to_server_time);

                if (fusion_expression)
                    fusion_expression = makeASTFunction("and", ASTs{fusion_expression, event_date_function, server_time_function});
                else
                    fusion_expression = makeASTFunction("and", ASTs{event_date_function, server_time_function});
            }
        }

        return fusion_expression;
    }
}

void RewriteFusionMerge::visit(ASTTableExpression & table_expr, ASTPtr &)
{
    if (!table_expr.table_function)
        return;

    auto table_func_ptr = table_expr.table_function;
    const auto * table_func = table_func_ptr->as<ASTFunction>();

    if (!table_func || table_func->name != "fusionMerge")
        return;

    const auto & table_args = table_func->arguments->children;

    if (table_args.size() != 7)
        throw Exception("Table function 'fusionMerge' requires exactly 7 arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    String database_name
        = evaluateConstantExpressionOrIdentifierAsLiteral(table_args[0], context)->as<ASTLiteral &>().value.safeGet<String>();
    String table1_name
        = evaluateConstantExpressionOrIdentifierAsLiteral(table_args[1], context)->as<ASTLiteral &>().value.safeGet<String>();
    String table2_name
        = evaluateConstantExpressionOrIdentifierAsLiteral(table_args[2], context)->as<ASTLiteral &>().value.safeGet<String>();
    String event_date_name
        = evaluateConstantExpressionOrIdentifierAsLiteral(table_args[3], context)->as<ASTLiteral &>().value.safeGet<String>();
    ASTPtr server_time_ast = table_args[4];
    Range server_time_interval1
        = convertToServeTimeRange(evaluateConstantExpressionAsLiteral(table_args[5], context)->as<ASTLiteral &>().value.safeGet<String>());
    Range server_time_interval2
        = convertToServeTimeRange(evaluateConstantExpressionAsLiteral(table_args[6], context)->as<ASTLiteral &>().value.safeGet<String>());

    String server_time_column_name;
    String server_time_expr_name;

    if (auto * server_time_func = server_time_ast->as<ASTFunction>())
    {
        server_time_column_name = server_time_func->arguments->children.back()->getColumnName();
        server_time_expr_name = server_time_func->tryGetAlias();
        if (server_time_expr_name.empty())
        {
            server_time_expr_name = "--calc_server_time_for_fusion_merge__" + std::to_string(reinterpret_cast<uintptr_t>(table_func));
        }
        if (server_time_expr_name == server_time_column_name)
        {
            throw Exception(ErrorCodes::SYNTAX_ERROR, "fusion merge rerwite fails: unexpected server time expression");
        }
    }
    else
    {
        server_time_column_name = server_time_ast->as<ASTIdentifier &>().name();
        server_time_expr_name = server_time_ast->as<ASTIdentifier &>().name();
    }

    auto selected_columns = [&]() -> Names {
        auto get_columns_for_table = [&](auto & table_name) {
            auto table_id = context->resolveStorageID(StorageID{database_name, table_name});
            const auto table = DatabaseCatalog::instance().getTable(table_id, context);
            auto table_metadata_snapshot = table->getInMemoryMetadataPtr();
            const auto & columns = table_metadata_snapshot->getColumns();
            auto ordinary = columns.getOrdinary();
            auto materialized = columns.getMaterialized();
            auto aliases = columns.getAliases();
            ordinary.insert(ordinary.end(), materialized.begin(), materialized.end());
            ordinary.insert(ordinary.end(), aliases.begin(), aliases.end());
            return ordinary;
        };

        auto table1_columns = get_columns_for_table(table1_name);
        auto table2_columns = get_columns_for_table(table2_name);
        Names common_columns;

        for (const auto & table1_column : table1_columns)
            for (const auto & table2_column : table2_columns)
                if (table1_column.name == table2_column.name)
                {
                    common_columns.push_back(table1_column.name);
                    break;
                }

        return common_columns;
    }();

    auto create_select_for_base_table = [&](const String & table_name, const Range & server_time_interval) -> ASTPtr {
        auto table = DatabaseCatalog::instance().getTable(StorageID{database_name, table_name}, context);
        auto storage_metadata = table->getInMemoryMetadataPtr();
        auto server_time_column = storage_metadata->getColumns().tryGetPhysical(server_time_column_name);
        if (!server_time_column)
            throw Exception(
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
                "Table {}.{} does not have server time column: {}",
                database_name,
                table_name,
                server_time_column_name);
        auto fusion_cond
            = makeFusionCondition(event_date_name, server_time_expr_name, server_time_column->type.get(), server_time_interval);
        auto select_query = std::make_shared<ASTSelectQuery>();
        auto select_clause = std::make_shared<ASTExpressionList>();

        for (const auto & column : selected_columns)
            select_clause->children.push_back(std::make_shared<ASTIdentifier>(column));

        if (server_time_expr_name != server_time_column_name)
        {
            auto server_time_ast_cloned = server_time_ast->clone();
            server_time_ast_cloned->setAlias(server_time_expr_name);
            select_clause->children.push_back(server_time_ast_cloned);
        }
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_clause));
        select_query->replaceDatabaseAndTable(database_name, table_name);
        if (fusion_cond)
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(fusion_cond));
        return select_query;
    };

    auto union_list = std::make_shared<ASTExpressionList>();
    union_list->children.push_back(create_select_for_base_table(table1_name, server_time_interval1));
    union_list->children.push_back(create_select_for_base_table(table2_name, server_time_interval2));

    auto select_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    select_union_query->list_of_selects = union_list;
    select_union_query->children.push_back(select_union_query->list_of_selects);
    select_union_query->union_mode = ASTSelectWithUnionQuery::Mode::UNION_ALL;
    select_union_query->is_normalized = true;

    auto subquery = std::make_shared<ASTSubquery>();
    subquery->children.push_back(select_union_query);
    if (!table_func->tryGetAlias().empty())
        subquery->setAlias(table_func->tryGetAlias());

    table_expr.table_function = nullptr;
    table_expr.subquery = subquery;
    table_expr.children.clear();
    table_expr.children.push_back(table_expr.subquery);

    LOG_DEBUG(
        getLogger("RewriteFusionMerge"), "Rewrite {} to {}", serializeAST(*table_func_ptr), serializeAST(*select_union_query));
}

}
