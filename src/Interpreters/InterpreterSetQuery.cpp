#include <Interpreters/InterpreterSetQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/PreparedStatement/PreparedStatementManager.h>
#include <Interpreters/PreparedStatement/PreparedStatementManager.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTPreparedStatement.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include "common/types.h"

namespace DB
{


BlockIO InterpreterSetQuery::execute()
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    getContext()->checkSettingsConstraints(ast.changes);
    getContext()->getSessionContext()->applySettingsChanges(ast.changes, false);
    return {};
}


void InterpreterSetQuery::executeForCurrentContext()
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    getContext()->checkSettingsConstraints(ast.changes);
    getContext()->applySettingsChanges(ast.changes, false);
}

void InterpreterSetQuery::applySettingsFromQuery(const ASTPtr & ast, ContextMutablePtr setting_scontext)
{
    auto changes = extractSettingsFromQuery(ast, setting_scontext);
    auto new_settings = std::make_shared<ASTSetQuery>();
    new_settings->changes = std::move(changes);
    InterpreterSetQuery(new_settings, setting_scontext).executeForCurrentContext();
}

static SettingsChanges extractSettingsFromSetQuery(const ASTPtr & ast)
{
    if (!ast)
        return {};

    const auto & set_query = ast->as<ASTSetQuery &>();
    return set_query.changes;
}

static SettingsChanges extractSettingsFromSelectWithUnion(const ASTSelectWithUnionQuery & select_with_union)
{
    auto settings = extractSettingsFromSetQuery(select_with_union.settings_ast);
    const ASTs & children = select_with_union.list_of_selects->children;
    if (!children.empty())
    {
        // We might have an arbitrarily complex UNION tree, so just give
        // up if the last first-order child is not a plain SELECT.
        // It is flattened later, when we process UNION ALL/DISTINCT.
        const auto * last_select = children.back()->as<ASTSelectQuery>();
        if (last_select && last_select->settings())
        {
            auto select_settings = extractSettingsFromSetQuery(last_select->settings());
            settings.merge(select_settings);
        }
    }
    return settings;
}

SettingsChanges InterpreterSetQuery::extractSettingsFromQuery(const ASTPtr & ast, ContextMutablePtr settings_context)
{
    if (!ast)
        return {};

    if (const auto * select_query = ast->as<ASTSelectQuery>())
    {
        return extractSettingsFromSetQuery(select_query->settings());
    }
    else if (const auto * select_with_union_query = ast->as<ASTSelectWithUnionQuery>())
    {
        return extractSettingsFromSelectWithUnion(*select_with_union_query);
    }
    else if (const auto * explain_query = ast->as<ASTExplainQuery>())
    {
        auto settings = extractSettingsFromSetQuery(explain_query->settings_ast);

        if (const auto * inner_select_union = explain_query->getExplainedQuery()->as<ASTSelectWithUnionQuery>())
        {
            auto inner_select_union_settings = extractSettingsFromSelectWithUnion(*inner_select_union);
            settings.merge(inner_select_union_settings);
        }
        else if (const auto * inner_insert = explain_query->getExplainedQuery()->as<ASTInsertQuery>())
        {
            auto inner_insert_settings = extractSettingsFromSetQuery(inner_insert->settings_ast);
            settings.merge(inner_insert_settings);
        }
        return settings;
    }
    else if (const auto * prepare_query = ast->as<ASTCreatePreparedStatementQuery>())
    {
        SettingsChanges settings;

        if (const auto * inner_select_union = prepare_query->getQuery()->as<ASTSelectWithUnionQuery>())
        {
            auto inner_select_union_settings = extractSettingsFromSelectWithUnion(*inner_select_union);
            settings.merge(inner_select_union_settings);
        }
        else if (const auto * inner_insert = prepare_query->getQuery()->as<ASTInsertQuery>())
        {
            auto inner_insert_settings = extractSettingsFromSetQuery(inner_insert->settings_ast);
            settings.merge(inner_insert_settings);
        }
        return settings;
    }
    else if (const auto * execute_query = ast->as<ASTExecutePreparedStatementQuery>())
    {
        // Settings of EXECUTE PREPARED STATEMENT should include settings of corresponding CREATE PREPARED STATEMENT
        auto * prepared_stat_manager = settings_context->getPreparedStatementManager();
        if (!prepared_stat_manager)
            throw Exception("Prepared statement cache is not initialized", ErrorCodes::LOGICAL_ERROR);

        auto settings = prepared_stat_manager->getSettings(execute_query->getName());
        auto execute_settings = extractSettingsFromSetQuery(execute_query->settings_ast);
        settings.merge(execute_settings);
        return settings;
    }
    else if (const auto * insert_query = ast->as<ASTInsertQuery>())
    {
        return extractSettingsFromSetQuery(insert_query->settings_ast);
    }
    else if (const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get()))
    {
        return extractSettingsFromSetQuery(query_with_output->settings_ast);
    }

    return {};
}

void InterpreterSetQuery::applyABTestProfile(ContextMutablePtr query_context)
{
    if (query_context->getSettingsRef().enable_ab_test)
    {
        String ab_test_profile = query_context->getSettingsRef().ab_test_profile;
        Float64 ab_test_traffic_factor = query_context->getSettingsRef().ab_test_traffic_factor;
        if (ab_test_profile != "default" && ab_test_traffic_factor > 0 && ab_test_traffic_factor <= 1)
        {
            try
            {
                std::random_device rd;
                std::mt19937 gen(rd()); 
                std::uniform_real_distribution<DB::Float64> distribution(0.0, 1.0);
                Float64 res = distribution(gen);
                if (res <= ab_test_traffic_factor)
                    query_context->setCurrentProfile(ab_test_profile);
            }
            catch (...)
            {
                tryLogWarningCurrentException(getLogger("applyABTestProfile"), "Apply ab test profile failed.");
            }
        }
        else
        {
            LOG_WARNING(getLogger("applyABTestProfile"), "Apply ab test profile failed, ab_test_traffic_factor must be between 0 and 1, ab_test_profile != default");        
        }
    }
}
}
