#include <memory>
#include <unordered_set>
#include <Analyzers/QualifiedName.h>
#include <Analyzers/SubstituteSelectItemToAnyFunction.h>
#include <Analyzers/function_utils.h>
#include <Core/Names.h>
#include <Interpreters/Aliases.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <common/types.h>


namespace DB
{

void SubstituteSelectItemToAnyFunction::visit(ASTPtr & ast)
{
    if (auto * select = ast->as<ASTSelectQuery>())
        visit(select);

    for (auto & child : ast->children)
        visit(child);
}

static bool needVisit(const ASTPtr & node)
{
    return !(node->as<ASTSelectQuery>() || node->as<ASTSubquery>() || node->as<ASTTableExpression>() || node->as<ASTLiteral>());
}

bool SubstituteSelectItemToAnyFunction::hasAggregate(ASTPtr & ast)
{
    if (auto * func = ast->as<ASTFunction>())
    {
        auto function_type = getFunctionType(*func, context);
        if (function_type == FunctionType::AGGREGATE_FUNCTION || function_type == FunctionType::GROUPING_OPERATION)
            return true;
        if (function_type == FunctionType::IN_SUBQUERY || function_type == FunctionType::EXISTS_SUBQUERY
            || function_type == FunctionType::LAMBDA_EXPRESSION || function_type == FunctionType::WINDOW_FUNCTION)
            return false;
    }
    if (!needVisit(ast))
        return false;
    bool has_aggregate = false;
    for (auto & child : ast->children)
        has_aggregate |= hasAggregate(child);
    return has_aggregate;
}

void SubstituteSelectItemToAnyFunction::visit(ASTSelectQuery * select_query)
{
    bool has_aggregate = false;
    bool has_group_by = false;

    if (select_query->groupBy())
        has_group_by = true;
    ASTs select_expressions;
    for (auto & select_item : select_query->refSelect()->children)
    {
        if (select_item->as<ASTAsterisk>() || select_item->as<ASTQualifiedAsterisk>() || select_item->as<ASTColumnsMatcher>()
            || (select_item->as<ASTFunction>() && select_item->as<ASTFunction>()->name == "untuple"))
            return;
        has_aggregate |= hasAggregate(select_item);

        if (has_group_by)
            select_expressions.push_back(select_item);
    }

    std::unordered_set<String> grouping_names;
    QualifiedNames grouping_qualified_names;
    if (has_group_by)
    {
        // get grouping
        bool allow_group_by_position = context->getSettingsRef().enable_positional_arguments && !select_query->group_by_with_rollup
            && !select_query->group_by_with_cube && !select_query->group_by_with_grouping_sets;
        auto get_grouping_expressions = [&](ASTs & grouping_expr_list) {
            for (ASTPtr grouping_expr : grouping_expr_list)
            {
                if (allow_group_by_position)
                    if (auto * literal = grouping_expr->as<ASTLiteral>(); literal && literal->tryGetAlias().empty()
                        && // avoid aliased expr being interpreted as positional argument
                        // e.g. SELECT 1 AS a ORDER BY a
                        literal->value.getType() == Field::Types::UInt64)
                    {
                        auto index = literal->value.get<UInt64>();
                        if (index > select_expressions.size() || index < 1)
                            return;
                        grouping_expr = select_expressions[index - 1];
                    }
                
                if (auto * grouping_identifier = grouping_expr->as<ASTIdentifier>())
                {
                    auto grouping_prefix = QualifiedName::extractQualifiedName(*grouping_identifier);
                    grouping_qualified_names.emplace_back(grouping_prefix);
                }
                grouping_names.emplace(grouping_expr->getAliasOrColumnName());
            }
        };

        if (select_query->group_by_with_grouping_sets)
        {
            for (auto & grouping_set_element : select_query->groupBy()->children)
                get_grouping_expressions(grouping_set_element->children);
        }
        else
        {
            get_grouping_expressions(select_query->groupBy()->children);
        }
    }

    if (!has_aggregate && !has_group_by)
        return;

    // process select
    NameAndQualifiedName processed_identifier_qualified_names;
    NameSet aliases;
    SubstituteIdentifierToAnyFunction::Data select_data{grouping_qualified_names, processed_identifier_qualified_names, aliases, context, true, true};
    SubstituteIdentifierToAnyFunction select_visitor(select_data);

    for (auto & select_item : select_query->refSelect()->children)
    {
        String name = select_item->getAliasOrColumnName();
        if (grouping_names.contains(name))
        {
        }
        else if (auto * select_identifier = select_item->as<ASTIdentifier>())
        {
            select_visitor.setAddAlias(true);
            if (aliases.contains(name) || !select_identifier->isShort())
                select_visitor.setAddAlias(false);
            select_visitor.visit(select_item);
        }
        else if (select_item->as<ASTFunction>())
        {
            select_visitor.setAddAlias(false);
            select_visitor.visit(select_item);
        }
        if (!select_item->tryGetAlias().empty())
            aliases.emplace(select_item->tryGetAlias());
    }

    // process having and order by
    if (!processed_identifier_qualified_names.empty())
    {
        SubstituteIdentifierToAnyFunction::Data expression_data{{}, processed_identifier_qualified_names, {}, context, false, false};
        SubstituteIdentifierToAnyFunction expression_visitor(expression_data);
        if (select_query->having())
            expression_visitor.visit(select_query->refHaving());
        if (select_query->orderBy())
            expression_visitor.visit(select_query->refOrderBy());
    }
}

void SubstituteIdentifierToAnyFunction::visit(ASTIdentifier & node, ASTPtr & ast, Data & data)
{
    String name = node.getAliasOrColumnName();
    if (!data.identifier_aliases.contains(name) && data.aliases.contains(name))
        return;
    auto qualified_name = QualifiedName::extractQualifiedName(node);
    if (data.process_grouping)
    {
        bool group_matched = false;
        for (const auto & grouping_qualified_name : data.grouping_qualified_names)
        {                
            if (qualified_name.hasSuffix(grouping_qualified_name) || grouping_qualified_name.hasSuffix(qualified_name))
            {
                group_matched = true;
                break;
            }
        }

        if (!group_matched)
        {
            bool has_alias = !node.tryGetAlias().empty();
            if (has_alias)
                node.setAlias("");
            ast = makeASTFunction("any", ast);
            if (has_alias || data.add_alias)
                ast->setAlias(name);
            data.identifier_aliases.emplace(name);
            data.processed_identifier_qualified_names.emplace_back(name, qualified_name);
        }
    }
    else
    {
        for (const auto & [name_, it_qualified_name] : data.processed_identifier_qualified_names)
        {
            if (name_ == node.name() && node.isShort())
                return;
            else if (it_qualified_name.hasSuffix(qualified_name) || qualified_name.hasSuffix(it_qualified_name))
            {
                ast = makeASTFunction("any", ast);
                return;
            }
        }
    }
}

void SubstituteIdentifierToAnyFunction::visitChildren(IAST * node, Data & data)
{
    if (auto * func_node = node->as<ASTFunction>())
    {
        auto function_type = getFunctionType(*func_node, data.context);
        if (function_type == FunctionType::AGGREGATE_FUNCTION || function_type == FunctionType::GROUPING_OPERATION)
            return;

        if (func_node->tryGetQueryArgument())
            return;
        /// We skip the first argument. We also assume that the lambda function can not have parameters.
        size_t first_pos = 0;
        if (func_node->name == "lambda")
            first_pos = 1;

        if (func_node->arguments)
        {
            auto & func_children = func_node->arguments->children;

            for (size_t i = first_pos; i < func_children.size(); ++i)
            {
                auto & child = func_children[i];

                if (needVisit(child))
                    visit(child, data);
            }
        }

        if (func_node->window_definition)
        {
            visitChildren(func_node->window_definition.get(), data);
        }
    }
    else if (!node->as<ASTSelectQuery>() && !node->as<ASTTablesInSelectQueryElement>() && !node->as<ASTLiteral>())
    {
        for (auto & child : node->children)
            if (needVisit(child))
                visit(child, data);
    }
}

void SubstituteIdentifierToAnyFunction::visit(ASTPtr & ast, Data & data)
{
    if (auto * node_id = ast->as<ASTIdentifier>())
    {
        visit(*node_id, ast, data);
        return;
    }

    visitChildren(ast.get(), data);
}

}
