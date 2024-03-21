#include <memory>
#include <Storages/MaterializedView/ApplyPartitionFilterVisitor.h>

#include <Interpreters/Context.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Common/Exception.h>

namespace DB
{
void ApplyPartitionFilterVisitor::visit(ASTPtr & ast, StorageID target_storage_id, ASTPtr target_filter, ContextPtr context)
{
    visit(ast, Data{.target_storage_id = target_storage_id, .target_filter = target_filter, .context = context});
}

void ApplyPartitionFilterVisitor::visit(ASTSelectQuery & ast, StorageID target_storage_id, ASTPtr target_filter, ContextPtr context)
{
    visit(ast, Data{.target_storage_id = target_storage_id, .target_filter = target_filter, .context = context});
}

void ApplyPartitionFilterVisitor::visit(
    ASTSelectWithUnionQuery & ast, StorageID target_storage_id, ASTPtr target_filter, ContextPtr context)
{
    visit(ast, Data{.target_storage_id = target_storage_id, .target_filter = target_filter, .context = context});
}

void ApplyPartitionFilterVisitor::visit(ASTPtr & ast, const Data & data)
{
    if (auto * node_select = ast->as<ASTSelectQuery>())
        visit(*node_select, data);
    else
    {
        for (auto & child : ast->children)
            visit(child, data);
        if (auto * node_func = ast->as<ASTFunction>())
            visit(*node_func, data);
        else if (auto * node_table = ast->as<ASTTableExpression>())
            visit(*node_table, data);
        // else if (auto * identifier = ast->as<ASTIdentifier>())
            // visit(*identifier, data);
    }
}

void ApplyPartitionFilterVisitor::visit(ASTSelectQuery & ast, const Data & data)
{
    std::optional<Data> new_data;
    if (auto with = ast.with())
    {
        for (auto & child : with->children)
        {
            visit(child, new_data ? *new_data : data);
            if (auto * ast_with_elem = child->as<ASTWithElement>())
            {
                if (!new_data)
                    new_data = data;
                new_data->subqueries[ast_with_elem->name] = ast_with_elem->subquery;
            }
        }
    }

    for (auto & child : ast.children)
    {
        if (child != ast.with())
            visit(child, new_data ? *new_data : data);
    }
}

void ApplyPartitionFilterVisitor::visit(ASTSelectWithUnionQuery & ast, const Data & data)
{
    for (auto & child : ast.children)
        visit(child, data);
}

void ApplyPartitionFilterVisitor::visit(ASTTableExpression & table_expression, const Data & data)
{
    if (table_expression.database_and_table_name)
    {
        auto table_identifier = table_expression.database_and_table_name;
        auto storage_id 
            = DatabaseAndTableWithAlias(table_identifier, data.context->getCurrentDatabase()).getStorageID();
        if (!data.subqueries.contains(storage_id.table_name) && storage_id == data.target_storage_id)
        {
            auto alias = table_identifier->tryGetAlias();
            if (alias.empty())
                alias = storage_id.table_name;

            table_expression.database_and_table_name.reset();
            table_expression.children.clear();

            table_expression.subquery = constructSubquery(table_identifier, data.target_filter, alias);
            table_expression.children.push_back(table_expression.subquery);
        }
    }
}

void ApplyPartitionFilterVisitor::visit(ASTFunction & func, const Data & data)
{
    /// Special case, where the right argument of IN is alias (ASTIdentifier) .
    if (checkFunctionIsInOrGlobalInOperator(func))
    {
        auto & ast = func.arguments->children.at(1);
        if (const auto * identifier = ast->as<ASTIdentifier>())
        {
            auto table_expression = identifier->createTable();
            auto storage_id
                = DatabaseAndTableWithAlias(table_expression, data.context->getCurrentDatabase()).getStorageID();

             if (!data.subqueries.contains(storage_id.table_name) && storage_id == data.target_storage_id)
             {
                ast = constructSubquery(table_expression, data.target_filter, ast->tryGetAlias());
             }
        }
    }
}

ASTPtr ApplyPartitionFilterVisitor::constructSubquery(const ASTPtr & table_identifier, const ASTPtr & filter, const String & alias)
{
    auto query = std::make_shared<ASTSelectQuery>();
    query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());

    auto table_expression = std::make_shared<ASTTableExpression>();
    table_expression->database_and_table_name = table_identifier;
    table_expression->children.push_back(table_identifier);
    auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    table_element->table_expression = table_expression;
    table_element->children.push_back(table_expression);
    query->tables()->children.emplace_back(table_element);

    auto output_list = std::make_shared<ASTExpressionList>();
    output_list->children.emplace_back(std::make_shared<ASTAsterisk>());
    query->setExpression(ASTSelectQuery::Expression::SELECT, output_list);
    query->setExpression(ASTSelectQuery::Expression::WHERE, filter->clone());

    auto subquery = std::make_shared<ASTSubquery>();
    subquery->children.push_back(query);
    if (!alias.empty())
        subquery->setAlias(alias);
    return subquery;
}

// void ApplyPartitionFilterVisitor::visit(ASTIdentifier & identifier, const Data & data)
// {
//     if (identifier.nameParts().size() == 3)
//     {
//         auto table_expression = std::make_shared<ASTTableIdentifier>(identifier.nameParts()[0], identifier.nameParts()[1]);
//         auto storage_id = DatabaseAndTableWithAlias(table_expression, data.context->getCurrentDatabase()).getStorageID();
//         if (storage_id == data.target_storage_id)
//         {
//              std::vector<String> name_parts{identifier.nameParts().end() - 2, identifier.nameParts().end()};
//              identifier = ASTIdentifier{std::move(name_parts)};
//         }
//     }
// }
}
