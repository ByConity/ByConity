#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Storages/StorageView.h>

namespace DB
{

struct ReplaceViewWithSubquery
{
    using TypeToVisit = ASTTableExpression;

    ContextPtr context;
    explicit ReplaceViewWithSubquery(ContextPtr context_) : context(std::move(context_)) { }

    void visit(ASTTableExpression & table_expression, ASTPtr &) const
    {
        if (table_expression.database_and_table_name)
        {
            auto db_and_table = DatabaseAndTableWithAlias(table_expression.database_and_table_name, context->getCurrentDatabase());

            auto table_name = db_and_table.table;
            auto database_name = db_and_table.database;

            if (database_name == "system")
                return;

            auto table_id = context->tryResolveStorageID(table_expression.database_and_table_name);
            auto table = DatabaseCatalog::instance().tryGetTable(table_id, context);
            if (!table)
                return;

            if (dynamic_cast<const StorageView *>(table.get()))
            {
                auto table_metadata_snapshot = table->getInMemoryMetadataPtr();
                auto subquery = table_metadata_snapshot->getSelectQuery().inner_query->clone();
                const auto alias = table_expression.database_and_table_name->tryGetAlias();
                table_expression.database_and_table_name = {};
                table_expression.subquery = std::make_shared<ASTSubquery>();
                table_expression.subquery->children.push_back(subquery);
                table_expression.subquery->as<ASTSubquery &>().database_of_view = database_name;
                table_expression.subquery->as<ASTSubquery &>().cte_name = table_name;
                if (!alias.empty())
                    table_expression.subquery->setAlias(alias);

                table_expression.children.clear();
                table_expression.children.push_back(table_expression.subquery);
            }
        }
    }
};

using ReplaceViewWithSubqueryMatcher = OneTypeMatcher<ReplaceViewWithSubquery>;
using ReplaceViewWithSubqueryVisitor = InDepthNodeVisitor<ReplaceViewWithSubqueryMatcher, true>;

}
