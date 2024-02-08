#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

struct RewriteFusionMerge
{
    using TypeToVisit = ASTTableExpression;

    ContextPtr context;
    explicit RewriteFusionMerge(ContextPtr context_) : context(std::move(context_))
    {
    }

    void visit(ASTTableExpression & table_expr, ASTPtr & ast);
};


using RewriteFusionMergeMatcher = OneTypeMatcher<RewriteFusionMerge>;
using RewriteFusionMergeVisitor = InDepthNodeVisitor<RewriteFusionMergeMatcher, true>;

}
