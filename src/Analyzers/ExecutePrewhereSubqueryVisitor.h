#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSubquery.h>

namespace DB
{

struct ExecutePrewhereSubquery
{
    using TypeToVisit = ASTSubquery;

    ContextMutablePtr context;
    explicit ExecutePrewhereSubquery(ContextMutablePtr context_) : context(std::move(context_)) {}

    void visit(ASTSubquery & subquery, ASTPtr & ast) const;
};

using ExecutePrewhereSubqueryMatcher = OneTypeMatcher<ExecutePrewhereSubquery>;
using ExecutePrewhereSubqueryVisitor = InDepthNodeVisitor<ExecutePrewhereSubqueryMatcher, true>;

}
