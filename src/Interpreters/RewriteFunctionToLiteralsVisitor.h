#pragma once

#include <Parsers/ASTFunction.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/evaluateConstantExpression.h>

namespace DB
{

/// Rewrites functions to literal if possible, e.g. tuple(1,3) --> (1,3), array(1,2,3) --> [1,2,3], 1 > 0 -> 0
class RewriteFunctionToLiteralsMatcher
{
public:
    struct Data : public WithContext
    {
        explicit Data(ContextPtr context_) : WithContext(context_) {}
    };

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

using RewriteFunctionToLiteralsVisitor = InDepthNodeVisitor<RewriteFunctionToLiteralsMatcher, false>;

}
