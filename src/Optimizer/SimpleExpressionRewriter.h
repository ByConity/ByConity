#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTVisitor.h>

namespace DB
{
/**
 * Rewrite AST in place, reduce the overhead of clone.
 */
template <typename C>
class SimpleExpressionRewriter : public ASTVisitor<ASTPtr, C>
{
public:
    ASTPtr visitNode(ASTPtr & node, C & context) override
    {
        for (auto & child : node->children)
        {
            auto result = ASTVisitorUtil::accept(child, *this, context);
            if (result != child)
                child = std::move(result);
        }
        return node;
    }
};
}
