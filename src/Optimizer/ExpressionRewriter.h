#pragma once

#include <Analyzers/ASTEquals.h>
#include <Optimizer/Utils.h>
#include <Optimizer/SimpleExpressionRewriter.h>
#include <Parsers/ASTVisitor.h>
#include <Parsers/IAST_fwd.h>

#include <unordered_map>

namespace DB
{
using ConstASTMap = ASTMap<ConstASTPtr, ConstASTPtr>;

class ExpressionRewriter
{
public:
    static ASTPtr rewrite(const ConstASTPtr & expression, ConstASTMap & expression_map);
};

class ExpressionRewriterVisitor : public SimpleExpressionRewriter<ConstASTMap>
{
public:
    ASTPtr visitNode(ASTPtr & node, ConstASTMap & expression_map) override;
};
}
