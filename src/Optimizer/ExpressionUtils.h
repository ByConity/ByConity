#pragma once

#include <Optimizer/SimpleExpressionRewriter.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTVisitor.h>
#include <Parsers/IAST_fwd.h>

#include <unordered_map>

namespace DB
{
class FunctionExtractorVisitor : public ConstASTVisitor<Void, std::set<String>>
{
public:
    Void visitNode(const ConstASTPtr &, std::set<String> & context) override;
    Void visitASTFunction(const ConstASTPtr &, std::set<String> & context) override;
};

class FunctionExtractor
{
public:
    static std::set<String> extract(ConstASTPtr node)
    {
        FunctionExtractorVisitor visitor;
        std::set<String> context;
        ASTVisitorUtil::accept(node, visitor, context);
        return context;
    }
};

}
