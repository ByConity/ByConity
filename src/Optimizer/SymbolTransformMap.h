#pragma once

#include <Parsers/IAST_fwd.h>
#include <QueryPlan/PlanNode.h>

#include <unordered_map>
#include <optional>

namespace DB
{
/**
 * Used to determines the origin of identifier in expression.
 */
class SymbolTransformMap
{
public:
    static std::optional<SymbolTransformMap> buildFrom(PlanNodeBase & plan);
//    SymbolTransformMap(const SymbolTransformMap &) = default;
//    SymbolTransformMap & operator=(const SymbolTransformMap &) = default;
//    SymbolTransformMap(SymbolTransformMap &&) = default;
//    SymbolTransformMap & operator=(SymbolTransformMap &&) = default;

    ASTPtr inlineReferences(const ConstASTPtr & expression) const;
private:
    SymbolTransformMap(
        std::unordered_map<String, ConstASTPtr> symbol_to_expressions_,
        std::unordered_map<String, ConstASTPtr> symbol_to_cast_lossless_expressions_)
        : symbol_to_expressions(std::move(symbol_to_expressions_))
        , symbol_to_cast_lossless_expressions(std::move(symbol_to_cast_lossless_expressions_))
    {
    }

    std::unordered_map<String, ConstASTPtr> symbol_to_expressions;
    std::unordered_map<String, ConstASTPtr> symbol_to_cast_lossless_expressions;

    mutable std::unordered_map<String, ConstASTPtr> expression_lineage;

    class Visitor;
    class Rewriter;
};
}
