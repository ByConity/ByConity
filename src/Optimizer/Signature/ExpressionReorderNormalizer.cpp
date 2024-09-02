#include <Optimizer/Signature/ExpressionReorderNormalizer.h>

#include <Analyzers/ASTEquals.h>
#include <Core/Names.h>
#include <Functions/FunctionsLogical.h>
#include <Interpreters/AggregateDescription.h>
#include <Parsers/ASTVisitor.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/Assignment.h>
#include <QueryPlan/Void.h>

#include <algorithm>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace DB
{

namespace
{
bool isCommutative(std::string_view function_name)
{
    return function_name == NameAnd::name || function_name == NameOr::name;
}
}

void ExpressionReorderNormalizer::reorder(Names & names)
{
    std::sort(names.begin(), names.end());
}

void ExpressionReorderNormalizer::reorder(NamesWithAliases & name_alias)
{
    std::sort(name_alias.begin(),
              name_alias.end(),
              [](const NameWithAlias & left, const NameWithAlias & right) { return left.first < right.first; });
}

void ExpressionReorderNormalizer::reorder(Assignments & assignments)
{
    std::vector<Assignment> res;
    std::unordered_map<std::string, size_t> assignment_hashes{};
    for (auto & [symbol, expr] : assignments)
    {
        ASTPtr expr_copy = expr->clone();
        size_t hash = reorder(expr_copy);
        assignment_hashes.emplace(symbol, hash);
        res.emplace_back(std::make_pair(symbol, expr_copy));
    }
    std::sort(res.begin(), res.end(),
              [&assignment_hashes](const Assignment & left, const Assignment & right) {
                  return assignment_hashes.at(left.first) < assignment_hashes.at(right.first);} );
    assignments = Assignments(res.begin(), res.end());
}

void ExpressionReorderNormalizer::reorder(AggregateDescriptions & descriptions)
{
    std::sort(descriptions.begin(),
              descriptions.end(),
              [](const AggregateDescription & left, const AggregateDescription & right) {
                  return std::forward_as_tuple(left.function->getName(), left.parameters, left.argument_names) <
                      std::forward_as_tuple(right.function->getName(), right.parameters, right.argument_names); });
}

size_t ExpressionReorderNormalizer::reorder(ASTPtr & ast)
{
    ExpressionReorderNormalizer normalizer;
    Void ctx{};
    return ASTVisitorUtil::accept(ast, normalizer, ctx);
}

void ExpressionReorderNormalizer::reorder(ColumnsWithTypeAndName & columnas)
{
    std::sort(columnas.begin(), columnas.end(), [](const auto & left, const auto & right) { return left.name < right.name; });
}

size_t ExpressionReorderNormalizer::visitNode(ASTPtr & ast, Void & ctx)
{
    for (auto & child : ast->getChildren())
        ASTVisitorUtil::accept(child, *this, ctx);
    return ASTEquality::hashTree(ast);
}

size_t ExpressionReorderNormalizer::visitASTFunction(ASTPtr & ast, Void & ctx)
{
    auto function = ast->as<ASTFunction &>();
    if (isCommutative(function.name))
    {
        std::unordered_map<ASTPtr, size_t> children_hashes{};
        auto & children = function.arguments->getChildren();
        for (auto & child : children)
        {
            size_t hash = ASTVisitorUtil::accept(child, *this, ctx);
            children_hashes.emplace(child, hash);
        }
        std::sort(children.begin(), children.end(), [&children_hashes](const ASTPtr & left, const ASTPtr & right) {
            return children_hashes.at(left) < children_hashes.at(right);} );
    }
    return ASTEquality::hashTree(ast);
}

}
