#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>
//#include <NavigationFunctions/NavigationFunctionFactory.h>
#include <Optimizer/ExpressionDeterminism.h>
#include <Optimizer/PredicateUtils.h>

namespace DB
{
std::set<String> ExpressionDeterminism::getDeterministicSymbols(Assignments & assignments, ContextMutablePtr & context)
{
    std::set<String> deterministic_symbols;
    for (auto & assignment : assignments)
    {
        if (ExpressionDeterminism::isDeterministic(assignment.second, context))
        {
            deterministic_symbols.emplace(assignment.first);
        }
    }
    return deterministic_symbols;
}

ConstASTPtr ExpressionDeterminism::filterDeterministicConjuncts(ConstASTPtr predicate, ContextMutablePtr & context)
{
    if (predicate == PredicateConst::TRUE_VALUE || predicate == PredicateConst::FALSE_VALUE)
    {
        return predicate;
    }
    std::vector<ConstASTPtr> predicates = PredicateUtils::extractConjuncts(predicate);
    std::vector<ConstASTPtr> deterministic;
    for (auto & pre : predicates)
    {
        if (ExpressionDeterminism::isDeterministic(pre, context))
        {
            deterministic.emplace_back(pre);
        }
    }
    return PredicateUtils::combineConjuncts(deterministic);
}

ConstASTPtr ExpressionDeterminism::filterNonDeterministicConjuncts(ConstASTPtr predicate, ContextMutablePtr & context)
{
    std::vector<ConstASTPtr> predicates = PredicateUtils::extractConjuncts(predicate);
    std::vector<ConstASTPtr> non_deterministic;
    for (auto & pre : predicates)
    {
        if (!ExpressionDeterminism::isDeterministic(pre, context))
        {
            non_deterministic.emplace_back(pre);
        }
    }
    return PredicateUtils::combineConjuncts(non_deterministic);
}

std::set<ConstASTPtr> ExpressionDeterminism::filterDeterministicPredicates(std::vector<ConstASTPtr> & predicates, ContextMutablePtr & context)
{
    std::set<ConstASTPtr> deterministic;
    for (auto & predicate : predicates)
    {
        if (isDeterministic(predicate, context))
        {
            deterministic.emplace(predicate);
        }
    }
    return deterministic;
}

bool ExpressionDeterminism::isDeterministic(ConstASTPtr expression, ContextMutablePtr & context)
{
    bool is_deterministic = true;
    DeterminismVisitor visitor{is_deterministic};
    ASTVisitorUtil::accept(expression, visitor, context);
    return visitor.isDeterministic();
}

DeterminismVisitor::DeterminismVisitor(bool isDeterministic) : is_deterministic(isDeterministic)
{
}

Void DeterminismVisitor::visitNode(const ConstASTPtr & node, ContextMutablePtr & context)
{
    for (ConstASTPtr child : node->children)
    {
        ASTVisitorUtil::accept(child, *this, context);
    }
    return Void{};
}

Void DeterminismVisitor::visitASTFunction(const ConstASTPtr & node, ContextMutablePtr & context)
{
    visitNode(node, context);
    auto & fun = node->as<const ASTFunction &>();
    if (AggregateFunctionFactory::instance().isAggregateFunctionName(fun.name))
    {
        return Void{};
    }
//    if (NavigationFunctionFactory::instance().isNavigationFunctionName(fun.name))
//    {
//        return Void{};
//    }
    if (!context->isFunctionDeterministic(fun.name))
    {
        is_deterministic = false;
    }
    return Void{};
}

}
