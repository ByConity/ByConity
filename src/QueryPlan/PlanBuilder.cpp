#include <QueryPlan/PlanBuilder.h>
#include <QueryPlan/planning_common.h>
#include <QueryPlan/ProjectionStep.h>

namespace DB
{
void PlanBuilder::addStep(QueryPlanStepPtr step, PlanNodes children)
{
    plan = plan->addStep(id_allocator->nextId(), std::move(step), std::move(children));
}

Names PlanBuilder::translateToSymbols(ASTs & expressions) const
{
    Names symbols;
    symbols.reserve(expressions.size());
    for (auto & expr : expressions)
        symbols.push_back(translateToSymbol(expr));
    return symbols;
}

Names PlanBuilder::translateToUniqueSymbols(ASTs & expressions) const
{
    Names symbols;
    NameSet exists;
    for (auto & expr : expressions)
    {
        auto symbol = translateToSymbol(expr);
        if (exists.emplace(symbol).second)
            symbols.push_back(symbol);
    }
    return symbols;
}

void PlanBuilder::appendProjection(ASTs & expressions)
{
    Assignments assignments;
    NameToType types;
    putIdentities(getOutputNamesAndTypes(), assignments, types);
    bool has_new_projection = false;
    AstToSymbol expression_to_symbols = createScopeAwaredASTMap<String>(analysis);

    for (auto & expr : expressions)
    {
        if (expression_to_symbols.find(expr) == expression_to_symbols.end() && !canTranslateToSymbol(expr))
        {
            String symbol = symbol_allocator->newSymbol(expr);
            assignments.emplace_back(symbol, translate(expr));
            types[symbol] = analysis.getExpressionType(expr);
            expression_to_symbols[expr] = symbol;
            has_new_projection = true;
        }
    }

    if (has_new_projection)
    {
        auto project = std::make_shared<ProjectionStep>(getCurrentDataStream(), assignments, types);
        addStep(std::move(project));
        withAdditionalMappings(expression_to_symbols);
    }
}
}
