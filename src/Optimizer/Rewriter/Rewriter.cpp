#pragma once

#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/GraphvizPrinter.h>

namespace DB
{

void Rewriter::rewritePlan(QueryPlan & plan, ContextMutablePtr context) const
{
    Stopwatch watch;
    watch.restart();

    if (isEnabled(context))
    {
        rewrite(plan, context);
    }

    double duration = watch.elapsedMillisecondsAsDouble();

    context->logOptimizerProfile(
        &Poco::Logger::get("PlanOptimizer"), "Optimizer rule run time: ", name(), std::to_string(duration) + "ms", true);

    if (duration >= 1000)
        LOG_WARNING(&Poco::Logger::get("PlanOptimizer"), "the execute time of " + name() + " rewriter greater than or equal to 1 second");

    if (context->getSettingsRef().print_graphviz)
    {
        GraphvizPrinter::printLogicalPlan(
            plan, context, std::to_string(context->getRuleId()) + "_" + name() + "_" + std::to_string(duration) + "ms");
    }
}

}
