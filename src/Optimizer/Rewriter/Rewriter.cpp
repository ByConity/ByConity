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

    if (duration >= context->getSettingsRef().plan_optimizer_rule_warning_time)
        LOG_WARNING(
            &Poco::Logger::get("PlanOptimizer"),
            "the execute time of " + name() + " rewriter " + std::to_string(duration)
                + " ms greater than or equal to " + std::to_string(context->getSettingsRef().plan_optimizer_rule_warning_time) + " ms");

    if (context->getSettingsRef().print_graphviz)
    {
        GraphvizPrinter::printLogicalPlan(
            plan, context, std::to_string(context->getRuleId()) + "_" + name() + "_" + std::to_string(duration) + "ms");
    }
}

}
