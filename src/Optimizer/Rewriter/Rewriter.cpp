#include <Optimizer/Rewriter/Rewriter.h>
#include <QueryPlan/GraphvizPrinter.h>

namespace DB
{
void Rewriter::rewritePlan(QueryPlan & plan, ContextMutablePtr context) const
{
    Stopwatch watch{CLOCK_THREAD_CPUTIME_ID};
    watch.restart();

    bool rewritten = false;
    if (isEnabled(context))
    {
        rewritten = rewrite(plan, context);
    }

    double duration = watch.elapsedMillisecondsAsDouble();

    context->logOptimizerProfile(
        getLogger("PlanOptimizer"), "Optimizer rule run time: ", name(), std::to_string(duration) + "ms", true);

    if (duration >= context->getSettingsRef().plan_optimizer_rule_warning_time)
        LOG_WARNING(
            getLogger("PlanOptimizer"),
            "the execute time of " + name() + " rewriter " + std::to_string(duration)
                + " ms greater than or equal to " + std::to_string(context->getSettingsRef().plan_optimizer_rule_warning_time) + " ms");

    if (context->getSettingsRef().print_graphviz)
    {
        GraphvizPrinter::printLogicalPlan(
            plan, context, std::to_string(context->getRuleId()) + "_" + name() + "_" + std::to_string(duration) + "ms");
    }

    if (rewritten)
    {
        for (const auto & after_rule : after_rules)
        {
            context->incRuleId();
            after_rule->rewritePlan(plan, context);
        }
    }
}

}
