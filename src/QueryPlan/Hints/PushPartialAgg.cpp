#include <Parsers/IAST.h>
#include <QueryPlan/Hints/PlanHintFactory.h>
#include <QueryPlan/Hints/PushPartialAgg.h>
#include <QueryPlan/PlanNode.h>


namespace DB
{

bool PushPartialAgg::canAttach(PlanNodeBase & node, HintOptions & hint_options) const
{
    if (node.getStep()->getType() != IQueryPlanStep::Type::Aggregating)
        return false;

    for (auto & func_name : hint_options.func_names)
    {
        if (func_name == options[0])
            return true;
    }
    return false;
}

void registerHintPushPartialAgg(PlanHintFactory & factory)
{
    factory.registerPlanHint(EnablePushPartialAgg::name, &EnablePushPartialAgg::create, PlanHintFactory::CaseInsensitive);
    factory.registerPlanHint(DisablePushPartialAgg::name, &DisablePushPartialAgg::create, PlanHintFactory::CaseInsensitive);
}

}
