#include <QueryPlan/Hints/JoinAlgorithmHints.h>
#include <QueryPlan/Hints/PlanHintFactory.h>
#include <QueryPlan/PlanNode.h>


namespace DB
{

JoinAlgorithmHints::JoinAlgorithmHints(const Strings & options_) : options(options_)
{
}

HintCategory JoinAlgorithmHints::getType() const
{
    return HintCategory::JOIN_ALGORITHM;
}

Strings JoinAlgorithmHints::getOptions() const
{
    return options;
}

bool JoinAlgorithmHints::checkOptions(Strings & table_name_list) const
{
    if (table_name_list.size() != 1)
        return false;

    return std::find(options.begin(), options.end(), table_name_list[0]) != options.end();
}

bool JoinAlgorithmHints::canAttach(PlanNodeBase & node, HintOptions & hint_options) const
{
    if (node.getStep()->getType() != IQueryPlanStep::Type::TableScan)
        return false;

    return checkOptions(hint_options.table_name_list);
}

}
