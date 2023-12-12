#include <QueryPlan/Hints/SwapJoinOrder.h>
#include <QueryPlan/Hints/PlanHintFactory.h>
#include <Parsers/IAST.h>
#include <QueryPlan/PlanNode.h>


namespace DB
{

bool SwapJoinOrder::checkOptions(Strings & table_name_list) const
{
    if (table_name_list.empty() || table_name_list.size() < table_list.size())
        return false;

    bool contain_all_names = false;
    for (const String & table_name : table_list)
    {
        contain_all_names = false;
        for (auto & table : table_name_list)
        {
            if (table_name == table)
                contain_all_names = true;
        }
        if (!contain_all_names)
            return false;
    }

    return contain_all_names;
}

bool SwapJoinOrder::canAttach(PlanNodeBase & node, HintOptions & hint_options) const
{
    auto & join_table_list = hint_options.table_name_list;
    if (node.getStep()->getType() != IQueryPlanStep::Type::Join)
        return false;

    if (!checkOptions(join_table_list))
        return false;

    return true;
}

String SwapJoinOrder::getJoinOrderString() const
{
    String res = ")";
    bool is_first = true;
    for (const auto & elemt : table_list)
    {
        if (!is_first)
            res += ",";
        res += elemt;
        is_first = false;
    }
        
    res += ")";
    return res;
}


void registerHintSwapJoinOrder(PlanHintFactory & factory)
{
    factory.registerPlanHint(SwapJoinOrder::name, &SwapJoinOrder::create, PlanHintFactory::CaseInsensitive);
}

}
