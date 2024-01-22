#pragma once
#include <Optimizer/Rule/Rule.h>
#include <QueryPlan/JoinStep.h>

namespace DB
{

using GroupIdToIds = std::unordered_map<GroupId, std::set<GroupId>>;

namespace JoinReorderUtils
{
    PlanNodePtr createNewJoin(GroupId left_id, GroupId right_id, const GroupIdToIds & group_id_map, const Graph & graph, RuleContext & rule_context);
    double computeFilterSelectivity(GroupId child, const Memo & memo);
    void pruneJoinColumns(std::vector<String> & expected_output_symbols, PlanNodePtr & join_node, ContextMutablePtr & context_ptr);
}

}
