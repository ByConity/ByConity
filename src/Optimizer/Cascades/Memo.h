#pragma once

#include <Optimizer/Cascades/Group.h>
#include <Optimizer/Cascades/GroupExpression.h>

#include <vector>

namespace DB
{
class Memo
{
public:
    GroupExprPtr insertGroupExpr(GroupExprPtr group_expr, CascadesContext & context, GroupId target = UNDEFINED_GROUP);

    GroupPtr getGroupById(GroupId id) const { return groups[id]; }

    const std::vector<GroupPtr> & getGroups() const { return groups; }

    bool containsCTEId(size_t id) const { return cte_group_id_map.contains(id); }

    GroupId getCTEDefGroupId(size_t id) const { return cte_group_id_map.at(id); }

    GroupPtr getCTEDefGroupByCTEId(size_t id) const { return getGroupById(getCTEDefGroupId(id)); }

    void recordCTEDefGroupId(size_t cte_id, GroupId group_id) { cte_group_id_map[cte_id] = group_id;}

private:
    GroupId addNewGroup()
    {
        auto new_group_id = groups.size();
        groups.emplace_back(std::make_shared<Group>(new_group_id));
        return new_group_id;
    }
    /**
     * Vector of groups tracked
     */
    std::vector<GroupPtr> groups;

    /**
     * Map of cte id to group id
     */
    std::unordered_map<size_t, GroupId> cte_group_id_map;

    /**
     * Vector of tracked GroupExpressions
     * Group owns GroupExpressions, not the memo
     */
    std::unordered_set<GroupExprPtr, GroupExprPtrHash, GroupExprPtrEq> group_expressions;
};
}
