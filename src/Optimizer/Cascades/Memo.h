/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <Optimizer/Cascades/Group.h>
#include <Optimizer/Cascades/GroupExpression.h>
#include <Common/Exception.h>

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

    void recordCTEDefGroupId(size_t cte_id, GroupId group_id) { cte_group_id_map[cte_id] = group_id; }

    const std::unordered_map<GroupExprPtr, GroupId, GroupExprPtrHash, GroupExprPtrEq> & getExprs() const { return group_expressions; }

    UInt32 nextJoinRootId() { return ++join_root_index; }
    void setJoinRootId(GroupId source_id, UInt32 root_id) 
    {
        source_to_join_root[source_id].set(root_id, true);
    }

    const auto & getSourceToJoinRoot() { return source_to_join_root; }
    static const UInt32 MAX_JOIN_ROOT_ID = 64;
private:
    GroupId addNewGroup()
    {
        auto new_group_id = groups.size();
        groups.emplace_back(std::make_shared<Group>(new_group_id));
        if (groups.size() > 10000)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Memo has so many group");
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
    std::unordered_map<GroupExprPtr, GroupId, GroupExprPtrHash, GroupExprPtrEq> group_expressions;
    std::unordered_map<GroupId, std::bitset<MAX_JOIN_ROOT_ID>> source_to_join_root;
    UInt32 join_root_index = 0;
};
}
