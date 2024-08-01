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
#include <Functions/FunctionsHashing.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Property/Equivalences.h>
#include <Optimizer/Rule/Rule.h>
#include <QueryPlan/JoinStep.h>
#include <boost/dynamic_bitset.hpp>

#include <unordered_set>
#include <utility>

namespace DB
{
using GroupId = UInt32;

class JoinEnumOnGraph : public Rule
{
public:
    explicit JoinEnumOnGraph(bool support_filter_) : support_filter(support_filter_) { }
    RuleType getType() const override { return RuleType::JOIN_ENUM_ON_GRAPH; }
    String getName() const override { return "JOIN_ENUM_ON_GRAPH"; }
    bool isEnabled(ContextPtr context) const override { return context->getSettingsRef().enable_join_enum_on_graph; }
    ConstRefPatternPtr getPattern() const override;

    const std::vector<RuleType> & blockRules() const override;
    static ConstASTs getJoinFilter(
        const ASTPtr & all_filter, std::set<String> & left_symbols, std::set<String> & right_symbols, ContextMutablePtr & context);

private:
    TransformResult transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & context) override;
    bool support_filter;
};

class JoinSet
{
public:
    JoinSet(const JoinSet & left_, const JoinSet & right_, const Names & left_join_keys, const Names & right_join_keys, ASTPtr filter_)
        : union_find(left_.union_find, right_.union_find), filter(std::move(filter_))
    {
        groups.insert(groups.end(), left_.groups.begin(), left_.groups.end());
        groups.insert(groups.end(), right_.groups.begin(), right_.groups.end());
        std::sort(groups.begin(), groups.end());

        for (size_t index = 0; index < left_join_keys.size(); ++index)
        {
            union_find.add(left_join_keys[index], right_join_keys[index]);
        }
    }

    explicit JoinSet(GroupId group_id_) { groups.emplace_back(group_id_); }

    const std::vector<GroupId> & getGroups() const { return groups; }
    bool operator<(const JoinSet & rhs) const { return groups < rhs.groups; }
    bool operator==(const JoinSet & rhs) const { return groups == rhs.groups; }
    bool operator!=(const JoinSet & rhs) const { return !(rhs == *this); }

    const UnionFind<String> & getUnionFind() const { return union_find; }

    const ASTPtr & getFilter() const { return filter; }

private:
    std::vector<GroupId> groups;
    UnionFind<String> union_find;
    ASTPtr filter;
};

struct JoinSetHash
{
    std::size_t operator()(JoinSet const & join_set) const
    {
        size_t hash = IntHash64Impl::apply(join_set.getGroups().size());
        for (auto child_group : join_set.getGroups())
        {
            hash = MurmurHash3Impl64::combineHashes(hash, child_group);
        }
        return hash;
    }
};

using JoinSets = std::set<JoinSet>;

}
