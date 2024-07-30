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

#include <Optimizer/Rule/Transformation/PushJoinThroughUnion.h>

#include <Core/Names.h>
#include <Optimizer/Rule/Patterns.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/PlanSymbolReallocator.h>
#include <QueryPlan/SymbolAllocator.h>
#include <QueryPlan/SymbolMapper.h>
#include <QueryPlan/UnionStep.h>

namespace DB
{
ConstRefPatternPtr PushJoinThroughUnion::getPattern() const
{
    static auto pattern = Patterns::join().with(Patterns::unionn(), Patterns::any()).result();
    return pattern;
}

const std::vector<RuleType> & PushJoinThroughUnion::blockRules() const
{
    static std::vector<RuleType> block{RuleType::JOIN_ENUM_ON_GRAPH, RuleType::PUSH_JOIN_THROUGH_UNION};
    return block;
}

TransformResult PushJoinThroughUnion::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    const auto & join = dynamic_cast<const JoinStep &>(*node->getStep());
    auto union_node = node->getChildren()[0];
    const auto & unionn = dynamic_cast<const UnionStep &>(*union_node->getStep());
    auto & context = rule_context.context;

    DataStreams input_streams;
    std::unordered_map<String, std::vector<String>> new_output_to_inputs;
    PlanNodes new_union_children;

    for (size_t i = 0; i < union_node->getChildren().size(); i++)
    {
        // reallocate
        auto plan_node_and_mappings = PlanSymbolReallocator::reallocate(node->getChildren()[1], context);
        
        // copy join
        SymbolMapper symbol_mapper{[&, i](const std::string & symbol) {
            if (unionn.getOutToInputs().contains(symbol))
                return unionn.getOutToInputs().at(symbol)[i];
            if (plan_node_and_mappings.mappings.contains(symbol))
                return plan_node_and_mappings.mappings.at(symbol);
            return symbol;
        }};

        auto new_join_step = symbol_mapper.map(join);

        // build union
        const auto & outputs = join.getOutputStream().header;
        input_streams.emplace_back();
        for (size_t j = 0; j < outputs.columns(); j++)
        {
            const auto & name_and_type = new_join_step->getOutputStream().header.getByPosition(j);
            new_output_to_inputs[outputs.getByPosition(j).name].emplace_back(name_and_type.name);
            input_streams.back().header.insert(name_and_type);
        }
        new_union_children.emplace_back(PlanNodeBase::createPlanNode(
            context->nextNodeId(), std::move(new_join_step), PlanNodes{union_node->getChildren()[i], plan_node_and_mappings.plan_node}));
    }

    return {PlanNodeBase::createPlanNode(
        context->nextNodeId(),
        std::make_shared<UnionStep>(
            std::move(input_streams), join.getOutputStream(), OutputToInputs{}, unionn.getMaxThreads(), unionn.isLocal()),
        new_union_children)};
}

}
