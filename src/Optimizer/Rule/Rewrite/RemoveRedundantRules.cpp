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

#include <memory>
#include <Optimizer/Rule/Rewrite/RemoveRedundantRules.h>

#include <DataTypes/DataTypeNullable.h>
#include <Optimizer/ExpressionInterpreter.h>
#include <Optimizer/PlanNodeCardinality.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rule/Pattern.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/ApplyStep.h>
#include <QueryPlan/CTERefStep.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/ReadNothingStep.h>
#include <QueryPlan/UnionStep.h>
#include "Parsers/ASTFunction.h"
#include "Parsers/IAST_fwd.h"
#include "QueryPlan/CTERefStep.h"
#include "QueryPlan/IQueryPlanStep.h"
#include "QueryPlan/PlanNode.h"

namespace DB
{
TransformResult RemoveRedundantFilter::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    const auto * step = dynamic_cast<const FilterStep *>(node->getStep().get());
    auto expr = step->getFilter();

    if (const auto * literal = expr->as<ASTLiteral>())
    {
        const auto & input_columns = step->getInputStreams()[0].header;
        auto result = ExpressionInterpreter::evaluateConstantExpression(expr, input_columns.getNamesToTypes(), context);
        if (result.has_value() && result->second.isNull())
        {
            auto null_step = std::make_unique<ReadNothingStep>(step->getOutputStream().header);
            auto null_node = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(null_step));
            return {null_node};
        }

        UInt64 value;
        if (literal->value.tryGet(value) && value == 0)
        {
            auto null_step = std::make_unique<ReadNothingStep>(step->getOutputStream().header);
            auto null_node = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(null_step));
            return {null_node};
        }
        if (literal->value.tryGet(value) && value == 1)
        {
            return node->getChildren()[0];
        }
    }

    return {};
}

TransformResult RemoveRedundantUnion::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    const auto * step = dynamic_cast<const UnionStep *>(node->getStep().get());

    DataStreams inputs;
    PlanNodes children;
    OutputToInputs output_to_inputs;
    int index = 0;
    for (auto & child : node->getChildren())
    {
        if (!dynamic_cast<const ReadNothingStep *>(child->getStep().get()))
        {
            inputs.emplace_back(child->getStep()->getOutputStream());
            children.emplace_back(child);
            for (const auto & output_to_input : step->getOutToInputs())
                output_to_inputs[output_to_input.first].push_back(output_to_input.second[index]);
        }
        ++index;
    }

    if (children.empty())
    {
        auto null_step = std::make_unique<ReadNothingStep>(step->getOutputStream().header);
        return PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(null_step));
    }

    // local union equals to local gather (make thread single), can't rewrite to projection.
    if (children.size() == 1 && !step->isLocal())
    {
        auto input_columns = children[0]->getStep()->getOutputStream().header;
        Assignments assignments;
        NameToType name_to_type;
        for (const auto & output_to_input : step->getOutToInputs())
        {
            String output = output_to_input.first;
            for (const auto & input : output_to_input.second)
            {
                for (auto & input_column : input_columns)
                {
                    if (input == input_column.name)
                    {
                        Assignment column{output, std::make_shared<ASTIdentifier>(input_column.name)};
                        assignments.emplace_back(column);
                        name_to_type[output] = input_column.type;
                    }
                }
            }
        }
        auto project_step = std::make_shared<ProjectionStep>(children[0]->getStep()->getOutputStream(), assignments, name_to_type);
        return PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(project_step), children, node->getStatistics());
    }

    if (children.size() != node->getChildren().size())
    {
        auto union_step
            = std::make_unique<UnionStep>(inputs, step->getOutputStream(), output_to_inputs, step->getMaxThreads(), step->isLocal());
        return PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(union_step), children, node->getStatistics());
    }

    return {};
}

TransformResult RemoveRedundantProjection::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto * projection_node = dynamic_cast<ProjectionNode *>(node.get());
    if (!projection_node)
        return {};
    const auto & step = *projection_node->getStep();

    if (Utils::isIdentity(step))
    {
        const DataStream & output_stream = step.getOutputStream();
        const DataStream & source_output_stream = node->getChildren()[0]->getStep()->getOutputStream();

        // remove duplicated columns
        std::unordered_set<std::string> output_symbols;
        for (const auto & column : output_stream.header)
        {
            output_symbols.emplace(column.name);
        }
        std::unordered_set<std::string> source_output_symbols;
        for (const auto & column : source_output_stream.header)
        {
            source_output_symbols.emplace(column.name);
        }
        if (output_symbols == source_output_symbols)
        {
            return node->getChildren()[0];
        }
    }

    if (node->getChildren()[0]->getStep()->getType() == IQueryPlanStep::Type::ReadNothing)
    {
        return PlanNodeBase::createPlanNode(
            context.context->nextNodeId(), std::make_shared<ReadNothingStep>(node->getStep()->getOutputStream().header));
    }
    return {};
}

TransformResult RemoveRedundantEnforceSingleRow::transformImpl(PlanNodePtr node, const Captures &, RuleContext &)
{
    if (PlanNodeCardinality::isScalar(*node->getChildren()[0]))
    {
        return node->getChildren()[0];
    }
    return {};
}

PatternPtr RemoveRedundantCrossJoin::getPattern() const
{
    return Patterns::join()
        .matchingStep<JoinStep>([&](const JoinStep & s) { return s.getKind() == ASTTableJoin::Kind::Cross; })
        .with(Patterns::any(), Patterns::any())
        .result();
}

TransformResult RemoveRedundantCrossJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext &)
{
    // normal case
    if (node->getChildren()[0]->getStep()->getOutputStream().header.columns() == 0
        && PlanNodeCardinality::isScalar(*node->getChildren()[0]))
    {
        return node->getChildren()[1];
    }
    if (node->getChildren()[1]->getStep()->getOutputStream().header.columns() == 0
        && PlanNodeCardinality::isScalar(*node->getChildren()[1]))
    {
        return node->getChildren()[0];
    }

    return {};
}

PatternPtr RemoveReadNothing::getPattern() const
{
    return Patterns::any().withSingle(Patterns::readNothing()).result();
}

TransformResult RemoveReadNothing::transformImpl(PlanNodePtr, const Captures &, RuleContext &)
{
    return {};
}

PatternPtr RemoveRedundantJoin::getPattern() const
{
    return Patterns::join()
        .matchingStep<JoinStep>(
            [&](const JoinStep & s) { return s.getKind() == ASTTableJoin::Kind::Inner || s.getKind() == ASTTableJoin::Kind::Cross; })
        .withAny(Patterns::readNothing())
        .result();
}

TransformResult RemoveRedundantJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    if (node->getChildren()[0]->getStep()->getType() == IQueryPlanStep::Type::ReadNothing
        || node->getChildren()[1]->getStep()->getType() == IQueryPlanStep::Type::ReadNothing)
    {
        return PlanNodeBase::createPlanNode(
            context.context->nextNodeId(), std::make_shared<ReadNothingStep>(node->getStep()->getOutputStream().header));
    }
    return {};
}

PatternPtr RemoveRedundantOuterJoin::getPattern() const
{
    return Patterns::join()
        .matchingStep<JoinStep>(
            [&](const JoinStep & s) { return s.getKind() == ASTTableJoin::Kind::Left || s.getKind() == ASTTableJoin::Kind::Right; })
        .result();
}

TransformResult RemoveRedundantOuterJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext &)
{
    auto * join_node = dynamic_cast<JoinNode *>(node.get());

    auto all_type_nothing = [](Block block, Names keys) {
        for (const auto & key : keys)
        {
            auto type = block.getByName(key).type;
            if (removeNullable(recursiveRemoveLowCardinality(type))->getTypeId() != TypeIndex::Nothing)
            {
                return false;
            }
        }
        return true;
    };

    if (join_node)
    {
        auto step = join_node->getStep();
        if (step->getKind() == ASTTableJoin::Kind::Left)
        {
            if (node->getChildren()[1]->getStep()->getType() == IQueryPlanStep::Type::ReadNothing
                || all_type_nothing(node->getChildren()[1]->getStep()->getOutputStream().header, step->getRightKeys()))
            {
                // todo add project to add joined columns
                return node->getChildren()[0];
            }
        }
        if (step->getKind() == ASTTableJoin::Kind::Right)
        {
            if (node->getChildren()[0]->getStep()->getType() == IQueryPlanStep::Type::ReadNothing
                || all_type_nothing(node->getChildren()[0]->getStep()->getOutputStream().header, step->getLeftKeys()))
            {
                // todo add project to add joined columns
                return node->getChildren()[1];
            }
        }
    }
    return {};
}

PatternPtr RemoveRedundantLimit::getPattern() const
{
    return Patterns::limit().result();
}

TransformResult RemoveRedundantLimit::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto * limit_node = dynamic_cast<LimitNode *>(node.get());
    if (!limit_node->getStep()->hasPreparedParam() && limit_node->getStep()->getLimitValue() == 0)
    {
        auto null_step = std::make_unique<ReadNothingStep>(limit_node->getStep()->getOutputStream().header);
        auto null_node = PlanNodeBase::createPlanNode(context.context->nextNodeId(), std::move(null_step));
        return {null_node};
    }

    return {};
}

PatternPtr RemoveRedundantAggregate::getPattern() const
{
    return Patterns::aggregating().result();
}

TransformResult RemoveRedundantAggregate::transformImpl(PlanNodePtr, const Captures &, RuleContext &)
{
    return {};
}

PatternPtr RemoveRedundantAggregateWithReadNothing::getPattern() const
{
    return Patterns::aggregating().matchingStep<AggregatingStep>([&](const AggregatingStep & s) { return !s.getKeys().empty(); })
        .withSingle(Patterns::readNothing()).result();
}

TransformResult RemoveRedundantAggregateWithReadNothing::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto * step = dynamic_cast<AggregatingStep *>(node->getStep().get());
    auto read_nothing_step = std::make_shared<ReadNothingStep>(step->getOutputStream().header);
    auto read_nothing_node = PlanNodeBase::createPlanNode(context.context->nextNodeId(), std::move(read_nothing_step), {});
    return {read_nothing_node};
}

PatternPtr RemoveRedundantTwoApply::getPattern() const
{
    return Patterns::filter()
        .withSingle(
            Patterns::apply()
                .matchingStep<ApplyStep>([](const ApplyStep & apply) { return apply.getSubqueryType() == ApplyStep::SubqueryType::IN; })
                .with(
                    Patterns::apply()
                        .matchingStep<ApplyStep>(
                            [](const ApplyStep & apply) { return apply.getSubqueryType() == ApplyStep::SubqueryType::IN; })
                        .with(Patterns::any(), Patterns::cte()),
                    Patterns::project().withSingle(Patterns::filter().withSingle(Patterns::join().with(Patterns::any(), Patterns::cte())))))
        .result();
}

TransformResult RemoveRedundantTwoApply::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    auto * filter = dynamic_cast<FilterStep *>(node->getStep().get());

    auto * first_apply = dynamic_cast<ApplyStep *>(node->getChildren()[0]->getStep().get());
    auto * first_cte = dynamic_cast<CTERefStep *>(
        node->getChildren()[0]->getChildren()[1]->getChildren()[0]->getChildren()[0]->getChildren()[1]->getStep().get());
    auto * second_apply = dynamic_cast<ApplyStep *>(node->getChildren()[0]->getChildren()[0]->getStep().get());
    auto * second_cte = dynamic_cast<CTERefStep *>(node->getChildren()[0]->getChildren()[0]->getChildren()[1]->getStep().get());

    if (first_cte->getId() != second_cte->getId())
        return {};


    auto conjuncts = PredicateUtils::extractConjuncts(filter->getFilter());
    bool match_first = false;
    bool match_second = false;
    for (const auto & conjunct : conjuncts)
    {
        if (const auto * id = conjunct->as<ASTIdentifier>())
        {
            if (id->getColumnName() == first_apply->getAssignment().first)
            {
                match_first = true;
            }
            if (id->getColumnName() == second_apply->getAssignment().first)
            {
                match_second = true;
            }
        }
    }

    if (match_first && match_second)
    {
        auto second_apply_left = node->getChildren()[0]->getChildren()[0]->getChildren()[0];
        auto new_apply = std::make_shared<ApplyStep>(
            DataStreams{second_apply_left->getCurrentDataStream(), first_apply->getInputStreams()[1]},
            first_apply->getCorrelation(),
            first_apply->getApplyType(),
            first_apply->getSubqueryType(),
            first_apply->getAssignment(),
            first_apply->getOuterColumns(),
            first_apply->supportSemiAnti());
        auto new_apply_node = PlanNodeBase::createPlanNode(
            context.context->nextNodeId(), new_apply, {second_apply_left, node->getChildren()[0]->getChildren()[1]});


        std::vector<ConstASTPtr> new_filter;
        for (const auto & conjunct : conjuncts)
        {
            if (const auto * id = conjunct->as<ASTIdentifier>())
            {
                if (id->getColumnName() == second_apply->getAssignment().first)
                {
                    continue;
                }
            }
            new_filter.emplace_back(conjunct);
        }

        ConstASTPtr filter_ast = PredicateUtils::combineConjuncts(new_filter);
        auto new_filter_step = std::make_shared<FilterStep>(new_apply_node->getCurrentDataStream(), filter_ast);
        return PlanNodeBase::createPlanNode(context.context->nextNodeId(), new_filter_step, {new_apply_node});
    }


    return {};
}

}
