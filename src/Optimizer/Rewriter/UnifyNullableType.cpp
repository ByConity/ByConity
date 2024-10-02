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
#include <Optimizer/Rewriter/UnifyNullableType.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Analyzers/TypeAnalyzer.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/join_common.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/CTERefStep.h>
#include <QueryPlan/ExchangeStep.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/LimitStep.h>
#include <QueryPlan/MergeSortingStep.h>
#include <QueryPlan/MergingAggregatedStep.h>
#include <QueryPlan/PartialSortingStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/UnionStep.h>
#include <QueryPlan/WindowStep.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

bool UnifyNullableType::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    UnifyNullableVisitor visitor{plan.getCTEInfo(), plan.getPlanNode()};
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, context);
    plan.update(result);
    return true;
}

PlanNodePtr UnifyNullableVisitor::visitPlanNode(PlanNodeBase & node, ContextMutablePtr & context)
{
    PlanNodes new_children;
    DataStreams new_inputs;

    for (auto & i : node.getChildren())
    {
        PlanNodePtr rewritten_child = VisitorUtil::accept(*i, *this, context);
        new_children.emplace_back(rewritten_child);
        new_inputs.push_back(rewritten_child->getStep()->getOutputStream());
    }

    QueryPlanStepPtr step = node.getStep();
    step->setInputStreams(new_inputs);

    return PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(step), new_children, node.getStatistics());
}

PlanNodePtr UnifyNullableVisitor::visitArrayJoinNode(ArrayJoinNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitExceptNode(ExceptNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitIntersectNode(IntersectNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitExtremesNode(ExtremesNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitFillingNode(FillingNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitFinalSampleNode(FinalSampleNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitBufferNode(BufferNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitExplainAnalyzeNode(ExplainAnalyzeNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitTotalsHavingNode(TotalsHavingNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitOffsetNode(OffsetNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitRemoteExchangeSourceNode(RemoteExchangeSourceNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitSortingNode(SortingNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitTableWriteNode(TableWriteNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitOutfileWriteNode(OutfileWriteNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitApplyNode(ApplyNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitIntersectOrExceptNode(IntersectOrExceptNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitIntermediateResultCacheNode(IntermediateResultCacheNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitLocalExchangeNode(LocalExchangeNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitMultiJoinNode(MultiJoinNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitFilterNode(FilterNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitDistinctNode(DistinctNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitMarkDistinctNode(MarkDistinctNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitWindowNode(WindowNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitPartialSortingNode(PartialSortingNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitPartitionTopNNode(PartitionTopNNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitTopNFilteringNode(TopNFilteringNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitMergeSortingNode(MergeSortingNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitMergingSortedNode(MergingSortedNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitFinishSortingNode(FinishSortingNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitLimitNode(LimitNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitLimitByNode(LimitByNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitEnforceSingleRowNode(EnforceSingleRowNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitAssignUniqueIdNode(AssignUniqueIdNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitTableFinishNode(TableFinishNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitOutfileFinishNode(OutfileFinishNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitReadNothingNode(ReadNothingNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitReadStorageRowCountNode(ReadStorageRowCountNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitTableScanNode(TableScanNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitValuesNode(ValuesNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitExpandNode(ExpandNode & node, ContextMutablePtr & context)
{
    return visitPlanNode(node, context);
}

PlanNodePtr UnifyNullableVisitor::visitProjectionNode(ProjectionNode & node, ContextMutablePtr & context)
{
    PlanNodePtr child = VisitorUtil::accept(node.getChildren()[0], *this, context);
    const auto & step = *node.getStep();
    const auto & assignments = step.getAssignments();
    NameToType set_nullable;
    const auto & input_header = child->getStep()->getOutputStream().header;
    auto type_analyzer = TypeAnalyzer::create(context, input_header.getNamesAndTypes());
    for (auto & assignment : assignments)
    {
        String name = assignment.first;
        ConstASTPtr value = assignment.second;
        DataTypePtr type = type_analyzer.getType(value);
        set_nullable[name] = type;
    }

    auto expression_step = std::make_shared<ProjectionStep>(
        child->getStep()->getOutputStream(), assignments, set_nullable, step.isFinalProject(), step.isIndexProject());
    return ProjectionNode::createPlanNode(context->nextNodeId(), std::move(expression_step), PlanNodes{child}, node.getStatistics());
}

PlanNodePtr UnifyNullableVisitor::visitJoinNode(JoinNode & node, ContextMutablePtr & context)
{
    PlanNodes children;
    DataStreams inputs;
    for (const auto & item : node.getChildren())
    {
        PlanNodePtr child = VisitorUtil::accept(*item, *this, context);
        children.emplace_back(child);
        inputs.push_back(child->getStep()->getOutputStream());
    }

    const auto & join_step = *node.getStep();

    const DataStreams & input_stream = inputs;
    const DataStream & output_stream = join_step.getOutputStream();

    auto output_set_null = output_stream.header.getNamesAndTypes();
    std::unordered_map<String, DataTypePtr> left_name_to_type;
    std::unordered_map<String, DataTypePtr> right_name_to_type;
    for (const auto & left : input_stream[0].header)
    {
        left_name_to_type[left.name] = left.type;
    }
    for (const auto & right : input_stream[1].header)
    {
        right_name_to_type[right.name] = right.type;
    }

    bool make_nullable_for_left = isRightOrFull(join_step.getKind());
    bool make_nullable_for_right = isLeftOrFull(join_step.getKind());

    auto update_symbol_type = [&output_set_null](const std::unordered_map<String, DataTypePtr> & type_provider, bool make_nullable) {
        std::transform(
            output_set_null.begin(),
            output_set_null.end(),
            output_set_null.begin(),
            [&type_provider, &make_nullable](const NameAndTypePair & symbol) -> NameAndTypePair {
                if (!type_provider.contains(symbol.name))
                    return symbol;

                const auto & type = type_provider.at(symbol.name);
                if (make_nullable && JoinCommon::canBecomeNullable(type))
                    return {symbol.name, JoinCommon::convertTypeToNullable(type_provider.at(symbol.name))};
                else
                    return {symbol.name, type_provider.at(symbol.name)};
            });
    };

    update_symbol_type(left_name_to_type, make_nullable_for_left);
    update_symbol_type(right_name_to_type, make_nullable_for_right);

    DataStream output_stream_set_null = DataStream{.header = output_set_null};
    auto join_step_set_null = std::make_shared<JoinStep>(
        inputs,
        output_stream_set_null,
        join_step.getKind(),
        join_step.getStrictness(),
        join_step.getMaxStreams(),
        join_step.getKeepLeftReadInOrder(),
        join_step.getLeftKeys(),
        join_step.getRightKeys(),
        join_step.getKeyIdsNullSafe(),
        join_step.getFilter(),
        join_step.isHasUsing(),
        join_step.getRequireRightKeys(),
        join_step.getAsofInequality(),
        join_step.getDistributionType(),
        join_step.getJoinAlgorithm(),
        join_step.isMagic(),
        join_step.isOrdered(),
        join_step.isSimpleReordered(),
        join_step.getRuntimeFilterBuilders(),
        join_step.getHints());
    return JoinNode::createPlanNode(context->nextNodeId(), std::move(join_step_set_null), children, node.getStatistics());
}

PlanNodePtr UnifyNullableVisitor::visitAggregatingNode(AggregatingNode & node, ContextMutablePtr & context)
{
    PlanNodePtr child = VisitorUtil::accept(node.getChildren()[0], *this, context);

    const auto & step = *node.getStep();

    const AggregateDescriptions & descs = step.getAggregates();
    AggregateDescriptions descs_set_nullable;

    auto input_columns = child->getStep()->getOutputStream().header;
    for (const auto & desc : descs)
    {
        AggregateDescription desc_with_null;
        AggregateFunctionPtr fun = desc.function;
        Names argument_names = desc.argument_names;
        DataTypes types;
        for (auto & argument_name : argument_names)
        {
            for (auto & column : input_columns)
            {
                if (argument_name == column.name)
                {
                    types.emplace_back(recursiveRemoveLowCardinality(column.type));
                    break;
                }
            }
        }
        String fun_name = fun->getName();
        AggregateFunctionPtr fun_with_null = desc.function;
        // tmp fix: For AggregateFunctionNothing, the argument types may diff with
        // the ones in `descr.function->argument_types`. In this case, reconstructing aggregate description will lead
        // to a different result.
        //
        // see also similar fix in AggregatingStep.cpp
        if (fun_name != "nothing")
        {
            AggregateFunctionProperties properties;
            fun_with_null = AggregateFunctionFactory::instance().get(fun_name, types, desc.parameters, properties);
        }
        desc_with_null.function = fun_with_null;
        desc_with_null.parameters = desc.parameters;
        desc_with_null.column_name = desc.column_name;
        desc_with_null.argument_names = argument_names;
        desc_with_null.parameters = desc.parameters;
        desc_with_null.arguments = desc.arguments;
        desc_with_null.mask_column = desc.mask_column;

        descs_set_nullable.emplace_back(desc_with_null);
    }

    auto agg_step_set_null = std::make_shared<AggregatingStep>(
        child->getStep()->getOutputStream(),
        step.getKeys(),
        step.getKeysNotHashed(),
        descs_set_nullable,
        step.getGroupingSetsParams(),
        step.isFinal(),
        step.getGroupBySortDescription(),
        step.getGroupings(),
        step.needOverflowRow(),
        step.shouldProduceResultsInOrderOfBucketNumber(),
        step.isNoShuffle(),
        step.isStreamingForCache(),
        step.getHints());
    auto agg_node_set_null
        = AggregatingNode::createPlanNode(context->nextNodeId(), std::move(agg_step_set_null), PlanNodes{child}, node.getStatistics());

    return agg_node_set_null;
}

PlanNodePtr UnifyNullableVisitor::visitMergingAggregatedNode(MergingAggregatedNode & node, ContextMutablePtr & context)
{
    PlanNodePtr child = VisitorUtil::accept(node.getChildren()[0], *this, context);

    const auto & step = *node.getStep();

    const AggregateDescriptions & descs = step.getAggregates();
    AggregateDescriptions descs_set_nullable;

    auto input_columns = child->getStep()->getOutputStream().header;
    for (const auto & desc : descs)
    {
        AggregateDescription desc_with_null;
        AggregateFunctionPtr fun = desc.function;

        // get type from AggregateFunction(...);
        auto argument_types = [&] {
            auto argument_name = desc.column_name;
            auto column = input_columns.getByName(argument_name, false);
            auto partial_type = typeid_cast<std::shared_ptr<const DataTypeAggregateFunction>>(column.type);
            if (!partial_type)
                throw Exception("unexpected merge agg input type", ErrorCodes::LOGICAL_ERROR);

            return partial_type->getArgumentsDataTypes();
        }();

        String fun_name = fun->getName();
        AggregateFunctionPtr fun_with_null = desc.function;
        // tmp fix: For AggregateFunctionNothing, the argument types may diff with
        // the ones in `descr.function->argument_types`. In this case, reconstructing aggregate description will lead
        // to a different result.
        //
        // see also similar fix in AggregatingStep.cpp
        if (fun_name != "nothing")
        {
            AggregateFunctionProperties properties;
            fun_with_null = AggregateFunctionFactory::instance().get(fun_name, argument_types, desc.parameters, properties);
        }
        desc_with_null.function = fun_with_null;
        desc_with_null.parameters = desc.parameters;
        desc_with_null.column_name = desc.column_name;
        desc_with_null.argument_names = desc.argument_names;
        desc_with_null.parameters = desc.parameters;
        desc_with_null.arguments = desc.arguments;
        desc_with_null.mask_column = desc.mask_column;

        descs_set_nullable.emplace_back(desc_with_null);
    }

    auto trans_params = step.getAggregatingTransformParams();
    const auto & agg_params = trans_params->params;
    ColumnNumbers key_positions;
    auto rewritten_header = child->getStep()->getOutputStream();
    for (const auto & key : step.getKeys())
        key_positions.emplace_back(rewritten_header.header.getPositionByName(key));

    Aggregator::Params new_agg_params{
        rewritten_header.header, key_positions, std::move(descs_set_nullable), agg_params.overflow_row, agg_params.max_threads};

    auto new_trans_params = std::make_shared<AggregatingTransformParams>(new_agg_params, trans_params->final);

    auto merge_agg_step_set_null = std::make_shared<MergingAggregatedStep>(
        rewritten_header,
        step.getKeys(),
        step.getGroupingSetsParamsList(),
        step.getGroupings(),
        new_trans_params,
        step.isMemoryEfficientAggregation(),
        step.getMaxThreads(),
        step.getMemoryEfficientMergeThreads());

    auto merge_agg_node_set_null = MergingAggregatedNode::createPlanNode(
        context->nextNodeId(), std::move(merge_agg_step_set_null), PlanNodes{child}, node.getStatistics());

    return merge_agg_node_set_null;
}

PlanNodePtr UnifyNullableVisitor::visitUnionNode(UnionNode & node, ContextMutablePtr & context)
{
    const auto & step = *node.getStep();
    PlanNodes new_children;
    DataStreams new_inputs;
    NamesAndTypes new_output = step.getOutputStream().header.getNamesAndTypes();

    auto update_output_data_type = [&step](NamesAndTypes & output_symbols, const NamesAndTypes & new_input_symbols, int child_id) {
        for (auto & output : output_symbols)
        {
            const auto & input_name = step.getOutToInputs().at(output.name)[child_id];
            DataTypePtr input_type = nullptr;

            for (const auto & item : new_input_symbols)
            {
                if (item.name == input_name)
                {
                    input_type = item.type;
                    break;
                }
            }

            if (isNullableOrLowCardinalityNullable(input_type) && !isNullableOrLowCardinalityNullable(output.type))
            {
                output.type = JoinCommon::tryConvertTypeToNullable(output.type);
            }
        }
    };

    for (size_t i = 0; i < node.getChildren().size(); ++i)
    {
        PlanNodePtr rewritten_child = VisitorUtil::accept(*node.getChildren()[i], *this, context);
        new_children.emplace_back(rewritten_child);
        new_inputs.push_back(rewritten_child->getStep()->getOutputStream());
        update_output_data_type(new_output, rewritten_child->getStep()->getOutputStream().header.getNamesAndTypes(), i);
    }

    Block new_output_header;
    for (const auto & item : new_output)
    {
        new_output_header.insert(ColumnWithTypeAndName{item.type, item.name});
    }

    // add cast projection, make Union's input stream/output stream type Nullable consistent.
    PlanNodes children_add_nullable;
    for (size_t i = 0; i < new_children.size(); i++)
    {
        Assignments add_cast;
        NameToType name_to_type;

        bool need_add_cast_projection = false;
        for (auto const & value : step.getOutToInputs())
        {
            auto output_name = value.first;
            auto output_type = new_output_header.getByName(output_name).type;

            auto input_name = value.second[i];
            auto input_type = new_children[i]->getOutputNamesToTypes().at(input_name);

            if (isNullableOrLowCardinalityNullable(output_type) && !isNullableOrLowCardinalityNullable(input_type))
            {
                need_add_cast_projection = true;
                input_type = JoinCommon::tryConvertTypeToNullable(input_type);
                Assignment assignment{
                    input_name,
                    makeASTFunction(
                        "cast", std::make_shared<ASTIdentifier>(input_name), std::make_shared<ASTLiteral>(input_type->getName()))};
                add_cast.emplace_back(assignment);
                name_to_type[input_name] = input_type;
            }
            else
            {
                Assignment assignment{input_name, std::make_shared<ASTIdentifier>(input_name)};
                add_cast.emplace_back(assignment);
                name_to_type[input_name] = input_type;
            }
        }

        if (need_add_cast_projection)
        {
            auto add_cast_step = std::make_shared<ProjectionStep>(new_children[i]->getStep()->getOutputStream(), add_cast, name_to_type);
            auto add_cast_node
                = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(add_cast_step), PlanNodes{new_children[i]});
            children_add_nullable.emplace_back(add_cast_node);
        }
        else
        {
            children_add_nullable.emplace_back(new_children[i]);
        }
    }

    DataStreams new_inputs_add_cast;
    for (auto & i : children_add_nullable)
    {
        new_inputs_add_cast.push_back(i->getStep()->getOutputStream());
    }

    auto rewritten_step = std::make_unique<UnionStep>(
        new_inputs_add_cast, DataStream{new_output_header}, step.getOutToInputs(), step.getMaxThreads(), step.isLocal());
    auto rewritten_node
        = UnionNode::createPlanNode(context->nextNodeId(), std::move(rewritten_step), children_add_nullable, node.getStatistics());
    return rewritten_node;
}

PlanNodePtr UnifyNullableVisitor::visitExchangeNode(ExchangeNode & node, ContextMutablePtr & context)
{
    const auto & step = *node.getStep();

    PlanNodes children;
    DataStreams inputs;
    for (auto & item : node.getChildren())
    {
        PlanNodePtr child = VisitorUtil::accept(*item, *this, context);
        children.emplace_back(child);
        inputs.emplace_back(child->getStep()->getOutputStream());
    }

    // update it's input/output stream types.
    auto exchange_step_set_null = std::make_unique<ExchangeStep>(inputs, step.getExchangeMode(), step.getSchema(), step.needKeepOrder());
    auto exchange_node_set_null
        = ExchangeNode::createPlanNode(context->nextNodeId(), std::move(exchange_step_set_null), children, node.getStatistics());
    return exchange_node_set_null;
}

PlanNodePtr UnifyNullableVisitor::visitCTERefNode(CTERefNode & node, ContextMutablePtr & context)
{
    auto cte_step = node.getStep();
    auto cte_id = cte_step->getId();
    auto cte_plan = cte_helper.acceptAndUpdate(cte_id, *this, context);

    const auto & cte_output_stream = cte_plan->getStep()->getOutputStream().header;

    DataStream output_stream;
    for (const auto & output : cte_step->getOutputColumns())
        output_stream.header.insert(ColumnWithTypeAndName{cte_output_stream.getByName(output.second).type, output.first});

    auto cte_ref_step = std::make_unique<CTERefStep>(output_stream, cte_step->getId(), cte_step->getOutputColumns(), cte_step->hasFilter());
    auto cte_ref_node = CTERefNode::createPlanNode(context->nextNodeId(), std::move(cte_ref_step), PlanNodes{}, node.getStatistics());
    return cte_ref_node;
}
}
