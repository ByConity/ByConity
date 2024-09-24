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
#include <Optimizer/Rewriter/RemoveApply.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/NameToType.h>
#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/WindowDescription.h>
#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/Correlation.h>
#include <Optimizer/ExpressionRewriter.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/formatAST.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/ApplyStep.h>
#include <QueryPlan/AssignUniqueIdStep.h>
#include <QueryPlan/Assignment.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/DistinctStep.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/SymbolAllocator.h>
#include <QueryPlan/TableScanStep.h>
#include <QueryPlan/WindowStep.h>
#include "DataTypes/DataTypesNumber.h"
#include "DataTypes/IDataType.h"

namespace DB
{
static ASTPtr getEmptySetResult(const QuantifierType & quantifier_type);
static ASTPtr
getBoundComparisons(const String & left, const String & minValue, const String & maxValue, const ASTQuantifiedComparison & qc_ast);
static bool shouldCompareValueWithLowerBound(const ASTQuantifiedComparison & qc_ast);
static void makeAggDescriptionsMinMaxCountCount2(
    AggregateDescriptions & aggregate_descriptions,
    ContextMutablePtr & context,
    String & min_value,
    String & max_value,
    String & count_all_value,
    String & count_non_null_value,
    PlanNodePtr & subquery_ptr,
    Names & qc_right);

namespace ErrorCodes
{
    extern const int REMOVE_SUBQUERY_ERROR;
}

template <class V>
static std::string printVector(const V & v, const String & sep = ",", const String & prefix = "", const String & suffix = "")
{
    std::stringstream out;
    out << prefix;
    if (!v.empty())
    {
        auto it = v.begin();
        out << *it++;
        for (; it != v.end(); ++it)
            out << sep << *it;
    }
    out << suffix;
    return out.str();
}

void RemoveCorrelatedScalarSubquery::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    CorrelatedScalarSubqueryVisitor visitor{context, plan.getCTEInfo()};
    Void v;
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, v);
    plan.update(result);
}

PlanNodePtr CorrelatedScalarSubqueryVisitor::visitApplyNode(ApplyNode & node, Void & v)
{
    auto apply_ptr = visitPlanNode(node, v);
    const auto & apply_step = *node.getStep();

    if (apply_step.getSubqueryType() != ApplyStep::SubqueryType::SCALAR)
    {
        return apply_ptr;
    }

    auto correlation = apply_step.getCorrelation();
    if (correlation.empty())
    {
        return apply_ptr;
    }

    PlanNodePtr input_ptr = apply_ptr->getChildren()[0];
    PlanNodePtr subquery_ptr = apply_ptr->getChildren()[1];

    auto subquery_step_ptr = subquery_ptr->getStep();
    // match pattern : scalar subquery without aggregation
    if (subquery_step_ptr->getType() == IQueryPlanStep::Type::EnforceSingleRow)
    {
        // step 1 : try to get the un-correlation part of subquery
        std::optional<DecorrelationResult> result = Decorrelation::decorrelateFilters(subquery_ptr->getChildren()[0], correlation, *context);
        if (!result.has_value())
        {
            throw Exception(
                "Correlated Scalar subquery de-correlation error, correlation filter not exists: ", ErrorCodes::REMOVE_SUBQUERY_ERROR);
        }

        DecorrelationResult & result_value = result.value();
        PlanNodePtr join_left = input_ptr;
        PlanNodePtr join_right = result_value.node;
        std::pair<Names, Names> key_pairs = result_value.buildJoinClause(join_left, join_right, correlation, context);

        // step 2 : Assign unique id symbol for join left
        String unique = context->getSymbolAllocator()->newSymbol("assign_unique_id_symbol");
        auto unique_id_step = std::make_unique<AssignUniqueIdStep>(join_left->getStep()->getOutputStream(), unique);
        auto unique_id_node = std::make_shared<AssignUniqueIdNode>(context->nextNodeId(), std::move(unique_id_step), PlanNodes{join_left});

        // step 3 : construct a Left JoinNode to replace ApplyNode
        const DataStream & left_data_stream = unique_id_node->getStep()->getOutputStream();
        const DataStream & right_data_stream = join_right->getStep()->getOutputStream();
        DataStreams streams = {left_data_stream, right_data_stream};

        auto left_header = left_data_stream.header;
        auto right_header = right_data_stream.header;
        NamesAndTypes output;
        for (const auto & item : left_header)
        {
            output.emplace_back(NameAndTypePair{item.name, item.type});
        }
        for (const auto & item : right_header)
        {
            output.emplace_back(NameAndTypePair{item.name, item.type});
        }
        auto join_step = std::make_shared<JoinStep>(
            streams,
            DataStream{.header = output},
            ASTTableJoin::Kind::Left,
            ASTTableJoin::Strictness::All,
            context->getSettingsRef().max_threads,
            context->getSettingsRef().optimize_read_in_order,
            key_pairs.first,
            key_pairs.second,
            std::vector<bool>{},
            PredicateConst::TRUE_VALUE,
            false,
            std::nullopt,
            ASOF::Inequality::GreaterOrEquals,
            DistributionType::UNKNOWN);
        PlanNodePtr join_node
            = std::make_shared<JoinNode>(context->nextNodeId(), std::move(join_step), PlanNodes{unique_id_node, join_right});

        String marker = context->getSymbolAllocator()->newSymbol("is_distinct");
        std::vector<String> distinct_symbols_list;
        for (const auto & item : left_header)
        {
            distinct_symbols_list.emplace_back(item.name);
        }
        auto mark_distinct_step
            = std::make_shared<MarkDistinctStep>(join_node->getStep()->getOutputStream(), marker, distinct_symbols_list);
        auto mark_distinct_node = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(mark_distinct_step), PlanNodes{join_node});

        auto check_subquery = makeASTFunction("check_subquery_return_single_row", ASTs{std::make_shared<ASTIdentifier>(marker)});

        auto filter_step = std::make_shared<FilterStep>(join_node->getStep()->getOutputStream(), check_subquery);
        PlanNodePtr filter_node
            = std::make_shared<FilterNode>(context->nextNodeId(), std::move(filter_step), PlanNodes{mark_distinct_node});

        return filter_node;
    }

    bool match = false;
    PlanNodePtr scalar_agg;
    PlanNodePtr scalar_agg_source;
    // match pattern : scalar subquery with aggregation
    if (subquery_step_ptr->getType() == IQueryPlanStep::Type::Aggregating)
    {
        const auto & step = dynamic_cast<const AggregatingStep &>(*subquery_step_ptr);
        const auto & keys = step.getKeys();
        if (keys.empty())
        {
            match = true;
            scalar_agg = subquery_ptr;
            scalar_agg_source = subquery_ptr->getChildren()[0];
        }
    }
    // match pattern : scalar subquery with projection + aggregation
    Assignments ass_after_scalar_agg;
    NameToType name_to_type_after_scalar_agg;
    if (subquery_step_ptr->getType() == IQueryPlanStep::Type::Projection)
    {
        const auto & expression_step = dynamic_cast<const ProjectionStep &>(*subquery_step_ptr);
        ass_after_scalar_agg = expression_step.getAssignments();
        name_to_type_after_scalar_agg = expression_step.getNameToType();
        PlanNodePtr child_ptr = subquery_ptr->getChildren()[0];
        auto child_step_ptr = child_ptr->getStep();
        if (child_step_ptr->getType() == IQueryPlanStep::Type::Aggregating)
        {
            const auto & step = dynamic_cast<const AggregatingStep &>(*child_step_ptr);
            const auto & keys = step.getKeys();
            if (keys.empty())
            {
                match = true;
                scalar_agg = child_ptr;
                scalar_agg_source = child_ptr->getChildren()[0];
            }
        }
    }
    if (!match)
    {
        throw Exception("Un-matched pattern for Correlated Scalar subquery", ErrorCodes::REMOVE_SUBQUERY_ERROR);
    }

    // step 1 : try to get the un-correlation part of subquery
    std::optional<DecorrelationResult> result = Decorrelation::decorrelateFilters(scalar_agg_source, correlation, *context);
    if (!result.has_value())
    {
        throw Exception(
            "Correlated Scalar subquery de-correlation error, correlation filter not exists: ", ErrorCodes::REMOVE_SUBQUERY_ERROR);
    }

    DecorrelationResult & result_value = result.value();
    PlanNodePtr join_left = input_ptr;
    PlanNodePtr join_right = result_value.node;
    std::pair<Names, Names> key_pairs = result_value.buildJoinClause(join_left, join_right, correlation, context);

    // step 2 : Assign unique id symbol for join left
    String unique = context->getSymbolAllocator()->newSymbol("assign_unique_id_symbol");
    auto unique_id_step = std::make_unique<AssignUniqueIdStep>(join_left->getStep()->getOutputStream(), unique);
    auto unique_id_node = std::make_shared<AssignUniqueIdNode>(context->nextNodeId(), std::move(unique_id_step), PlanNodes{join_left});

    // step 3 : Assign non null symbol for build side.
    Assignments null_value_assignments;
    NameToType null_value_name_to_type;
    for (const auto & column : join_right->getStep()->getOutputStream().header)
    {
        Assignment previous{column.name, std::make_shared<ASTIdentifier>(column.name)};
        null_value_assignments.emplace_back(previous);
        null_value_name_to_type[column.name] = column.type;
    }
    String non_null = context->getSymbolAllocator()->newSymbol("build_side_non_null_symbol");
    Assignment non_null_value{non_null, std::make_shared<ASTLiteral>(1u)};
    null_value_assignments.emplace_back(non_null_value);
    null_value_name_to_type[non_null] = std::make_shared<DataTypeUInt8>();

    auto non_null_step
        = std::make_shared<ProjectionStep>(join_right->getStep()->getOutputStream(), null_value_assignments, null_value_name_to_type);
    auto non_null_node = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(non_null_step), PlanNodes{join_right});

    // step 4 : construct a Left JoinNode to replace ApplyNode
    const DataStream & left_data_stream = unique_id_node->getStep()->getOutputStream();
    const DataStream & right_data_stream = non_null_node->getStep()->getOutputStream();
    DataStreams streams = {left_data_stream, right_data_stream};

    auto left_header = left_data_stream.header;
    auto right_header = right_data_stream.header;
    NamesAndTypes output;
    for (const auto & item : left_header)
    {
        output.emplace_back(NameAndTypePair{item.name, item.type});
    }
    for (const auto & item : right_header)
    {
        output.emplace_back(NameAndTypePair{item.name, item.type});
    }
    auto join_step = std::make_shared<JoinStep>(
        streams,
        DataStream{.header = output},
        ASTTableJoin::Kind::Left,
        ASTTableJoin::Strictness::All,
        context->getSettingsRef().max_threads,
        context->getSettingsRef().optimize_read_in_order,
        key_pairs.first,
        key_pairs.second,
        std::vector<bool>{},
        PredicateConst::TRUE_VALUE,
        false,
        std::nullopt,
        ASOF::Inequality::GreaterOrEquals,
        DistributionType::UNKNOWN);
    PlanNodePtr join_node
        = std::make_shared<JoinNode>(context->nextNodeId(), std::move(join_step), PlanNodes{unique_id_node, non_null_node});

    // step 5 : construct a AggregateNode with group-by columns (replace the scalar aggregation)
    const auto & scalar_agg_step = dynamic_cast<const AggregatingStep &>(*scalar_agg->getStep());
    Names keys;
    for (const auto & item : left_header)
    {
        keys.emplace_back(item.name);
    }

    const AggregateDescriptions & descs = scalar_agg_step.getAggregates();
    AggregateDescriptions descs_with_mask;
    for (const auto & desc : descs)
    {
        AggregateDescription desc_with_mask;
        AggregateFunctionPtr fun = desc.function;
        Names argument_names = desc.argument_names;
        DataTypes types;
        for (auto & argument_name : argument_names)
        {
            for (const auto & column : non_null_node->getStep()->getOutputStream().header)
            {
                if (argument_name == column.name)
                {
                    types.emplace_back(recursiveRemoveLowCardinality(column.type));
                }
            }
        }
        types.emplace_back(std::make_shared<DataTypeUInt8>());
        String fun_name = fun->getName();

        // calculate result with subquery condition match, e.g. non_null symbols equals to 1.
        AggregateFunctionProperties properties;
        AggregateFunctionPtr fun_with_mask = AggregateFunctionFactory::instance().get(fun_name + "If", types, desc.parameters, properties);
        argument_names.emplace_back(non_null);

        desc_with_mask.mask_column = non_null;
        desc_with_mask.function = fun_with_mask;
        desc_with_mask.parameters = desc.parameters;
        desc_with_mask.column_name = desc.column_name;
        desc_with_mask.argument_names = argument_names;
        desc_with_mask.parameters = desc.parameters;
        desc_with_mask.arguments = desc.arguments;
        descs_with_mask.emplace_back(desc_with_mask);
    }
    auto group_agg_step = std::make_shared<AggregatingStep>(
        join_node->getStep()->getOutputStream(), keys, NameSet{}, descs_with_mask, GroupingSetsParamsList{}, true);
    auto group_agg_node = std::make_shared<AggregatingNode>(context->nextNodeId(), std::move(group_agg_step), PlanNodes{join_node});

    // step 6 : project used columns
    Assignments assignments;
    NameToType name_to_type;
    const DataStream & agg_output = group_agg_node->getStep()->getOutputStream();
    for (const auto & column : agg_output.header)
    {
        Assignment agg_column{column.name, std::make_shared<ASTIdentifier>(column.name)};
        assignments.emplace_back(agg_column);
        name_to_type[column.name] = column.type;
    }

    // Add projections from origin query plan.
    if (!ass_after_scalar_agg.empty())
    {
        for (auto & ass : ass_after_scalar_agg)
        {
            if (!name_to_type.contains(ass.first))
            {
                assignments.emplace_back(ass);
                name_to_type[ass.first] = name_to_type_after_scalar_agg[ass.first];
            }
        }
    }

    auto expression_step = std::make_shared<ProjectionStep>(group_agg_node->getStep()->getOutputStream(), assignments, name_to_type);
    auto expression_node = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(expression_step), PlanNodes{group_agg_node});
    return expression_node;
}

void RemoveUnCorrelatedScalarSubquery::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    UnCorrelatedScalarSubqueryVisitor visitor{context, plan.getCTEInfo()};
    Void v;
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, v);
    plan.update(result);
}

PlanNodePtr UnCorrelatedScalarSubqueryVisitor::visitApplyNode(ApplyNode & node, Void & v)
{
    PlanNodePtr apply_ptr = visitPlanNode(node, v);
    const auto & apply_step = *node.getStep();

    if (apply_step.getSubqueryType() != ApplyStep::SubqueryType::SCALAR)
    {
        return apply_ptr;
    }

    const auto & correlation = apply_step.getCorrelation();
    if (!correlation.empty())
    {
        return apply_ptr;
    }

    PlanNodePtr input_ptr = apply_ptr->getChildren()[0];
    PlanNodePtr subquery_ptr = apply_ptr->getChildren()[1];

    if (Correlation::isUnreferencedScalar(input_ptr))
    {
        if (apply_step.getApplyType() == ApplyStep::ApplyType::CROSS)
        {
            return subquery_ptr;
        }
    }

    if (Correlation::isUnreferencedScalar(subquery_ptr))
    {
        return input_ptr;
    }

    if (context->getSettingsRef().enable_execute_uncorrelated_subquery)
    {
        const Assignment & apply_assignment = apply_step.getAssignment();
        String sub_query_column_name = apply_assignment.first;
        DataTypePtr sub_query_column_type = apply_step.getAssignmentDataType();

        int rule_id = context->getRuleId();
        String sub_query_id = std::to_string(context->incAndGetSubQueryId());
        String subquery_name_prefix = std::to_string(rule_id) + "_RemoveUnCorrelatedScalarSubquery_ExecuteSubQuery_" + sub_query_id + "_";

        context->setExecuteSubQueryPath(subquery_name_prefix);

        // set a different query id for sub query.
        String query_id = context->getCurrentQueryId();
        context->setCurrentQueryId(query_id + "_sub_query_" + sub_query_id);

        SelectQueryOptions sub_query_options;
        InterpreterSelectQueryUseOptimizer interpreter{subquery_ptr, cte_helper.getCTEInfo(), context, sub_query_options};
        BlockIO sub_query_result = interpreter.execute();

        context->removeExecuteSubQueryPath();
        context->setCurrentQueryId(query_id);
        context->setRuleId(rule_id);

        BlockInputStreamPtr sub_query_block_stream = sub_query_result.getInputStream();
        Block block = sub_query_block_stream->read();

        size_t rows = block.rows();
        Utils::checkArgument(rows == 1, "Scalar sub-query must return single row");

        const auto & sub_query_column = *(block.getByName(sub_query_column_name).column);

        ASTPtr sub_query_column_value;

        // process tuple values
        if (strcmp(sub_query_column_type->getFamilyName(), "Tuple") == 0)
        {
            const DataTypeTuple & tuple_type = dynamic_cast<const DataTypeTuple &>(*sub_query_column_type);
            const DataTypes & types = tuple_type.getElements();
            ASTs sub_query_column_values;
            Tuple sub_query_column_tuple = sub_query_column[0].get<Tuple>();
            for (size_t i = 0; i < sub_query_column_tuple.size(); i++)
            {
                String type_name = types[i]->getName();
                sub_query_column_values.emplace_back(makeASTFunction(
                    "cast", std::make_shared<ASTLiteral>(sub_query_column_tuple[i]), std::make_shared<ASTLiteral>(type_name)));
            }
            sub_query_column_value = makeASTFunction("tuple", sub_query_column_values);
        }
        else
        {
            // scalar values
            String type_name = sub_query_column_type->getName();
            sub_query_column_value
                = makeASTFunction("cast", std::make_shared<ASTLiteral>(sub_query_column[0]), std::make_shared<ASTLiteral>(type_name));
        }

        // create a new projection step, include scalar sub query result.
        Assignments assignments;
        NameToType types;

        // add input columns
        for (const auto & column : input_ptr->getStep()->getOutputStream().header)
        {
            assignments.emplace_back(column.name, std::make_shared<ASTIdentifier>(column.name));
            types[column.name] = column.type;
        }

        // add sub query column
        assignments.emplace_back(sub_query_column_name, sub_query_column_value);
        types[sub_query_column_name] = sub_query_column_type;

        auto scalar_step = std::make_shared<ProjectionStep>(input_ptr->getStep()->getOutputStream(), assignments, types);
        return PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(scalar_step), PlanNodes{input_ptr});
    }

    const DataStream & left_data_stream = input_ptr->getStep()->getOutputStream();
    const DataStream & right_data_stream = subquery_ptr->getStep()->getOutputStream();
    DataStreams streams = {left_data_stream, right_data_stream};

    NamesAndTypes output;
    for (const auto & item : left_data_stream.header)
    {
        output.emplace_back(NameAndTypePair{item.name, item.type});
    }
    for (const auto & item : right_data_stream.header)
    {
        output.emplace_back(NameAndTypePair{item.name, item.type});
    }

    auto join_step = std::make_shared<JoinStep>(
        streams,
        DataStream{.header = std::move(output)},
        ASTTableJoin::Kind::Cross,
        ASTTableJoin::Strictness::Unspecified,
        context->getSettingsRef().max_threads,
        context->getSettingsRef().optimize_read_in_order,
        Names{},
        Names{},
        std::vector<bool>{},
        PredicateConst::TRUE_VALUE,
        false,
        std::nullopt,
        ASOF::Inequality::GreaterOrEquals,
        DistributionType::UNKNOWN);

    return std::make_shared<JoinNode>(context->nextNodeId(), std::move(join_step), PlanNodes{input_ptr, subquery_ptr});
}

void RemoveCorrelatedInSubquery::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    CorrelatedInSubqueryVisitor visitor{context, plan.getCTEInfo()};
    Void v;
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, v);
    plan.update(result);
}

PlanNodePtr CorrelatedInSubqueryVisitor::visitApplyNode(ApplyNode & node, Void & v)
{
    PlanNodePtr apply_ptr = visitPlanNode(node, v);
    const auto & apply_step = *node.getStep();

    if (apply_step.getSubqueryType() != ApplyStep::SubqueryType::IN)
    {
        return apply_ptr;
    }

    auto correlation = apply_step.getCorrelation();
    if (correlation.empty())
    {
        return apply_ptr;
    }

    PlanNodePtr input_ptr = apply_ptr->getChildren()[0];
    PlanNodePtr subquery_ptr = apply_ptr->getChildren()[1];

    std::optional<DecorrelationResult> result = Decorrelation::decorrelateFilters(subquery_ptr, correlation, *context);
    if (!result.has_value())
    {
        throw Exception(
            "Correlated In subquery de-correlation error, correlation filter not exists: " + printVector(correlation),
            ErrorCodes::REMOVE_SUBQUERY_ERROR);
    }

    DecorrelationResult & result_value = result.value();
    PlanNodePtr decorrelation_source = result_value.node;
    std::pair<Names, Names> correlation_predicate = result_value.buildJoinClause(input_ptr, decorrelation_source, correlation, context);

    // step 1 : Assign unique id symbol for join left
    String unique = context->getSymbolAllocator()->newSymbol("assign_unique_id_symbol");
    auto unique_id_step = std::make_unique<AssignUniqueIdStep>(input_ptr->getStep()->getOutputStream(), unique);
    auto unique_id_node = std::make_shared<AssignUniqueIdNode>(context->nextNodeId(), std::move(unique_id_step), PlanNodes{input_ptr});

    const DataStream & decorrelation_output = decorrelation_source->getStep()->getOutputStream();

    // step 2 : Assign non null symbol with default value 0.
    Assignments assignments_new;
    NameToType name_to_type;
    for (const auto & column : decorrelation_output.header)
    {
        Assignment ass{column.name, std::make_shared<ASTIdentifier>(column.name)};
        assignments_new.emplace_back(ass);
        name_to_type[column.name] = column.type;
    }

    String non_null = context->getSymbolAllocator()->newSymbol("build_side_non_null_symbol");
    ASTPtr value = std::make_shared<ASTLiteral>(1u);
    Assignment ass{non_null, value};
    assignments_new.emplace_back(ass);
    name_to_type[non_null] = std::make_shared<DataTypeUInt8>();

    auto expression_step = std::make_shared<ProjectionStep>(decorrelation_source->getStep()->getOutputStream(), assignments_new, name_to_type);
    auto expression_node
        = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(expression_step), PlanNodes{decorrelation_source});

    // step 3 : construct a JoinNode to replace ApplyNode
    const PlanNodePtr & join_left = unique_id_node;
    const PlanNodePtr & join_right = expression_node;
    const DataStream & left_data_stream = join_left->getStep()->getOutputStream();
    const DataStream & right_data_stream = join_right->getStep()->getOutputStream();
    DataStreams streams = {left_data_stream, right_data_stream};
    auto left_header = left_data_stream.header;
    auto right_header = right_data_stream.header;
    NamesAndTypes output;
    Names left_names;
    Names right_names;
    for (const auto & item : left_header)
    {
        output.emplace_back(NameAndTypePair{item.name, item.type});
        left_names.emplace_back(item.name);
    }
    for (const auto & item : right_header)
    {
        output.emplace_back(NameAndTypePair{item.name, item.type});
        right_names.emplace_back(item.name);
    }

    const auto & in_assignment = apply_step.getAssignment();
    const auto & in_fun = in_assignment.second->as<ASTFunction &>();
    ASTIdentifier & fun_left = in_fun.arguments->children[0]->as<ASTIdentifier &>();
    ASTIdentifier & fun_right = in_fun.arguments->children[1]->as<ASTIdentifier &>();
    Names in_left{fun_left.name()};
    Names in_right{fun_right.name()};

    String probe_symbol = in_left[0];
    String build_symbol = in_right[0];

    ASTPtr probe_symbol_isnull = makeASTFunction("isNull", ASTs{std::make_shared<ASTIdentifier>(probe_symbol)});
    ASTPtr probe_build_equal
        = makeASTFunction("equals", ASTs{std::make_shared<ASTIdentifier>(probe_symbol), std::make_shared<ASTIdentifier>(build_symbol)});
    ASTPtr build_symbol_isnull = makeASTFunction("isNull", ASTs{std::make_shared<ASTIdentifier>(build_symbol)});

    ASTPtr or_filter = makeASTFunction("or", ASTs{probe_symbol_isnull, probe_build_equal, build_symbol_isnull});

    std::vector<ConstASTPtr> filter = result->extractFilter();
    ASTPtr join_filter = PredicateUtils::combineConjuncts(filter);

    ConstASTs combine_filters{or_filter, join_filter};
    ASTPtr combine_filter = PredicateUtils::combineConjuncts(combine_filters);

    // TODO Left join with filter in operators level. for now, correlation in subquery will throw exception when executing.
    auto join_step = std::make_shared<JoinStep>(
        streams,
        DataStream{.header = output},
        ASTTableJoin::Kind::Left,
        ASTTableJoin::Strictness::All,
        context->getSettingsRef().max_threads,
        context->getSettingsRef().optimize_read_in_order,
        correlation_predicate.first,
        correlation_predicate.second,
        std::vector<bool>{},
        combine_filter,
        false,
        std::nullopt,
        ASOF::Inequality::GreaterOrEquals,
        DistributionType::UNKNOWN);
    PlanNodePtr join_node = std::make_shared<JoinNode>(context->nextNodeId(), std::move(join_step), PlanNodes{join_left, join_right});

    // step 6 project match_condition_symbol, null_match_condition_symbol
    String match_condition_symbol = context->getSymbolAllocator()->newSymbol("system_match_condition_symbol");
    ASTPtr probe_symbol_is_not_null = makeASTFunction("isNotNull", ASTs{std::make_shared<ASTIdentifier>(probe_symbol)});
    ASTPtr build_symbol_is_not_null = makeASTFunction("isNotNull", ASTs{std::make_shared<ASTIdentifier>(build_symbol)});
    ASTPtr match_condition = makeASTFunction("and", ASTs{probe_symbol_is_not_null, build_symbol_is_not_null});

    String null_match_condition_symbol = context->getSymbolAllocator()->newSymbol("system_null_match_condition_symbol");
    ASTPtr build_side_known_non_null_is_not_null = makeASTFunction("isNotNull", ASTs{std::make_shared<ASTIdentifier>(non_null)});
    ASTPtr match_condition_not = makeASTFunction("not", ASTs{match_condition});
    ASTPtr null_match_condition = makeASTFunction("and", ASTs{build_side_known_non_null_is_not_null, match_condition_not});

    Assignments pre_assignments;
    NameToType pre_name_to_type;
    for (const auto & column : join_node->getStep()->getOutputStream().header)
    {
        Assignment filter_output{column.name, std::make_shared<ASTIdentifier>(column.name)};
        pre_assignments.emplace_back(filter_output);
        pre_name_to_type[column.name] = column.type;
    }

    // step 7 : count match_condition_symbol、null_match_condition_symbol
    Assignment ass_match_condition_symbol{
        match_condition_symbol, makeASTFunction("cast", match_condition, std::make_shared<ASTLiteral>("UInt8"))};
    pre_assignments.emplace_back(ass_match_condition_symbol);
    pre_name_to_type[match_condition_symbol] = std::make_shared<DataTypeUInt8>();

    Assignment ass_null_match_condition_symbol{
        null_match_condition_symbol, makeASTFunction("cast", null_match_condition, std::make_shared<ASTLiteral>("UInt8"))};
    pre_assignments.emplace_back(ass_null_match_condition_symbol);
    pre_name_to_type[null_match_condition_symbol] = std::make_shared<DataTypeUInt8>();

    auto pre_expression_step = std::make_shared<ProjectionStep>(join_node->getStep()->getOutputStream(), pre_assignments, pre_name_to_type);
    auto pre_expression_node
        = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(pre_expression_step), PlanNodes{join_node});

    Names keys;
    for (const auto & item : left_header)
    {
        keys.emplace_back(item.name);
    }

    DataTypes types{std::make_shared<DataTypeUInt8>()};
    Array parameters = Array();
    AggregateFunctionProperties properties;
    AggregateFunctionPtr agg_fun = AggregateFunctionFactory::instance().get("countIf", types, parameters, properties);
    AggregateDescription aggregate_desc_1;
    aggregate_desc_1.column_name = context->getSymbolAllocator()->newSymbol("count_matches");
    aggregate_desc_1.parameters = parameters;
    aggregate_desc_1.function = agg_fun;
    aggregate_desc_1.argument_names = Names{match_condition_symbol};

    DataTypes types_2{std::make_shared<DataTypeUInt8>()};
    Array parameters_2 = Array();
    AggregateFunctionPtr agg_fun_2 = AggregateFunctionFactory::instance().get("countIf", types_2, parameters_2, properties);
    AggregateDescription aggregate_desc_2;
    aggregate_desc_2.column_name = context->getSymbolAllocator()->newSymbol("count_null_matches");
    aggregate_desc_2.parameters = parameters_2;
    aggregate_desc_2.function = agg_fun_2;
    aggregate_desc_2.argument_names = Names{null_match_condition_symbol};

    AggregateDescriptions aggregate_descs{aggregate_desc_1, aggregate_desc_2};
    auto count_step = std::make_shared<AggregatingStep>(
        pre_expression_node->getStep()->getOutputStream(), keys, NameSet{}, aggregate_descs, GroupingSetsParamsList{}, true);
    auto count_node = std::make_shared<AggregatingNode>(context->nextNodeId(), std::move(count_step), PlanNodes{pre_expression_node});

    // step 8 project match values
    ASTPtr count_matches_greater_zero
        = makeASTFunction("greater", ASTs{std::make_shared<ASTIdentifier>(aggregate_desc_1.column_name), std::make_shared<ASTLiteral>(0u)});
    ASTPtr true_value = std::make_shared<ASTLiteral>(true);
    ASTPtr count_null_matches_greater_zero
        = makeASTFunction("greater", ASTs{std::make_shared<ASTIdentifier>(aggregate_desc_2.column_name), std::make_shared<ASTLiteral>(0u)});
    ASTPtr false_value = std::make_shared<ASTLiteral>(false);
    ASTPtr else_value = std::make_shared<ASTLiteral>(false);
    ASTPtr multi_if = makeASTFunction(
        "multiIf", ASTs{count_matches_greater_zero, true_value, count_null_matches_greater_zero, false_value, else_value});

    Assignments multi_if_assignments;
    NameToType multi_if_pre_name_to_type;
    for (const auto & column : join_left->getStep()->getOutputStream().header)
    {
        Assignment join_left_column{column.name, std::make_shared<ASTIdentifier>(column.name)};
        multi_if_assignments.emplace_back(join_left_column);
        multi_if_pre_name_to_type[column.name] = column.type;
    }

    // step 9 rewrite in/notIn predicate to symbols
    if (in_fun.name == "in" || in_fun.name == "globalIn")
    {
        ASTPtr multi_if_match = makeASTFunction("equals", ASTs{multi_if, std::make_shared<ASTLiteral>(1u)});
        Assignment multi_if_output{apply_step.getAssignment().first, multi_if_match};
        multi_if_assignments.emplace_back(multi_if_output);
        multi_if_pre_name_to_type[apply_step.getAssignment().first] = apply_step.getAssignmentDataType();
    }
    if (in_fun.name == "notIn" || in_fun.name == "globalNotIn")
    {
        ASTPtr multi_if_match = makeASTFunction("equals", ASTs{multi_if, std::make_shared<ASTLiteral>(0u)});
        Assignment not_multi_if_output{apply_step.getAssignment().first, multi_if_match};
        multi_if_assignments.emplace_back(not_multi_if_output);
        multi_if_pre_name_to_type[apply_step.getAssignment().first] = apply_step.getAssignmentDataType();
    }
    auto multi_if_expression_step
        = std::make_shared<ProjectionStep>(count_node->getStep()->getOutputStream(), multi_if_assignments, multi_if_pre_name_to_type);
    auto multi_if_expression_node
        = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(multi_if_expression_step), PlanNodes{count_node});

    // In/notIn function has been rewritten into symbols, e.g. apply_step.getAssignment().
    // if symbol values is true, then condition match, else not match.
    return multi_if_expression_node;
}

void RemoveUnCorrelatedInSubquery::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    UnCorrelatedInSubqueryVisitor visitor{context, plan.getCTEInfo()};
    Void v;
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, v);
    plan.update(result);
}

PlanNodePtr UnCorrelatedInSubqueryVisitor::visitApplyNode(ApplyNode & node, Void & v)
{
    PlanNodePtr apply_ptr = visitPlanNode(node, v);
    const auto & apply_step = *node.getStep();

    if (apply_step.getSubqueryType() != ApplyStep::SubqueryType::IN)
    {
        return apply_ptr;
    }

    const auto & correlation = apply_step.getCorrelation();
    if (!correlation.empty())
    {
        return apply_ptr;
    }

    PlanNodePtr input_ptr = apply_ptr->getChildren()[0];
    PlanNodePtr subquery_ptr = apply_ptr->getChildren()[1];

    const DataStream & left_data_stream = input_ptr->getStep()->getOutputStream();
    const DataStream & right_data_stream = subquery_ptr->getStep()->getOutputStream();

    const auto & in_assignment = apply_step.getAssignment();
    const auto & in_fun = in_assignment.second->as<ASTFunction &>();
    ASTIdentifier & fun_left = in_fun.arguments->children[0]->as<ASTIdentifier &>();
    ASTIdentifier & fun_right = in_fun.arguments->children[1]->as<ASTIdentifier &>();

    // apply_assignment : _in_subquery = d_year IN (s_store_sk)
    // apply_assignment.first : _in_subquery
    // apply_assignment.second : d_year IN (s_store_sk)
    // in_fun_left : d_year
    // in_fun_right : s_store_sk
    if (context->getSettingsRef().enable_execute_uncorrelated_subquery)
    {
        std::optional<PlanNodeStatisticsPtr> stats = CardinalityEstimator::estimate(*subquery_ptr, cte_helper.getCTEInfo(), context, true);

        // For large in subquery, execution performance is slow !!!
        if (stats.has_value() && stats.value()->getRowCount() <= context->getSettingsRef().execute_uncorrelated_in_subquery_size)
        {
            const Assignment & apply_assignment = apply_step.getAssignment();
            String sub_query_column_name = apply_assignment.first;
            DataTypePtr sub_query_column_type = apply_step.getAssignmentDataType();

            int rule_id = context->getRuleId();
            String sub_query_id = std::to_string(context->incAndGetSubQueryId());
            String subquery_name_prefix = std::to_string(rule_id) + "_RemoveUnCorrelatedInSubquery_ExecuteSubQuery_" + sub_query_id + "_";

            context->setExecuteSubQueryPath(subquery_name_prefix);

            // set a different query id for sub query.
            String query_id = context->getCurrentQueryId();
            context->setCurrentQueryId(query_id + "_sub_query_" + sub_query_id);

            SelectQueryOptions sub_query_options;
            InterpreterSelectQueryUseOptimizer interpreter{subquery_ptr, cte_helper.getCTEInfo(), context, sub_query_options};
            BlockIO sub_query_result = interpreter.execute();

            context->removeExecuteSubQueryPath();
            context->setCurrentQueryId(query_id);
            context->setRuleId(rule_id);

            BlockInputStreamPtr sub_query_block_stream = sub_query_result.getInputStream();

            ASTs sub_query_column_values;
            while (Block block = sub_query_block_stream->read())
            {
                for (size_t i = 0; i < block.rows(); ++i)
                {
                    const auto & sub_query_column = *(block.getByName(in_fun.arguments->children[1]->getColumnName()).column);
                    ASTPtr sub_query_column_value = std::make_shared<ASTLiteral>(sub_query_column[i]);
                    sub_query_column_values.emplace_back(sub_query_column_value);
                }
            }

            // create a new projection step, include scalar sub query result.
            Assignments assignments_in;
            NameToType types;

            // add input columns
            for (const auto & column : input_ptr->getStep()->getOutputStream().header)
            {
                assignments_in.emplace_back(column.name, std::make_shared<ASTIdentifier>(column.name));
                types[column.name] = column.type;
            }

            /// Function tuple requires at least one argument,
            /// insert a Null element when sub query result is empty.
            if (sub_query_column_values.empty())
            {
                sub_query_column_values.emplace_back(std::make_shared<ASTLiteral>(DB::Field()));
            }
            ASTPtr value_tuple = makeASTFunction("tuple", sub_query_column_values);

            // add sub query column
            assignments_in.emplace_back(sub_query_column_name, makeASTFunction(in_fun.name, ASTs{in_fun.arguments->children[0], value_tuple}));
            types[sub_query_column_name] = sub_query_column_type;

            auto in_step = std::make_shared<ProjectionStep>(input_ptr->getStep()->getOutputStream(), assignments_in, types);
            return PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(in_step), PlanNodes{input_ptr});
        }
    }

    Names in_left{fun_left.name()};
    Names in_right{fun_right.name()};

    const auto & settings = context->getSettingsRef();
    UInt64 limit_for_distinct = 0;
    // step 1 : add distinct step on subquery to remove duplicated values.
    // this is to avoid the expand of join left side.
    auto distinct_step = std::make_unique<DistinctStep>(
        right_data_stream,
        SizeLimits(settings.max_rows_in_distinct, settings.max_bytes_in_distinct, settings.distinct_overflow_mode),
        limit_for_distinct,
        in_right,
        true);
    auto distinct_node = std::make_shared<DistinctNode>(context->nextNodeId(), std::move(distinct_step), PlanNodes{subquery_ptr});

    // step 2 : Assign non null symbol with default value 0.
    Assignments assignments;
    NameToType name_to_type;
    for (const auto & column : distinct_node->getStep()->getOutputStream().header)
    {
        Assignment ass{column.name, std::make_shared<ASTIdentifier>(column.name)};
        assignments.emplace_back(ass);
        name_to_type[column.name] = column.type;
    }

    String non_null = context->getSymbolAllocator()->newSymbol("build_side_non_null_symbol");
    ASTPtr value = std::make_shared<ASTLiteral>(1u);
    Assignment ass{non_null, value};
    assignments.emplace_back(ass);
    name_to_type[non_null] = std::make_shared<DataTypeUInt8>();

    auto expression_step = std::make_shared<ProjectionStep>(distinct_node->getStep()->getOutputStream(), assignments, name_to_type);
    auto expression_node = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(expression_step), PlanNodes{distinct_node});

    // step 3 : Rewrite un-correlated in subquery to Left Join
    const DataStream & distinct_data_stream = expression_node->getStep()->getOutputStream();
    DataStreams streams = {left_data_stream, distinct_data_stream};
    auto left_header = left_data_stream.header;
    auto right_header = distinct_data_stream.header;
    NamesAndTypes output;
    for (const auto & item : left_header)
    {
        output.emplace_back(NameAndTypePair{item.name, item.type});
    }
    for (const auto & item : right_header)
    {
        output.emplace_back(NameAndTypePair{item.name, item.type});
    }

    auto join_step = std::make_shared<JoinStep>(
        streams,
        DataStream{.header = output},
        ASTTableJoin::Kind::Left,
        ASTTableJoin::Strictness::All,
        context->getSettingsRef().max_threads,
        context->getSettingsRef().optimize_read_in_order,
        in_left,
        in_right,
        std::vector<bool>{},
        PredicateConst::TRUE_VALUE,
        false,
        std::nullopt,
        ASOF::Inequality::GreaterOrEquals,
        DistributionType::UNKNOWN);

    PlanNodePtr join_node = std::make_shared<JoinNode>(context->nextNodeId(), std::move(join_step), PlanNodes{input_ptr, expression_node});

    // step 3 : project in result
    Assignments in_assignments;
    NameToType in_name_to_type;
    for (const auto & column : input_ptr->getStep()->getOutputStream().header)
    {
        if (column.name == non_null)
            continue;
        Assignment join_column{column.name, std::make_shared<ASTIdentifier>(column.name)};
        in_assignments.emplace_back(join_column);
        in_name_to_type[column.name] = column.type;
    }

    // compute in/notIn function result, project it as a bool symbol.
    if (in_fun.name == "in" || in_fun.name == "globalIn")
    {
        ASTPtr true_predicate = makeASTFunction("equals", std::make_shared<ASTIdentifier>(non_null), std::make_shared<ASTLiteral>(1u));
        ASTPtr true_value = std::make_shared<ASTLiteral>(1u);
        ASTPtr false_predicate
            = makeASTFunction("equals", std::make_shared<ASTIdentifier>(non_null), std::make_shared<ASTLiteral>(Field()));
        ASTPtr false_value = std::make_shared<ASTLiteral>(0u);
        ASTPtr else_value = std::make_shared<ASTLiteral>(0u);
        auto multi_if = makeASTFunction("multiIf", true_predicate, true_value, false_predicate, false_value, else_value);
        auto cast = makeASTFunction("cast", multi_if, std::make_shared<ASTLiteral>("UInt8"));

        if (context->getSettingsRef().join_use_nulls)
        {
            // if left is NULL, then result is null
            ASTPtr left_is_null = makeASTFunction("isNull", std::make_shared<ASTIdentifier>(fun_left));

            Assignment in_ass{
                apply_step.getAssignment().first, makeASTFunction("if", left_is_null, std::make_shared<ASTLiteral>(Field()), cast)};
            in_assignments.emplace_back(in_ass);
        }
        else
        {
            Assignment in_ass{apply_step.getAssignment().first, cast};
            in_assignments.emplace_back(in_ass);
        }
        in_name_to_type[apply_step.getAssignment().first] = apply_step.getAssignmentDataType();
    }
    if (in_fun.name == "notIn" || in_fun.name == "globalNotIn")
    {
        ASTPtr true_predicate = makeASTFunction("equals", std::make_shared<ASTIdentifier>(non_null), std::make_shared<ASTLiteral>(1u));
        ASTPtr true_value = std::make_shared<ASTLiteral>(1u);
        ASTPtr false_predicate
            = makeASTFunction("equals", std::make_shared<ASTIdentifier>(non_null), std::make_shared<ASTLiteral>(Field()));
        ASTPtr false_value = std::make_shared<ASTLiteral>(0u);
        ASTPtr else_value = std::make_shared<ASTLiteral>(0u);
        auto multi_if = makeASTFunction("multiIf", true_predicate, true_value, false_predicate, false_value, else_value);
        auto cast = makeASTFunction("cast", multi_if, std::make_shared<ASTLiteral>("UInt8"));
        ASTPtr not_equals_fn = makeASTFunction("not", cast);

        if (context->getSettingsRef().join_use_nulls)
        {
            // if left is NULL, then result is null
            ASTPtr left_is_null = makeASTFunction("isNull", std::make_shared<ASTIdentifier>(fun_left));

            Assignment not_in_ass{
                apply_step.getAssignment().first,
                makeASTFunction("if", left_is_null, std::make_shared<ASTLiteral>(Field()), not_equals_fn)};
            in_assignments.emplace_back(not_in_ass);
        }
        else
        {
            Assignment not_in_ass{apply_step.getAssignment().first, not_equals_fn};
            in_assignments.emplace_back(not_in_ass);
        }
        in_name_to_type[apply_step.getAssignment().first] = apply_step.getAssignmentDataType();
    }

    auto project_step = std::make_shared<ProjectionStep>(join_node->getStep()->getOutputStream(), in_assignments, in_name_to_type);
    auto project_node = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(project_step), PlanNodes{join_node});

    // In/notIn function has been rewritten into symbols, e.g. apply_step.getAssignment().
    // if symbol values is true, then condition match, else not match.
    return project_node;
}

void RemoveCorrelatedExistsSubquery::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    CorrelatedExistsSubqueryVisitor visitor{context, plan.getCTEInfo()};
    Void v;
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, v);
    plan.update(result);
}

PlanNodePtr CorrelatedExistsSubqueryVisitor::visitApplyNode(ApplyNode & node, Void & v)
{
    PlanNodePtr apply_ptr = visitPlanNode(node, v);
    const auto & apply_step = *node.getStep();

    if (apply_step.getSubqueryType() != ApplyStep::SubqueryType::EXISTS)
    {
        return apply_ptr;
    }

    auto correlation = apply_step.getCorrelation();
    if (correlation.empty())
    {
        return apply_ptr;
    }

    PlanNodePtr input_ptr = apply_ptr->getChildren()[0];
    PlanNodePtr subquery_ptr = apply_ptr->getChildren()[1];

    PlanNodePtr source = subquery_ptr;

    std::optional<DecorrelationResult> result = Decorrelation::decorrelateFilters(source, correlation, *context);
    if (!result.has_value())
    {
        throw Exception(
            "Correlated Exists subquery de-correlation error, correlation filter not exists: ", ErrorCodes::REMOVE_SUBQUERY_ERROR);
    }

    DecorrelationResult & result_value = result.value();
    subquery_ptr = result_value.node;

    std::pair<Names, Names> key_pairs = result->buildJoinClause(input_ptr, subquery_ptr, correlation, context);

    std::vector<ConstASTPtr> filter = result->extractFilter();
    if (filter.empty())
    {
        // step 1 : projection correlation symbols of subquery part.
        Assignments right_correlation_assignments;
        NameToType right_correlation_name_to_type;
        for (const auto & column : subquery_ptr->getStep()->getOutputStream().header)
        {
            if (std::find(key_pairs.second.begin(), key_pairs.second.end(), column.name) != key_pairs.second.end())
            {
                Assignment assignment{column.name, std::make_shared<ASTIdentifier>(column.name)};
                right_correlation_assignments.emplace_back(assignment);
                right_correlation_name_to_type[column.name] = column.type;
            }
        }

        auto right_correlation_step = std::make_shared<ProjectionStep>(
            subquery_ptr->getStep()->getOutputStream(), right_correlation_assignments, right_correlation_name_to_type);
        auto right_correlation_node
            = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(right_correlation_step), PlanNodes{subquery_ptr});

        // step 2 : add distinct step, remove duplication.
        const auto & settings = context->getSettingsRef();
        UInt64 limit_for_distinct = 0;
        auto distinct_step = std::make_unique<DistinctStep>(
            right_correlation_node->getStep()->getOutputStream(),
            SizeLimits(settings.max_rows_in_distinct, settings.max_bytes_in_distinct, settings.distinct_overflow_mode),
            limit_for_distinct,
            key_pairs.second,
            true);
        auto distinct_node
            = std::make_shared<DistinctNode>(context->nextNodeId(), std::move(distinct_step), PlanNodes{right_correlation_node});

        // step 3 : add extra non-null symbol, default value = 1.
        Assignments non_null_assignments;
        NameToType non_null_name_to_type;
        String non_null_symbol = context->getSymbolAllocator()->newSymbol("build_side_non_null_symbol");
        Assignment non_null_assignment{non_null_symbol, std::make_shared<ASTLiteral>(1u)};
        non_null_assignments.emplace_back(non_null_assignment);
        non_null_name_to_type[non_null_symbol] = std::make_shared<DataTypeUInt8>();
        for (const auto & column : distinct_node->getStep()->getOutputStream().header)
        {
            Assignment assignment{column.name, std::make_shared<ASTIdentifier>(column.name)};
            non_null_assignments.emplace_back(assignment);
            non_null_name_to_type[column.name] = column.type;
        }

        auto non_null_step
            = std::make_shared<ProjectionStep>(subquery_ptr->getStep()->getOutputStream(), non_null_assignments, non_null_name_to_type);
        auto non_null_node = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(non_null_step), PlanNodes{distinct_node});

        // step 4 : use left join replace correlated apply.
        const PlanNodePtr & join_left = input_ptr;
        PlanNodePtr join_right = non_null_node;

        const DataStream & left_data_stream = join_left->getStep()->getOutputStream();
        const DataStream & right_data_stream = join_right->getStep()->getOutputStream();

        DataStreams streams = {left_data_stream, right_data_stream};
        auto left_header = left_data_stream.header;
        auto right_header = right_data_stream.header;
        NamesAndTypes output;
        for (const auto & item : left_header)
        {
            output.emplace_back(NameAndTypePair{item.name, item.type});
        }
        for (const auto & item : right_header)
        {
            output.emplace_back(NameAndTypePair{item.name, item.type});
        }

        auto join_step = std::make_shared<JoinStep>(
            streams,
            DataStream{.header = output},
            ASTTableJoin::Kind::Left,
            ASTTableJoin::Strictness::All,
            context->getSettingsRef().max_threads,
            context->getSettingsRef().optimize_read_in_order,
            key_pairs.first,
            key_pairs.second,
            std::vector<bool>{},
            PredicateConst::TRUE_VALUE,
            false,
            std::nullopt,
            ASOF::Inequality::GreaterOrEquals,
            DistributionType::UNKNOWN);
        PlanNodePtr join_node = std::make_shared<JoinNode>(context->nextNodeId(), std::move(join_step), PlanNodes{join_left, join_right});

        Assignments exists_assignments;
        NameToType exists_name_to_type;

        bool is_not_exist = false;
        if (const auto * literal = apply_step.getAssignment().second->as<ASTLiteral>())
        {
            UInt64 value = 1;
            literal->value.tryGet(value);
            if (!value)
            {
                is_not_exist = true;
            }
        }
        for (const auto & column : join_node->getStep()->getOutputStream().header)
        {
            // if predicate match, then non_null symbol equals to 1, else equals to 0.
            if (column.name == non_null_symbol)
            {
                ASTPtr coalesce
                    = makeASTFunction("coalesce", ASTs{std::make_shared<ASTIdentifier>(non_null_symbol), std::make_shared<ASTLiteral>(0)});
                ASTPtr cast = makeASTFunction("cast", coalesce, std::make_shared<ASTLiteral>("UInt8"));
                if (is_not_exist)
                {
                    cast = makeASTFunction("not", cast);
                }
                Assignment exists{apply_step.getAssignment().first, cast};
                exists_assignments.emplace_back(exists);
                exists_name_to_type[apply_step.getAssignment().first] = std::make_shared<DataTypeUInt8>();
            }
            else
            {
                Assignment assignment{column.name, std::make_shared<ASTIdentifier>(column.name)};
                exists_assignments.emplace_back(assignment);
                exists_name_to_type[column.name] = column.type;
            }
        }
        auto exists_step
            = std::make_shared<ProjectionStep>(join_node->getStep()->getOutputStream(), exists_assignments, exists_name_to_type);
        auto exists_node = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(exists_step), PlanNodes{join_node});
        return exists_node;
    }

    // for filter exists

    // step 1 : Assign unique id for left side
    String unique = context->getSymbolAllocator()->newSymbol("assign_unique_id_symbol");
    auto unique_id_step = std::make_unique<AssignUniqueIdStep>(input_ptr->getStep()->getOutputStream(), unique);
    auto unique_id_node = std::make_shared<AssignUniqueIdNode>(context->nextNodeId(), std::move(unique_id_step), PlanNodes{input_ptr});

    // step 2 : add extra non-null symbol, default value = 1.
    Assignments non_null_assignments;
    NameToType non_null_name_to_type;
    String non_null_symbol = context->getSymbolAllocator()->newSymbol("build_side_non_null_symbol");
    Assignment non_null_assignment{non_null_symbol, std::make_shared<ASTLiteral>(1u)};
    non_null_assignments.emplace_back(non_null_assignment);
    non_null_name_to_type[non_null_symbol] = std::make_shared<DataTypeUInt8>();
    for (const auto & column : subquery_ptr->getStep()->getOutputStream().header)
    {
        Assignment assignment{column.name, std::make_shared<ASTIdentifier>(column.name)};
        non_null_assignments.emplace_back(assignment);
        non_null_name_to_type[column.name] = column.type;
    }

    auto non_null_step
        = std::make_shared<ProjectionStep>(subquery_ptr->getStep()->getOutputStream(), non_null_assignments, non_null_name_to_type);
    auto non_null_node = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(non_null_step), PlanNodes{subquery_ptr});

    // step 3 : use left join replace correlated apply.
    const PlanNodePtr & join_left = unique_id_node;
    PlanNodePtr join_right = non_null_node;

    const DataStream & left_data_stream = join_left->getStep()->getOutputStream();
    const DataStream & right_data_stream = join_right->getStep()->getOutputStream();

    DataStreams streams = {left_data_stream, right_data_stream};
    auto left_header = left_data_stream.header;
    auto right_header = right_data_stream.header;
    NamesAndTypes output;
    for (const auto & item : left_header)
    {
        output.emplace_back(NameAndTypePair{item.name, item.type});
    }
    for (const auto & item : right_header)
    {
        output.emplace_back(NameAndTypePair{item.name, item.type});
    }

    auto join_step = std::make_shared<JoinStep>(
        streams,
        DataStream{.header = output},
        ASTTableJoin::Kind::Left,
        ASTTableJoin::Strictness::All,
        context->getSettingsRef().max_threads,
        context->getSettingsRef().optimize_read_in_order,
        key_pairs.first,
        key_pairs.second,
        std::vector<bool>{},
        PredicateConst::TRUE_VALUE,
        false,
        std::nullopt,
        ASOF::Inequality::GreaterOrEquals,
        DistributionType::UNKNOWN);
    PlanNodePtr join_node = std::make_shared<JoinNode>(context->nextNodeId(), std::move(join_step), PlanNodes{join_left, join_right});

    // step 4 : process extra filters.
    // Attention :
    // we will try to rewrite exists subquery to left join, and left join will only expand left side,
    // and will not reduce left side.
    // for these extra filters, they are used for determine whether the subquery is matched or not.
    // hence, if these filters match, non_null symbols is true, otherwise is false.
    Assignments filter_assignments;
    NameToType filter_name_to_type;
    for (const auto & column : join_node->getStep()->getOutputStream().header)
    {
        // if predicate match, then non_null symbol equals to 1, else equals to 0.
        if (column.name == non_null_symbol)
        {
            ASTPtr non_null_predicate
                = makeASTFunction("equals", std::make_shared<ASTIdentifier>(non_null_symbol), std::make_shared<ASTLiteral>(1u));
            filter.emplace_back(non_null_predicate);
            ASTPtr filter_predicate = PredicateUtils::combineConjuncts(filter);
            auto if_fun = makeASTFunction("if", filter_predicate, std::make_shared<ASTLiteral>(1u), std::make_shared<ASTLiteral>(Field()));
            Assignment if_assignment{non_null_symbol, if_fun};
            filter_assignments.emplace_back(if_assignment);
            filter_name_to_type[non_null_symbol] = std::make_shared<DataTypeUInt8>();
        }
        else
        {
            Assignment assignment{column.name, std::make_shared<ASTIdentifier>(column.name)};
            filter_assignments.emplace_back(assignment);
            filter_name_to_type[column.name] = column.type;
        }
    }
    auto filter_step = std::make_shared<ProjectionStep>(join_node->getStep()->getOutputStream(), filter_assignments, filter_name_to_type);
    auto filter_node = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(filter_step), PlanNodes{join_node});

    join_node = filter_node;

    Assignments remove_null_assignments;
    NameToType remove_null_name_to_type;
    for (const auto & column : join_node->getStep()->getOutputStream().header)
    {
        // if predicate match, then non_null symbol equals to 1, else equals to 0.
        if (column.name == non_null_symbol)
        {
            ASTPtr true_predicate
                = makeASTFunction("equals", std::make_shared<ASTIdentifier>(non_null_symbol), std::make_shared<ASTLiteral>(1u));
            ASTPtr true_value = std::make_shared<ASTLiteral>(1u);
            ASTPtr false_predicate
                = makeASTFunction("equals", std::make_shared<ASTIdentifier>(non_null_symbol), std::make_shared<ASTLiteral>(Field()));
            ASTPtr false_value = std::make_shared<ASTLiteral>(0u);
            ASTPtr else_value = std::make_shared<ASTLiteral>(0u);
            auto multi_if = makeASTFunction("multiIf", true_predicate, true_value, false_predicate, false_value, else_value);
            auto cast = makeASTFunction("cast", multi_if, std::make_shared<ASTLiteral>("UInt8"));
            Assignment remove_null{non_null_symbol, cast};
            remove_null_assignments.emplace_back(remove_null);
            remove_null_name_to_type[non_null_symbol] = std::make_shared<DataTypeUInt8>();
        }
        else
        {
            Assignment assignment{column.name, std::make_shared<ASTIdentifier>(column.name)};
            remove_null_assignments.emplace_back(assignment);
            remove_null_name_to_type[column.name] = column.type;
        }
    }
    auto remove_null_step
        = std::make_shared<ProjectionStep>(join_node->getStep()->getOutputStream(), remove_null_assignments, remove_null_name_to_type);
    auto remove_null_node = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(remove_null_step), PlanNodes{join_node});

    // step 5 : count non_null value
    Names keys;
    for (const auto & column : join_left->getStep()->getOutputStream().header)
    {
        keys.emplace_back(column.name);
    }

    DataTypes types{std::make_shared<DataTypeUInt8>()};
    Array parameters = Array();
    AggregateFunctionProperties properties;
    AggregateFunctionPtr agg_fun = AggregateFunctionFactory::instance().get("countIf", types, parameters, properties);
    AggregateDescription aggregate_desc;
    String count_non_null_value = context->getSymbolAllocator()->newSymbol("count_non_null_value");
    aggregate_desc.column_name = count_non_null_value;
    aggregate_desc.parameters = parameters;
    aggregate_desc.function = agg_fun;
    aggregate_desc.argument_names = Names{non_null_symbol};
    AggregateDescriptions aggregate_descs{aggregate_desc};

    auto count_non_null_step = std::make_shared<AggregatingStep>(
        remove_null_node->getStep()->getOutputStream(), keys, NameSet{}, aggregate_descs, GroupingSetsParamsList{}, true);
    auto count_non_null_node
        = std::make_shared<AggregatingNode>(context->nextNodeId(), std::move(count_non_null_step), PlanNodes{remove_null_node});

    // step 6 : compute exists symbol, i.g, exists = count_non_null_value > 0 ? true : false.
    Assignments exist_assignments;
    NameToType exist_name_to_type;
    for (const auto & column : count_non_null_node->getStep()->getOutputStream().header)
    {
        if (column.name == count_non_null_value)
        {
            ASTs arguments{std::make_shared<ASTIdentifier>(count_non_null_value), std::make_shared<ASTLiteral>(0u)};
            auto predicate = makeASTFunction("greater", arguments);
            if (const auto * literal = apply_step.getAssignment().second->as<ASTLiteral>())
            {
                UInt64 value = 1;
                literal->value.tryGet(value);
                if (!value)
                {
                    predicate = makeASTFunction("not", ASTs{predicate});
                }
            }
            Assignment exists{apply_step.getAssignment().first, predicate};
            exist_assignments.emplace_back(exists);
            exist_name_to_type[apply_step.getAssignment().first] = std::make_shared<DataTypeUInt8>();
        }
        else
        {
            Assignment assignment{column.name, std::make_shared<ASTIdentifier>(column.name)};
            exist_assignments.emplace_back(assignment);
            exist_name_to_type[column.name] = column.type;
        }
    }

    // Exists function has been rewritten into "exists" symbols
    // if exists values is true, then condition match, else not match.
    auto exists_step
        = std::make_shared<ProjectionStep>(count_non_null_node->getStep()->getOutputStream(), exist_assignments, exist_name_to_type);
    auto exists_node = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(exists_step), PlanNodes{count_non_null_node});
    return exists_node;
}

void RemoveUnCorrelatedExistsSubquery::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    UnCorrelatedExistsSubqueryVisitor visitor{context, plan.getCTEInfo()};
    Void v;
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, v);
    plan.update(result);
}

PlanNodePtr UnCorrelatedExistsSubqueryVisitor::visitApplyNode(ApplyNode & node, Void & v)
{
    PlanNodePtr apply_ptr = visitPlanNode(node, v);
    const auto & apply_step = *node.getStep();

    if (apply_step.getSubqueryType() != ApplyStep::SubqueryType::EXISTS)
    {
        return apply_ptr;
    }

    auto correlation = apply_step.getCorrelation();
    if (!correlation.empty())
    {
        return apply_ptr;
    }

    PlanNodePtr input_ptr = apply_ptr->getChildren()[0];
    PlanNodePtr subquery_ptr = apply_ptr->getChildren()[1];

    // step 1 : count the row number of subquery
    Names keys;
    DataTypes types;
    Array parameters = Array();
    AggregateFunctionProperties properties;
    AggregateFunctionPtr agg_fun = AggregateFunctionFactory::instance().get("count", types, parameters, properties);
    AggregateDescription aggregate_desc;
    aggregate_desc.column_name = context->getSymbolAllocator()->newSymbol("count");
    aggregate_desc.parameters = parameters;
    aggregate_desc.function = agg_fun;
    AggregateDescriptions aggregate_descs{aggregate_desc};
    auto count_subquery_step = std::make_shared<AggregatingStep>(
        subquery_ptr->getStep()->getOutputStream(), keys, NameSet{}, aggregate_descs, GroupingSetsParamsList{}, true);
    auto count_subquery_node
        = std::make_shared<AggregatingNode>(context->nextNodeId(), std::move(count_subquery_step), PlanNodes{subquery_ptr});

    // step 2 : compute exists symbol, i.g, exists = count > 0 ? true : false.
    Assignments assignments;
    NameToType name_to_type;
    ASTPtr symbol = std::make_shared<ASTIdentifier>(aggregate_desc.column_name);
    ASTPtr value = std::make_shared<ASTLiteral>(0u);

    Assignment exists_symbol{apply_step.getAssignment().first, makeASTFunction("greater", ASTs{symbol, value})};
    assignments.emplace_back(exists_symbol);
    name_to_type[apply_step.getAssignment().first] = std::make_shared<DataTypeUInt8>();
    PlanNodes expression_children{count_subquery_node};
    auto project_exists_symbol_step
        = std::make_shared<ProjectionStep>(count_subquery_node->getStep()->getOutputStream(), assignments, name_to_type);
    auto project_exists_symbol_node
        = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(project_exists_symbol_step), expression_children);

    if (context->getSettingsRef().enable_execute_uncorrelated_subquery)
    {
        const Assignment & apply_assignment = apply_step.getAssignment();
        String sub_query_column_name = apply_assignment.first;
        DataTypePtr sub_query_column_type = apply_step.getAssignmentDataType();

        int rule_id = context->getRuleId();
        String sub_query_id = std::to_string(context->incAndGetSubQueryId());
        String subquery_name_prefix = std::to_string(rule_id) + "_RemoveUnCorrelatedExistsSubquery_ExecuteSubQuery_" + sub_query_id + "_";

        context->setExecuteSubQueryPath(subquery_name_prefix);

        // set a different query id for sub query.
        String query_id = context->getCurrentQueryId();
        context->setCurrentQueryId(query_id + "_sub_query_" + sub_query_id);

        SelectQueryOptions sub_query_options;
        InterpreterSelectQueryUseOptimizer interpreter{project_exists_symbol_node, cte_helper.getCTEInfo(), context, sub_query_options};
        BlockIO sub_query_result = interpreter.execute();

        context->removeExecuteSubQueryPath();
        context->setCurrentQueryId(query_id);
        context->setRuleId(rule_id);

        BlockInputStreamPtr sub_query_block_stream = sub_query_result.getInputStream();
        Block block = sub_query_block_stream->read();

        size_t rows = block.rows();
        Utils::checkArgument(rows == 1, "Exists sub-query must return single row");

        const auto & sub_query_column = *(block.getByName(sub_query_column_name).column);

        ASTPtr sub_query_column_value = std::make_shared<ASTLiteral>(sub_query_column[0]);

        // create a new projection step, include exists sub query result.
        Assignments assignments_exists;
        NameToType types_new;

        // add input columns
        for (const auto & column : input_ptr->getStep()->getOutputStream().header)
        {
            assignments_exists.emplace_back(column.name, std::make_shared<ASTIdentifier>(column.name));
            types_new[column.name] = column.type;
        }

        // add sub query column
        assignments_exists.emplace_back(sub_query_column_name, sub_query_column_value);
        types_new[sub_query_column_name] = sub_query_column_type;

        auto step_exists = std::make_shared<ProjectionStep>(input_ptr->getStep()->getOutputStream(), assignments_exists, types_new);
        return PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(step_exists), PlanNodes{input_ptr});
    }

    const DataStream & left_data_stream = input_ptr->getStep()->getOutputStream();
    const DataStream & right_data_stream = project_exists_symbol_node->getStep()->getOutputStream();

    DataStreams streams = {left_data_stream, right_data_stream};

    NamesAndTypes output;
    for (const auto & item : left_data_stream.header)
    {
        output.emplace_back(NameAndTypePair{item.name, item.type});
    }
    for (const auto & item : right_data_stream.header)
    {
        output.emplace_back(NameAndTypePair{item.name, item.type});
    }

    // step 3 : cross join, join rights side is a scalar value. (true/false)
    auto join_step = std::make_shared<JoinStep>(
        std::move(streams),
        DataStream{.header = std::move(output)},
        ASTTableJoin::Kind::Cross,
        ASTTableJoin::Strictness::Unspecified,
        context->getSettingsRef().max_threads,
        context->getSettingsRef().optimize_read_in_order,
        Names{},
        Names{},
        std::vector<bool>{},
        PredicateConst::TRUE_VALUE,
        false,
        std::nullopt,
        ASOF::Inequality::GreaterOrEquals,
        DistributionType::UNKNOWN);

    // Exists function has been rewritten into "exists" symbols
    // if exists values is true, then condition match, else not match.
    auto join_node
        = std::make_shared<JoinNode>(context->nextNodeId(), std::move(join_step), PlanNodes{input_ptr, project_exists_symbol_node});


    if (const auto * literal = apply_step.getAssignment().second->as<ASTLiteral>())
    {
        UInt64 literal_value = 1;
        literal->value.tryGet(literal_value);
        if (!literal_value)
        {
            Assignments exists_assignments;
            NameToType exists_name_to_type;
            for (const auto & column : join_node->getStep()->getOutputStream().header)
            {
                // if predicate match, then non_null symbol equals to 1, else equals to 0.
                if (column.name == apply_step.getAssignment().first)
                {
                    ASTPtr cast = makeASTFunction("not", std::make_shared<ASTIdentifier>(apply_step.getAssignment().first));
                    Assignment exists{apply_step.getAssignment().first, cast};
                    exists_assignments.emplace_back(exists);
                    exists_name_to_type[apply_step.getAssignment().first] = std::make_shared<DataTypeUInt8>();
                }
                else
                {
                    Assignment assignment{column.name, std::make_shared<ASTIdentifier>(column.name)};
                    exists_assignments.emplace_back(assignment);
                    exists_name_to_type[column.name] = column.type;
                }
            }
            auto exists_step
                = std::make_shared<ProjectionStep>(join_node->getStep()->getOutputStream(), exists_assignments, exists_name_to_type);
            auto exists_node = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(exists_step), PlanNodes{join_node});
            return exists_node;
        }
    }

    return join_node;
}

void RemoveUnCorrelatedQuantifiedComparisonSubquery::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    UnCorrelatedQuantifiedComparisonSubqueryVisitor visitor{context, plan.getCTEInfo()};
    Void v;
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, v);
    plan.update(result);
}

PlanNodePtr UnCorrelatedQuantifiedComparisonSubqueryVisitor::visitApplyNode(ApplyNode & node, Void & v)
{
    PlanNodePtr apply_ptr = visitPlanNode(node, v);
    auto & apply_step = *node.getStep();

    if (apply_step.getSubqueryType() != ApplyStep::SubqueryType::QUANTIFIED_COMPARISON)
    {
        return apply_ptr;
    }

    auto & correlation = apply_step.getCorrelation();
    if (!correlation.empty())
    {
        return apply_ptr;
    }

    PlanNodePtr input_ptr = apply_ptr->getChildren()[0];
    PlanNodePtr subquery_ptr = apply_ptr->getChildren()[1];

    const DataStream & left_data_stream = input_ptr->getStep()->getOutputStream();
    const DataStream & right_data_stream = subquery_ptr->getStep()->getOutputStream();

    auto & qc_assignment = apply_step.getAssignment();
    auto & qc_ast = qc_assignment.second->as<ASTQuantifiedComparison &>();
    ASTIdentifier & qc_child_left = qc_ast.children[0]->as<ASTIdentifier &>();
    ASTIdentifier & qc_child_right = qc_ast.children[1]->as<ASTIdentifier &>();
    const QuantifierType & quantifier_type = qc_ast.quantifier_type;
    Names qc_left{qc_child_left.name()};
    Names qc_right{qc_child_right.name()};

    //step1 :
    //// build aggregation descriptions
    String min_value, max_value, count_all_value, count_non_null_value;
    AggregateDescriptions aggregate_descriptions;

    makeAggDescriptionsMinMaxCountCount2(
        aggregate_descriptions, context, min_value, max_value, count_all_value, count_non_null_value, subquery_ptr, qc_right);

    Names keys;
    auto agg_step
        = std::make_shared<AggregatingStep>(right_data_stream, keys, NameSet{}, aggregate_descriptions, GroupingSetsParamsList{}, true);
    auto agg_node = std::make_shared<AggregatingNode>(context->nextNodeId(), std::move(agg_step), PlanNodes{subquery_ptr});

    //step2 : construct a join node to replace apply node
    const DataStream & agg_data_stream = agg_node->getStep()->getOutputStream();
    DataStreams streams = {left_data_stream, agg_data_stream};
    auto left_header = left_data_stream.header;
    auto right_header = agg_data_stream.header;
    NamesAndTypes output;
    for (const auto & item : left_header)
    {
        output.emplace_back(NameAndTypePair{item.name, item.type});
    }
    for (const auto & item : right_header)
    {
        output.emplace_back(NameAndTypePair{item.name, item.type});
    }

    auto join_step = std::make_shared<JoinStep>(
        streams,
        DataStream{.header = output},
        ASTTableJoin::Kind::Cross,
        ASTTableJoin::Strictness::Unspecified,
        context->getSettingsRef().max_threads,
        context->getSettingsRef().optimize_read_in_order,
        Names{},
        Names{},
        std::vector<bool>{},
        PredicateConst::TRUE_VALUE,
        false,
        std::nullopt,
        ASOF::Inequality::GreaterOrEquals,
        DistributionType::UNKNOWN);
    PlanNodePtr join_node = std::make_shared<JoinNode>(context->nextNodeId(), std::move(join_step), PlanNodes{input_ptr, agg_node});

    // step 3: compute function result, project it as a bool symbol.
    // A > ALL B  => if (isNull(a), NULL, multiIf (count_non_null_value = 0, 1, count_all_value = count_non_null_value, if (a > max_value, 1, 0), NULL));
    // A >= ALL B  => if (isNull(a), NULL, multiIf (count_non_null_value = 0, 1, count_all_value = count_non_null_value, if (a >= max_value, 1, 0), NULL));
    // A > ANY B  => if (isNull(a), NULL, multiIf (count_non_null_value = 0, 0, count_all_value = count_non_null_value, if (a > min_value, 1, 0), NULL));
    // A >= ANY B  => if (isNull(a), NULL, multiIf (count_non_null_value = 0, 0, count_all_value = count_non_null_value, if (a >= min_value, 1, 0), NULL));

    // A < ALL B  => if (isNull(a), NULL, multiIf (count_non_null_value = 0, 1, count_all_value = count_non_null_value, if (a < min_value, 1, 0), NULL));
    // A <= ALL B  => if (isNull(a), NULL, multiIf (count_non_null_value = 0, 1, count_all_value = count_non_null_value, if (a <= min_value, 1, 0), NULL));
    // A < ANY B  => if (isNull(a), NULL, multiIf (count_non_null_value = 0, 0, count_all_value = count_non_null_value, if (a < max_value, 1, 0), NULL));
    // A <= ANY B  => if (isNull(a), NULL, multiIf (count_non_null_value = 0, 0, count_all_value = count_non_null_value, if (a <= max_value, 1, 0), NULL));

    // A = ALL B  => if(isNull(a), NULL, multiIf (count_non_null_value = 0, 1, count_all_value = count_non_null_value, if ((min_value = max_value) AND (a = max_value), 1, 0), NULL));
    // A != ANY B <=> !(A = ALL B)
    Assignments qc_assignments;
    NameToType qc_name_to_type;
    for (auto & column : input_ptr->getStep()->getOutputStream().header)
    {
        Assignment join_column{column.name, std::make_shared<ASTIdentifier>(column.name)};
        qc_assignments.emplace_back(join_column);
        qc_name_to_type[column.name] = column.type;
    }

    ASTPtr empty_set_result = getEmptySetResult(quantifier_type);

    ASTPtr comparison_with_extreme_value = getBoundComparisons(qc_child_left.name(), min_value, max_value, qc_ast);

    //if the number of right table is 0(countAllValue == 0), then output 'emptySetResult'
    ASTPtr right_num_is_zero
        = makeASTFunction("equals", std::make_shared<ASTIdentifier>(count_all_value), std::make_shared<ASTLiteral>(0u));

    //If right TABLE contains the values NULL ,e.g.({100.00, NULL, 300.00}), the expression is UNKNOWN: when NULLs are involved, ALL/ANY is UNKNOWN.
    ASTPtr not_involve_null = makeASTFunction(
        "equals", std::make_shared<ASTIdentifier>(count_all_value), std::make_shared<ASTIdentifier>(count_non_null_value));

    ASTPtr multi_if = makeASTFunction(
        "multiIf",
        right_num_is_zero,
        empty_set_result,
        not_involve_null,
        comparison_with_extreme_value,
        std::make_shared<ASTLiteral>(Null{}));

    // if left is NULL, then result is null
    ASTPtr left_is_null = makeASTFunction("isNull", std::make_shared<ASTIdentifier>(qc_child_left));

    Assignment qc_ass{
        apply_step.getAssignment().first, makeASTFunction("if", left_is_null, std::make_shared<ASTLiteral>(Null{}), multi_if)};
    qc_assignments.emplace_back(qc_ass);
    qc_name_to_type[apply_step.getAssignment().first] = std::make_shared<DataTypeUInt8>();

    auto project_step = std::make_shared<ProjectionStep>(join_node->getStep()->getOutputStream(), qc_assignments, qc_name_to_type);
    auto project_node = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(project_step), PlanNodes{join_node});
    return project_node;
}

void RemoveCorrelatedQuantifiedComparisonSubquery::rewrite(QueryPlan & plan, ContextMutablePtr context) const
{
    CorrelatedQuantifiedComparisonSubqueryVisitor visitor{context, plan.getCTEInfo()};
    Void v;
    auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, v);
    plan.update(result);
}

PlanNodePtr CorrelatedQuantifiedComparisonSubqueryVisitor::visitApplyNode(ApplyNode & node, Void & v)
{
    PlanNodePtr apply_ptr = visitPlanNode(node, v);
    auto & apply_step = *node.getStep();

    if (apply_step.getSubqueryType() != ApplyStep::SubqueryType::QUANTIFIED_COMPARISON)
    {
        return apply_ptr;
    }

    auto correlation = apply_step.getCorrelation();
    if (correlation.empty())
    {
        return apply_ptr;
    }

    PlanNodePtr input_ptr = apply_ptr->getChildren()[0];
    PlanNodePtr subquery_ptr = apply_ptr->getChildren()[1];

    std::optional<DecorrelationResult> result = Decorrelation::decorrelateFilters(subquery_ptr, correlation, *context);
    if (!result.has_value())
    {
        throw Exception(
            "Correlated quantified comparison subquery de-correlation error, correlation filter not exists: " + printVector(correlation),
            ErrorCodes::REMOVE_SUBQUERY_ERROR);
    }

    DecorrelationResult & result_value = result.value();
    PlanNodePtr decorrelation_source = result_value.node;
    std::pair<Names, Names> correlation_predicate = result_value.buildJoinClause(input_ptr, decorrelation_source, correlation, context);

    //step1 : Assign unique id symbol for join left
    String unique = context->getSymbolAllocator()->newSymbol("assign_unique_id_symbol");
    auto unique_id_step = std::make_unique<AssignUniqueIdStep>(input_ptr->getStep()->getOutputStream(), unique);
    auto unique_id_node = std::make_shared<AssignUniqueIdNode>(context->nextNodeId(), std::move(unique_id_step), PlanNodes{input_ptr});

    const DataStream & decorrelation_output = decorrelation_source->getStep()->getOutputStream();

    // step 2 : Assign non null symbol with default value 1.
    Assignments assignments;
    NameToType name_to_type;
    for (auto & column : decorrelation_output.header)
    {
        Assignment ass{column.name, std::make_shared<ASTIdentifier>(column.name)};
        assignments.emplace_back(ass);
        name_to_type[column.name] = column.type;
    }

    String non_null = context->getSymbolAllocator()->newSymbol("build_side_non_null_symbol");
    ASTPtr value = std::make_shared<ASTLiteral>(1u);
    Assignment ass{non_null, value};
    assignments.emplace_back(ass);
    name_to_type[non_null] = std::make_shared<DataTypeUInt8>();

    auto expression_step = std::make_shared<ProjectionStep>(decorrelation_source->getStep()->getOutputStream(), assignments, name_to_type);
    auto expression_node
        = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(expression_step), PlanNodes{decorrelation_source});

    // step 3 : construct a JoinNode to replace ApplyNode
    const PlanNodePtr & join_left = unique_id_node;
    const PlanNodePtr & join_right = expression_node;
    const DataStream & left_data_stream = join_left->getStep()->getOutputStream();
    const DataStream & right_data_stream = join_right->getStep()->getOutputStream();
    DataStreams streams = {left_data_stream, right_data_stream};
    auto left_header = left_data_stream.header;
    auto right_header = right_data_stream.header;
    NamesAndTypes output;
    Names left_names;
    Names right_names;
    for (const auto & item : left_header)
    {
        output.emplace_back(NameAndTypePair{item.name, item.type});
        left_names.emplace_back(item.name);
    }
    for (const auto & item : right_header)
    {
        output.emplace_back(NameAndTypePair{item.name, item.type});
        right_names.emplace_back(item.name);
    }

    auto & qc_assignment = apply_step.getAssignment();
    auto & quantified_comparison = qc_assignment.second->as<ASTQuantifiedComparison &>();
    ASTIdentifier & qc_child_left = quantified_comparison.children[0]->as<ASTIdentifier &>();
    ASTIdentifier & qc_child_right = quantified_comparison.children[1]->as<ASTIdentifier &>();

    Names qc_left{qc_child_left.name()};
    Names qc_right{qc_child_right.name()};

    String probe_symbol = qc_left[0];
    String build_symbol = qc_right[0];

    std::vector<ConstASTPtr> filter = result->extractFilter();
    ASTPtr join_filter = PredicateUtils::combineConjuncts(filter);

    auto join_step = std::make_shared<JoinStep>(
        streams,
        DataStream{.header = output},
        ASTTableJoin::Kind::Left,
        ASTTableJoin::Strictness::All,
        context->getSettingsRef().max_threads,
        context->getSettingsRef().optimize_read_in_order,
        correlation_predicate.first,
        correlation_predicate.second,
        std::vector<bool>{},
        join_filter,
        false,
        std::nullopt,
        ASOF::Inequality::GreaterOrEquals,
        DistributionType::UNKNOWN);
    PlanNodePtr join_node = std::make_shared<JoinNode>(context->nextNodeId(), std::move(join_step), PlanNodes{join_left, join_right});

    // step4 : project min_value, max_value, count_all_value, count_non_null_value, whether_num_of_matching_join_result_is_zero which group by left_header's colomns
    String min_value, max_value, count_all_value, count_non_null_value;
    AggregateDescriptions aggregate_descriptions;

    makeAggDescriptionsMinMaxCountCount2(
        aggregate_descriptions, context, min_value, max_value, count_all_value, count_non_null_value, subquery_ptr, qc_right);

    AggregateFunctionProperties properties;
    String num_of_matching_is_zero_symbol = context->getSymbolAllocator()->newSymbol("whether_num_of_matching_join_result_is_zero");
    AggregateDescription count_if_agg_desc
        = {.function = AggregateFunctionFactory::instance().get("count", {std::make_shared<DataTypeUInt8>()}, Array(), properties),
           .parameters = Array(),
           .argument_names = Names{non_null},
           .column_name = num_of_matching_is_zero_symbol};
    aggregate_descriptions.emplace_back(count_if_agg_desc);

    Names keys;
    for (const auto & item : left_header)
    {
        keys.emplace_back(item.name);
    }

    auto count_step = std::make_shared<AggregatingStep>(
        join_node->getStep()->getOutputStream(), keys, NameSet{}, aggregate_descriptions, GroupingSetsParamsList{}, true);
    auto count_node = std::make_shared<AggregatingNode>(context->nextNodeId(), std::move(count_step), PlanNodes{join_node});

    // step5 : project match values;
    Assignments qc_assignments;
    NameToType qc_name_to_type;
    for (auto & column : input_ptr->getStep()->getOutputStream().header)
    {
        Assignment join_column{column.name, std::make_shared<ASTIdentifier>(column.name)};
        qc_assignments.emplace_back(join_column);
        qc_name_to_type[column.name] = column.type;
    }
    ASTPtr empty_set_result = getEmptySetResult(quantified_comparison.quantifier_type);

    ASTPtr comparison_with_extreme_value = getBoundComparisons(qc_child_left.name(), min_value, max_value, quantified_comparison);

    //if the number of right table is 0(num_of_matching_is_zero_symbol == 0), then output 'emptySetResult'
    ASTPtr right_num_is_zero
        = makeASTFunction("equals", std::make_shared<ASTIdentifier>(num_of_matching_is_zero_symbol), std::make_shared<ASTLiteral>(0u));

    //If right TABLE contains the values NULL ,e.g.({100.00, NULL, 300.00}), the expression is UNKNOWN: when NULLs are involved, ALL/ANY is UNKNOWN.
    ASTPtr not_involve_null = makeASTFunction(
        "equals", std::make_shared<ASTIdentifier>(count_all_value), std::make_shared<ASTIdentifier>(count_non_null_value));

    ASTPtr multi_if = makeASTFunction(
        "multiIf",
        right_num_is_zero,
        empty_set_result,
        not_involve_null,
        comparison_with_extreme_value,
        std::make_shared<ASTLiteral>(Null{}));

    // if left is NULL, then result is null
    ASTPtr left_is_null = makeASTFunction("isNull", std::make_shared<ASTIdentifier>(qc_child_left));

    Assignment qc_ass{
        apply_step.getAssignment().first, makeASTFunction("if", left_is_null, std::make_shared<ASTLiteral>(Null{}), multi_if)};
    qc_assignments.emplace_back(qc_ass);
    qc_name_to_type[apply_step.getAssignment().first] = std::make_shared<DataTypeUInt8>();

    auto project_step = std::make_shared<ProjectionStep>(count_node->getStep()->getOutputStream(), qc_assignments, qc_name_to_type);
    auto project_node = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(project_step), PlanNodes{count_node});
    return project_node;
}

ASTPtr getEmptySetResult(const QuantifierType & quantifier_type)
{
    if (quantifier_type == QuantifierType::ALL)
    {
        return std::make_shared<ASTLiteral>(1u);
    }
    return std::make_shared<ASTLiteral>(0u);
}

ASTPtr getBoundComparisons(const String & left, const String & minValue, const String & maxValue, const ASTQuantifiedComparison & qc_ast)
{
    if (qc_ast.comparator == "equals" && qc_ast.quantifier_type == QuantifierType::ALL)
    {
        // A = ALL B <=> min B = max B && A = min B
        ConstASTs combine_filters{
            makeASTFunction("equals", std::make_shared<ASTIdentifier>(minValue), std::make_shared<ASTIdentifier>(maxValue)),
            makeASTFunction("equals", std::make_shared<ASTIdentifier>(left), std::make_shared<ASTIdentifier>(maxValue))};
        return PredicateUtils::combineConjuncts(combine_filters);
    }
    else if (qc_ast.comparator == "notEquals" && qc_ast.quantifier_type == QuantifierType::ANY)
    {
        // A != ANY B <=> !(A = ALL B)
        // A <> ANY B <=> min B <> max B || A <> min B <=> !(min B = max B && A = min B) <=> !(A = ALL B)
        // "A <> ANY B" is equivalent to "NOT (A = ALL B)" so add a rewrite for the initial quantifiedComparison to notAll
        ConstASTs combine_filters{
            makeASTFunction("equals", std::make_shared<ASTIdentifier>(minValue), std::make_shared<ASTIdentifier>(maxValue)),
            makeASTFunction("equals", std::make_shared<ASTIdentifier>(left), std::make_shared<ASTIdentifier>(maxValue))};
        return makeASTFunction("not", PredicateUtils::combineConjuncts(combine_filters));
    }
    // A < ALL B <=> A < min B
    // A > ALL B <=> A > max B
    // A < ANY B <=> A < max B
    // A > ANY B <=> A > min B
    const String & boundValue = shouldCompareValueWithLowerBound(qc_ast) ? minValue : maxValue;
    return makeASTFunction(qc_ast.comparator, std::make_shared<ASTIdentifier>(left), std::make_shared<ASTIdentifier>(boundValue));
}

bool shouldCompareValueWithLowerBound(const ASTQuantifiedComparison & qc_ast)
{
    bool is_all = qc_ast.quantifier_type == QuantifierType::ALL;
    bool is_less = qc_ast.comparator == "less" || qc_ast.comparator == "lessOrEquals";
    return is_all == is_less;
}

void makeAggDescriptionsMinMaxCountCount2(
    AggregateDescriptions & aggregate_descriptions,
    ContextMutablePtr & context,
    String & min_value,
    String & max_value,
    String & count_all_value,
    String & count_non_null_value,
    PlanNodePtr & subquery_ptr,
    Names & qc_right)
{
    min_value = context->getSymbolAllocator()->newSymbol("min_value");
    max_value = context->getSymbolAllocator()->newSymbol("max_value");
    count_all_value = context->getSymbolAllocator()->newSymbol("count_all_value");
    count_non_null_value = context->getSymbolAllocator()->newSymbol("count_non_null_value");
    DataTypes argument_types = {subquery_ptr->getOutputNamesToTypes()[qc_right[0]]};

    AggregateFunctionProperties properties;
    AggregateDescription min_agg_desc
        = {.function = AggregateFunctionFactory::instance().get("min", argument_types, Array(), properties),
           .parameters = Array(),
           .argument_names = qc_right,
           .column_name = min_value};
    AggregateDescription max_agg_desc
        = {.function = AggregateFunctionFactory::instance().get("max", argument_types, Array(), properties),
           .parameters = Array(),
           .argument_names = qc_right,
           .column_name = max_value};
    AggregateDescription count_all_value_agg_desc
        = {.function = AggregateFunctionFactory::instance().get("count", {}, Array(), properties),
           .parameters = Array(),
           .argument_names = {},
           .column_name = count_all_value};
    AggregateDescription count_non_null_value_agg_desc
        = {.function = AggregateFunctionFactory::instance().get("count", argument_types, Array(), properties),
           .parameters = Array(),
           .argument_names = qc_right,
           .column_name = count_non_null_value};
    aggregate_descriptions.emplace_back(min_agg_desc);
    aggregate_descriptions.emplace_back(max_agg_desc);
    aggregate_descriptions.emplace_back(count_all_value_agg_desc);
    aggregate_descriptions.emplace_back(count_non_null_value_agg_desc);
}


ConstRefPatternPtr UnnestingWithWindow::getPattern() const
{
    static NameSet agg_white_list{"count", "sum", "avg", "min", "max"};
    static auto pattern = Patterns::filter()
        .with(Patterns::apply()
                  .matchingStep<ApplyStep>([](const ApplyStep & s) {
                      return s.getSubqueryType() == ApplyStep::SubqueryType::SCALAR && !s.getCorrelation().empty();
                  })
                  .with(Patterns::any(), Patterns::aggregating().matchingStep<AggregatingStep>([](const AggregatingStep & agg) {
                      return agg.getAggregates().size() == 1 && agg_white_list.count(agg.getAggregates()[0].function->getName());
                  })))
        .result();
    return pattern;
}

PlanNodes collectPlanNodes(PlanNodePtr node)
{
    PlanNodes result;
    std::queue<PlanNodePtr> q;
    q.push(node);
    while (!q.empty())
    {
        auto item = q.front();
        result.emplace_back(item);
        for (auto & child : item->getChildren())
        {
            q.push(child);
        }
        q.pop();
    }
    return result;
}

TransformResult UnnestingWithWindow::transformImpl(PlanNodePtr filter_node, const Captures &, RuleContext & rule_context)
{
        auto apply_node = filter_node->getChildren()[0];
    auto apply_step = dynamic_cast<ApplyNode *>(apply_node.get())->getStep();
    auto left_node = apply_node->getChildren()[0];
    auto right_node = apply_node->getChildren()[1];
    AggregatingStep * agg_step = nullptr;
    ProjectionStep * proj_step = nullptr;
    if (right_node->getStep()->getType() == IQueryPlanStep::Type::Aggregating)
    {
        agg_step = dynamic_cast<AggregatingNode *>(right_node.get())->getStep().get();
    }
    else
    {
        proj_step = dynamic_cast<ProjectionNode *>(right_node.get())->getStep().get();
        right_node = right_node->getChildren()[0];
        agg_step = dynamic_cast<AggregatingNode *>(right_node.get())->getStep().get();
    }
    auto left_nodes = collectPlanNodes(left_node);
    auto right_nodes = collectPlanNodes(right_node->getChildren()[0]);

    // todo support more types
    std::set<IQueryPlanStep::Type> left_support_step_types{
        IQueryPlanStep::Type::Projection,
        IQueryPlanStep::Type::TableScan,
        IQueryPlanStep::Type::Join,
    };
    std::set<IQueryPlanStep::Type> right_support_step_types{
        IQueryPlanStep::Type::TableScan,
        IQueryPlanStep::Type::Join,
        IQueryPlanStep::Type::Filter,
    };


    std::unordered_map<StorageID, TableScanStep *> outer_id_to_plan_node;
    std::vector<StorageID> left_tables;
    for (const auto & node : left_nodes)
    {
        // check children type
        auto step_type = node->getStep()->getType();
        if (!left_support_step_types.count(step_type))
            return {};
        if (step_type == IQueryPlanStep::Type::Join)
        {
            // todo support more join type (inner)
            auto join_step = dynamic_cast<JoinNode *>(node.get())->getStep();
            if (!join_step->isCrossJoin())
                return {};
        }
        if (step_type == IQueryPlanStep::Type::TableScan)
        {
            auto table_scan = dynamic_cast<TableScanNode *>(node.get())->getStep();
            left_tables.emplace_back(table_scan->getStorageID());
            outer_id_to_plan_node[table_scan->getStorageID()] = table_scan.get();
        }
    }

    std::unordered_map<StorageID, TableScanStep *> sub_id_to_plan_node;
    std::vector<StorageID> right_tables;
    size_t filter_count = 0;
    FilterStep * sub_filter_step = nullptr;
    for (const auto & node : right_nodes)
    {
        // check children type
        auto step_type = node->getStep()->getType();
        if (!right_support_step_types.count(step_type))
            return {};
        if (step_type == IQueryPlanStep::Type::Join)
        {
            // todo support more join type (inner)
            auto join_step = dynamic_cast<JoinNode *>(node.get())->getStep();
            if (!join_step->isCrossJoin())
                return {};
        }
        if (step_type == IQueryPlanStep::Type::TableScan)
        {
            auto table_scan = dynamic_cast<TableScanNode *>(node.get())->getStep();
            right_tables.emplace_back(table_scan->getStorageID());
            sub_id_to_plan_node[table_scan->getStorageID()] = table_scan.get();
        }
        if (step_type == IQueryPlanStep::Type::Filter)
        {
            filter_count++;
            sub_filter_step = dynamic_cast<FilterNode *>(node.get())->getStep().get();
            // todo
            // subquery only one filter
            if (filter_count > 1)
            {
                return {};
            }
        }
    }

    std::unordered_set<StorageID> left_table_set{left_tables.begin(), left_tables.end()};
    std::unordered_set<StorageID> right_table_set{right_tables.begin(), right_tables.end()};
    if (left_table_set.size() != left_tables.size() || right_table_set.size() != right_tables.size()
        || left_tables.size() != right_tables.size() + 1)
    {
        return {};
    }

    for (const auto & item : right_tables)
    {
        left_table_set.erase(item);
    }

    auto * correlation_table_step = outer_id_to_plan_node[*left_table_set.begin()];
    auto output_names = correlation_table_step->getTableOutputStream().header.getNameSet();
    for (const auto & col : apply_step->getCorrelation())
    {
        if (!output_names.count(col))
            return {};
    }

    auto filter_step = dynamic_cast<FilterNode *>(filter_node.get())->getStep();
    auto filter_ast = filter_step->getFilter();
    auto outer_conjuncts = PredicateUtils::extractConjuncts(filter_ast);

    // 1. remove correlation predicate
    auto correlation = apply_step->getCorrelation();
    std::optional<DecorrelationResult> result
        = Decorrelation::decorrelateFilters(right_node->getChildren()[0], correlation, *rule_context.context);
    if (!result.has_value())
    {
        throw Exception(
            "Correlated Scalar subquery de-correlation error, correlation filter not exists: ", ErrorCodes::REMOVE_SUBQUERY_ERROR);
    }

    DecorrelationResult & result_value = result.value();
    PlanNodePtr join_right = result_value.node;
    std::pair<Names, Names> key_pairs = result_value.buildJoinClause(left_node, join_right, correlation, rule_context.context);

    auto extract_symbol_mapping = [](const std::unordered_map<StorageID, TableScanStep *> & id_to_plan_node) {
        ConstASTMap symbol_mapping;
        NameToNameMap col_to_symbol;
        for (const auto & item : id_to_plan_node)
        {
            for (const auto & alias : item.second->getColumnAlias())
            {
                ASTPtr a = std::make_shared<ASTIdentifier>(alias.second);
                auto col = ConstHashAST::make(std::make_shared<ASTIdentifier>(alias.first));
                symbol_mapping.emplace(a, col);
                col_to_symbol.emplace(alias.first, alias.second);
            }
        }
        return std::make_pair(symbol_mapping, col_to_symbol);
    };

    auto [outer_symbol_mapping, outer_col_to_symbol] = extract_symbol_mapping(outer_id_to_plan_node);
    auto [sub_symbol_mapping, sub_col_to_symbol] = extract_symbol_mapping(sub_id_to_plan_node);

    NameToNameMap sub_to_outer;
    for (auto & item : sub_col_to_symbol)
        if (outer_col_to_symbol.contains(item.first))
        {
            sub_to_outer[item.second] = outer_col_to_symbol[item.first];
        }


    EqualityASTSet distinct{outer_conjuncts.begin(), outer_conjuncts.end()};
    for (size_t i = 0; i < key_pairs.first.size(); i++)
    {
        auto left = sub_to_outer.contains(key_pairs.first[i]) ? sub_to_outer[key_pairs.first[i]] : key_pairs.first[i];
        auto right = sub_to_outer.contains(key_pairs.second[i]) ? sub_to_outer[key_pairs.second[i]] : key_pairs.second[i];
        ConstASTPtr join_predicate
            = makeASTFunction("equals", ASTs{std::make_shared<ASTIdentifier>(left), std::make_shared<ASTIdentifier>(right)});

        // todo rewrite using sub to outer
        join_predicate = ExpressionRewriter::rewrite(join_predicate, sub_symbol_mapping);
        if (distinct.erase(join_predicate))
        {
            continue;
        }

        join_predicate = makeASTFunction("equals", ASTs{std::make_shared<ASTIdentifier>(left), std::make_shared<ASTIdentifier>(right)});
        join_predicate = ExpressionRewriter::rewrite(join_predicate, sub_symbol_mapping);
        if (distinct.erase(join_predicate))
        {
            continue;
        }
        return {};
    }

    if (distinct.empty())
        return {};

    // 2. remove subquery conjunct and only outer table conjunct
    ConstASTPtr subquery_conjunct;
    ConstASTs removed_conjuncts;
    for (auto & conjunct : outer_conjuncts)
    {
        auto symbols = SymbolsExtractor::extract(conjunct);
        //  remove subquery conjunct
        if (symbols.contains(apply_step->getAssignment().first))
        {
            subquery_conjunct = conjunct;
            continue;
        }
        // remove only outer table conjunct
        bool only_outer_table = true;
        for (const auto & symbol : symbols)
        {
            if (!output_names.contains(symbol))
            {
                only_outer_table = false;
                break;
            }
        }
        if (only_outer_table)
        {
            continue;
        }
        removed_conjuncts.emplace_back(conjunct);
    }

    if (!subquery_conjunct)
        return {};


    auto normalize_expr = [](ConstASTPtr & compare) {
        if (const auto * func = compare->as<ASTFunction>())
        {
            if (func->name == "equals")
            {
                if (func->arguments->children.size() == 2)
                {
                    if (auto * left = func->arguments->children[0]->as<ASTIdentifier>())
                    {
                        auto left_name = left->name();
                        if (auto * right = func->arguments->children[1]->as<ASTIdentifier>())
                        {
                            auto right_name = right->name();
                            if (right_name > left_name)
                            {
                                compare = makeASTFunction(
                                    "equals",
                                    ASTs{std::make_shared<ASTIdentifier>(right_name), std::make_shared<ASTIdentifier>(left_name)});
                            }
                        }
                    }
                }
            }
        }
    };

    if (sub_filter_step)
    {
        auto sub_conjuncts = PredicateUtils::extractConjuncts(sub_filter_step->getFilter());
        if (removed_conjuncts.size() != sub_conjuncts.size())
        {
            return {};
        }
        for (auto & removed_conjunct : removed_conjuncts)
        {
            removed_conjunct = ExpressionRewriter::rewrite(removed_conjunct, outer_symbol_mapping);
            normalize_expr(removed_conjunct);
        }
        for (auto & sub_conjunct : sub_conjuncts)
        {
            sub_conjunct = ExpressionRewriter::rewrite(sub_conjunct, sub_symbol_mapping);
            normalize_expr(sub_conjunct);
        }
        std::sort(removed_conjuncts.begin(), removed_conjuncts.end(), [](auto & a, auto & b) { return ASTEquality::compareTree(a, b); });
        std::sort(sub_conjuncts.begin(), sub_conjuncts.end(), [](auto & a, auto & b) { return ASTEquality::compareTree(a, b); });

        auto outer_predicate = PredicateUtils::combineConjuncts(removed_conjuncts);
        auto sub_predicate = PredicateUtils::combineConjuncts(sub_conjuncts);
        auto outer_str = serializeAST(*outer_predicate);
        auto sub_str = serializeAST(*sub_predicate);
        if (outer_str != sub_str)
        {
            return {};
        }
    }
    else if (!removed_conjuncts.empty())
    {
        return {};
    }

    WindowDescription desc;
    desc.window_name = rule_context.context->getSymbolAllocator()->newSymbol("__window_name");
    for (const auto & col : apply_step->getCorrelation())
    {
        desc.partition_by.emplace_back(SortColumnDescription{col, 1, 1});
    }
    desc.full_sort_description = desc.partition_by;

    WindowFunctionDescription func_desc;
    func_desc.column_name = agg_step->getAggregates()[0].column_name;
    func_desc.aggregate_function = agg_step->getAggregates()[0].function;
    func_desc.function_parameters = agg_step->getAggregates()[0].parameters;
    auto input_header = agg_step->getInputStreams()[0].header;
    for (const auto & arg_name : agg_step->getAggregates()[0].argument_names)
    {
        func_desc.argument_types.emplace_back(input_header.getByName(arg_name).type);
        func_desc.argument_names.emplace_back(sub_to_outer.contains(arg_name) ? sub_to_outer[arg_name] : arg_name);
    }
    desc.window_functions.emplace_back(func_desc);

    auto new_outer_conjuncts = PredicateUtils::extractConjuncts(filter_step->getFilter());
    ConstASTs removed_outer_conjuncts;
    ConstASTPtr correlation_filter;
    for (auto & conjunct : new_outer_conjuncts)
    {
        auto symbols = SymbolsExtractor::extract(conjunct);
        //  remove subquery conjunct
        if (symbols.contains(apply_step->getAssignment().first))
        {
            correlation_filter = conjunct;
            continue;
        }
        removed_outer_conjuncts.emplace_back(conjunct);
    }

    auto new_filter_ast = PredicateUtils::combineConjuncts(removed_outer_conjuncts);
    auto new_filter_step = std::make_shared<FilterStep>(left_node->getCurrentDataStream(), new_filter_ast);
    auto new_filter_node = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), new_filter_step, {left_node});

    auto window_step = std::make_shared<WindowStep>(new_filter_node->getCurrentDataStream(), desc, true, SortDescription{});
    auto window_node = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), window_step, {new_filter_node});

    if (proj_step)
    {
        auto window_output = window_step->getOutputStream().header;
        Assignments assignments;
        NameToType name_to_type;
        for (const auto & item : filter_node->getCurrentDataStream().header)
        {
            if (window_output.has(item.name))
            {
                assignments.emplace(item.name, std::make_shared<ASTIdentifier>(item.name));
                name_to_type[item.name] = item.type;
            }
            else
            {
                if (proj_step->getAssignments().contains(item.name))
                {
                    assignments.emplace(item.name, proj_step->getAssignments().at(item.name));
                    name_to_type[item.name] = item.type;
                }
                else
                    return {};
            }
        }

        auto new_proj_step = std::make_shared<ProjectionStep>(left_node->getCurrentDataStream(), assignments, name_to_type);
        window_node = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), new_proj_step, {window_node});
    }

    auto after_filter_step = std::make_shared<FilterStep>(left_node->getCurrentDataStream(), correlation_filter);
    auto after_filter_node = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), after_filter_step, {window_node});

    return after_filter_node;
}


ConstRefPatternPtr UnnestingWithProjectionWindow::getPattern() const
{
    static NameSet agg_white_list{"count", "sum", "avg", "min", "max"};
    static auto pattern = Patterns::filter()
        .with(Patterns::apply()
                  .matchingStep<ApplyStep>([](const ApplyStep & s) {
                      return s.getSubqueryType() == ApplyStep::SubqueryType::SCALAR && !s.getCorrelation().empty();
                  })
                  .with(
                      Patterns::any(),
                      Patterns::project().with(Patterns::aggregating().matchingStep<AggregatingStep>([](const AggregatingStep & agg) {
                          return agg.getAggregates().size() == 1 && agg_white_list.count(agg.getAggregates()[0].function->getName());
                      }))))
        .result();
    return pattern;
}

ConstRefPatternPtr ExistsToSemiJoin::getPattern() const
{
    static auto pattern = Patterns::apply()
        .matchingStep<ApplyStep>([](const ApplyStep & s) {
            return s.supportSemiAnti() && s.getSubqueryType() == ApplyStep::SubqueryType::EXISTS && !s.getCorrelation().empty();
        })
        .result();
    return pattern;
}

TransformResult ExistsToSemiJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    if (context->getSettingsRef().enable_unnesting_subquery_with_semi_anti_join)
    {
        auto * apply = dynamic_cast<ApplyNode *>(node.get());
        auto & apply_step = *apply->getStep();

        PlanNodePtr input_ptr = apply->getChildren()[0];
        PlanNodePtr subquery_ptr = apply->getChildren()[1];

        PlanNodePtr source = subquery_ptr;
        auto correlation = apply_step.getCorrelation();

        std::optional<DecorrelationResult> result = Decorrelation::decorrelateFilters(source, correlation, *context);
        if (!result.has_value())
        {
            throw Exception(
                "Correlated Exists subquery de-correlation error, correlation filter not exists: ", ErrorCodes::REMOVE_SUBQUERY_ERROR);
        }

        DecorrelationResult & result_value = result.value();
        subquery_ptr = result_value.node;

        std::pair<Names, Names> key_pairs = result->buildJoinClause(input_ptr, subquery_ptr, correlation, context);

        std::vector<ConstASTPtr> filter = result->extractFilter();

        auto strictness = ASTTableJoin::Strictness::Unspecified;

        if (const auto * literal = apply_step.getAssignment().second->as<ASTLiteral>())
        {
            UInt64 value = 1;
            literal->value.tryGet(value);
            if (!value)
            {
                strictness = ASTTableJoin::Strictness::Anti;
            }
            else
            {
                strictness = ASTTableJoin::Strictness::Semi;
            }
        }

        else
        {
            return {};
        }

        Assignments right_correlation_assignments;
        NameToType right_correlation_name_to_type;
        for (const auto & column : subquery_ptr->getStep()->getOutputStream().header)
        {
            if (std::find(key_pairs.second.begin(), key_pairs.second.end(), column.name) != key_pairs.second.end())
            {
                Assignment assignment{column.name, std::make_shared<ASTIdentifier>(column.name)};
                right_correlation_assignments.emplace_back(assignment);
                right_correlation_name_to_type[column.name] = column.type;
            }
            }

            auto right_correlation_step = std::make_shared<ProjectionStep>(
                subquery_ptr->getStep()->getOutputStream(), right_correlation_assignments, right_correlation_name_to_type);
            auto right_correlation_node
                = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(right_correlation_step), PlanNodes{subquery_ptr});


            const auto & join_left = input_ptr;
            const auto & join_right = right_correlation_node;
            DataStreams streams = {join_left->getCurrentDataStream(), join_right->getCurrentDataStream()};
            auto left_header = join_left->getCurrentDataStream().header;
            auto right_header = join_right->getCurrentDataStream().header;
            NamesAndTypes output;
            for (const auto & item : left_header)
            {
                output.emplace_back(NameAndTypePair{item.name, item.type});
            }
            for (const auto & item : right_header)
            {
                output.emplace_back(NameAndTypePair{item.name, item.type});
            }

            auto join_step = std::make_shared<JoinStep>(
                streams,
                DataStream{.header = output},
                ASTTableJoin::Kind::Left,
                strictness,
                context->getSettingsRef().max_threads,
                context->getSettingsRef().optimize_read_in_order,
                key_pairs.first,
                key_pairs.second,
                std::vector<bool>{},
                PredicateUtils::combineConjuncts(filter),
                false,
                std::nullopt,
                ASOF::Inequality::GreaterOrEquals,
                DistributionType::UNKNOWN);
            PlanNodePtr join_node
                = std::make_shared<JoinNode>(context->nextNodeId(), std::move(join_step), PlanNodes{join_left, join_right});

            Assignments exist_assignments;
        NameToType exist_name_to_type;
        for (const auto & column : join_node->getStep()->getOutputStream().header)
        {
            Assignment assignment{column.name, std::make_shared<ASTIdentifier>(column.name)};
            exist_assignments.emplace_back(assignment);
            exist_name_to_type[column.name] = column.type;
        }
        Assignment assignment{apply_step.getAssignment().first, std::make_shared<ASTLiteral>(1u)};
        exist_assignments.emplace_back(assignment);
        exist_name_to_type[apply_step.getAssignment().first] = std::make_shared<DataTypeUInt8>();

        // Exists function has been rewritten into "exists" symbols
        auto exists_step = std::make_shared<ProjectionStep>(join_node->getStep()->getOutputStream(), exist_assignments, exist_name_to_type);
            PlanNodePtr exists_node = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(exists_step), PlanNodes{join_node});
            return exists_node;
            }

    return {};
}


ConstRefPatternPtr InToSemiJoin::getPattern() const
{
     static auto pattern = Patterns::apply()
        .matchingStep<ApplyStep>([](const ApplyStep & s) {
            return s.supportSemiAnti() && s.getSubqueryType() == ApplyStep::SubqueryType::IN && s.getCorrelation().empty();
        })
        .result();
    return pattern;
}

TransformResult InToSemiJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    if (context->getSettingsRef().enable_unnesting_subquery_with_semi_anti_join)
    {
        auto * apply = dynamic_cast<ApplyNode *>(node.get());
        auto & apply_step = *apply->getStep();

        const auto & correlation = apply_step.getCorrelation();
        if (!correlation.empty())
        {
            return {};
        }

        PlanNodePtr input_ptr = node->getChildren()[0];
        PlanNodePtr subquery_ptr = node->getChildren()[1];

        // const DataStream & left_data_stream = input_ptr->getStep()->getOutputStream();
        // const DataStream & right_data_stream = subquery_ptr->getStep()->getOutputStream();

        const auto & in_assignment = apply_step.getAssignment();
        const auto & in_fun = in_assignment.second->as<ASTFunction &>();
        ASTIdentifier & fun_left = in_fun.arguments->children[0]->as<ASTIdentifier &>();
        ASTIdentifier & fun_right = in_fun.arguments->children[1]->as<ASTIdentifier &>();

        Names in_left{fun_left.name()};
        Names in_right{fun_right.name()};

        auto strictness = ASTTableJoin::Strictness::Unspecified;

        if (in_fun.name == "notIn")
        {
            if (!isNullableOrLowCardinalityNullable(subquery_ptr->getOutputNamesToTypes().at(fun_right.name())))
                strictness = ASTTableJoin::Strictness::Anti;
            else
                return {};
        }
        else if (in_fun.name == "in")
        {
            strictness = ASTTableJoin::Strictness::Semi;
        }
        else
        {
            return {};
        }

        if (strictness == ASTTableJoin::Strictness::Unspecified)
        {
            return {};
        }


        const auto & join_left = node->getChildren()[0];
        const auto & join_right = node->getChildren()[1];
        DataStreams streams = {join_left->getCurrentDataStream(), join_right->getCurrentDataStream()};
        auto left_header = join_left->getCurrentDataStream().header;
        auto right_header = join_right->getCurrentDataStream().header;
        NamesAndTypes output;
        for (const auto & item : left_header)
        {
            output.emplace_back(NameAndTypePair{item.name, item.type});
        }
        for (const auto & item : right_header)
        {
            output.emplace_back(NameAndTypePair{item.name, item.type});
        }

        auto join_step = std::make_shared<JoinStep>(
            streams,
            DataStream{.header = output},
            ASTTableJoin::Kind::Left,
            strictness,
            context->getSettingsRef().max_threads,
            context->getSettingsRef().optimize_read_in_order,
            in_left,
            in_right,
            std::vector<bool>{},
            PredicateConst::TRUE_VALUE,
            false,
            std::nullopt,
            ASOF::Inequality::GreaterOrEquals,
            DistributionType::UNKNOWN);
        PlanNodePtr join_node = std::make_shared<JoinNode>(context->nextNodeId(), std::move(join_step), PlanNodes{join_left, join_right});

        Assignments exist_assignments;
        NameToType exist_name_to_type;
        for (const auto & column : join_node->getStep()->getOutputStream().header)
        {
            Assignment assignment{column.name, std::make_shared<ASTIdentifier>(column.name)};
            exist_assignments.emplace_back(assignment);
            exist_name_to_type[column.name] = column.type;
        }
        Assignment assignment{apply_step.getAssignment().first, std::make_shared<ASTLiteral>(1u)};
        exist_assignments.emplace_back(assignment);
        exist_name_to_type[apply_step.getAssignment().first] = std::make_shared<DataTypeUInt8>();

        // Exists function has been rewritten into "exists" symbols
        auto exists_step = std::make_shared<ProjectionStep>(join_node->getStep()->getOutputStream(), exist_assignments, exist_name_to_type);
        PlanNodePtr exists_node = std::make_shared<ProjectionNode>(context->nextNodeId(), std::move(exists_step), PlanNodes{join_node});
        return exists_node;
    }

    return {};
}

}
