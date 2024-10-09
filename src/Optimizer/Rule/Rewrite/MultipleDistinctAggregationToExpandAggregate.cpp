#include <string>
#include <unordered_map>
#include <vector>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/NameToType.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/Context.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Rule/Rewrite/MultipleDistinctAggregationToExpandAggregate.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/Assignment.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/ExpandStep.h>
#include <QueryPlan/MarkDistinctStep.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/QueryPlan.h>

namespace DB
{

const std::set<String> MultipleDistinctAggregationToExpandAggregate::distinct_func{
    "uniqexact", "countdistinct", "avgdistinct", "maxdistinct", "mindistinct", "sumdistinct"};

const std::set<String> MultipleDistinctAggregationToExpandAggregate::distinct_func_with_if{
    "uniqexactif", "countdistinctif", "avgdistinctif", "maxdistinctif", "mindistinctif", "sumdistinctif"};

const std::set<String> MultipleDistinctAggregationToExpandAggregate::non_distinct_func_with_if{
    "sumif", "countif", "avgif", "maxif", "minif"};

const std::unordered_map<String, String> MultipleDistinctAggregationToExpandAggregate::distinct_func_normal_func{
    {"uniqexact", "countIf"},
    {"countdistinct", "countIf"},
    {"avgdistinct", "avgIf"},
    {"maxdistinct", "maxIf"},
    {"mindistinct", "minIf"},
    {"sumdistinct", "sumIf"}};

const std::set<String> MultipleDistinctAggregationToExpandAggregate::un_supported_func{"hllsketchestimate"};

bool MultipleDistinctAggregationToExpandAggregate::hasNoFilterOrMask(const AggregatingStep & step)
{
    const AggregateDescriptions & agg_descs = step.getAggregates();
    for (const auto & agg_desc : agg_descs)
    {
        if (distinct_func.contains(Poco::toLower(agg_desc.function->getName())) && !agg_desc.mask_column.empty())
            return false;

        if (distinct_func_with_if.contains(Poco::toLower(agg_desc.function->getName())))
            return false;

        if (non_distinct_func_with_if.contains(Poco::toLower(agg_desc.function->getName())))
            return false;
    }
    return true;
}

bool MultipleDistinctAggregationToExpandAggregate::hasMultipleDistincts(const AggregatingStep & step)
{
    std::set<Names> multiple_distinct_aggs;
    const AggregateDescriptions & agg_descs = step.getAggregates();
    for (const auto & agg_desc : agg_descs)
    {
        if (distinct_func.contains(Poco::toLower(agg_desc.function->getName())))
            multiple_distinct_aggs.emplace(agg_desc.argument_names);
    }
    return multiple_distinct_aggs.size() > 1;
}

bool MultipleDistinctAggregationToExpandAggregate::hasMixedDistinctAndNonDistincts(const AggregatingStep & step)
{
    size_t distinct_aggs = 0;
    const AggregateDescriptions & agg_descs = step.getAggregates();
    for (const auto & agg_desc : agg_descs)
    {
        if (distinct_func.contains(Poco::toLower(agg_desc.function->getName())))
        {
            distinct_aggs++;
        }
    }
    return distinct_aggs > 0 && distinct_aggs < agg_descs.size();
}

bool MultipleDistinctAggregationToExpandAggregate::hasUniqueArgument(const AggregatingStep & step)
{
    const AggregateDescriptions & agg_descs = step.getAggregates();
    Names distinct_arguments;
    for (const auto & agg_desc : agg_descs)
    {
        if (distinct_func.contains(Poco::toLower(agg_desc.function->getName())))
        {
            distinct_arguments.insert(distinct_arguments.end(), agg_desc.argument_names.begin(), agg_desc.argument_names.end());
        }
    }

    for (const auto & agg_desc : agg_descs)
    {
        if (!distinct_func.contains(Poco::toLower(agg_desc.function->getName())))
        {
            for (const auto & argument : agg_desc.argument_names)
            {
                auto it = std::find(distinct_arguments.begin(), distinct_arguments.end(), argument);
                // if distinct aggregate arguments contains non-distinct aggregate argument, return false.
                if (it != distinct_arguments.end())
                {
                    return false;
                }
            }
        }
    }
    return true;
}

bool MultipleDistinctAggregationToExpandAggregate::hasNoUnSupportedFunc(const AggregatingStep & step)
{
    const AggregateDescriptions & agg_descs = step.getAggregates();
    for (const auto & agg_desc : agg_descs)
    {
        if (un_supported_func.contains(Poco::toLower(agg_desc.function->getName())))
            return false;
        if (!distinct_func.contains(Poco::toLower(agg_desc.function->getName())) && agg_desc.arguments.size() > 1)
            return false;
    }
    return true;
}

ConstRefPatternPtr MultipleDistinctAggregationToExpandAggregate::getPattern() const
{
    static auto pattern = Patterns::aggregating()
                              .matchingStep<AggregatingStep>([](const AggregatingStep & s) {
                                  return hasNoFilterOrMask(s) && (hasMultipleDistincts(s) || hasMixedDistinctAndNonDistincts(s))
                                      && hasUniqueArgument(s) && hasNoUnSupportedFunc(s);
                              })
                              .result();
    return pattern;
}

TransformResult MultipleDistinctAggregationToExpandAggregate::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto step_ptr = node->getStep();
    const auto & step = dynamic_cast<const AggregatingStep &>(*step_ptr);
    const AggregateDescriptions & agg_descs = step.getAggregates();

    PlanNodePtr child = node->getChildren()[0];
    const Block & input = child->getStep()->getOutputStream().header;

    /// step 1 : expand input data
    Assignments assignments;
    NameToType name_type;
    for (const auto & input_column : input)
    {
        DataTypePtr type = input_column.type;
        // type = JoinCommon::tryConvertTypeToNullable(type);
        name_type[input_column.name] = type;
        assignments.emplace(
            input_column.name,
            makeASTFunction("cast", std::make_shared<ASTLiteral>(type->getDefault()), std::make_shared<ASTLiteral>(type->getName())));
    }

    /// append a extra mark field : group_id.
    String group_id_symbol = rule_context.context->getSymbolAllocator()->newSymbol(ExpandStep::group_id);

    DataStream output_stream;
    for (auto & entry : name_type)
    {
        output_stream.header.insert(ColumnWithTypeAndName{entry.second, entry.first});
    }
    output_stream.header.insert(ColumnWithTypeAndName{std::make_shared<DataTypeInt32>(), group_id_symbol});

    /// all non distinct aggregate functions will be assign to group (0)
    int non_distinct_group_id = 0;

    /// every distinct aggregate function will be assign to a new group. e.g. group(0), group(1), ...
    int distinct_group_id = 1;

    AggregateDescriptions non_distinct_aggs;
    AggregateDescriptions distinct_aggs;

    std::set<String> distinct_arguments;
    std::set<Int32> group_id_value;

    std::map<Int32, Names> group_id_non_null_symbol;

    Assignments group_id_mask_assignment;
    AggregateDescriptions aggs_with_mask;

    String non_distinct_agg_group_id_mask;

    Assignments new_argument_assignments;

    for (const auto & agg_desc : agg_descs)
    {
        String group_id_mask;
        if (distinct_func.contains(Poco::toLower(agg_desc.function->getName())) && agg_desc.mask_column.empty())
        {
            distinct_aggs.emplace_back(agg_desc);

            for (const auto & argument : agg_desc.argument_names)
            {
                distinct_arguments.insert(argument);
            }

            group_id_value.insert(distinct_group_id);
            group_id_non_null_symbol[distinct_group_id] = agg_desc.argument_names;

            group_id_mask
                = rule_context.context->getSymbolAllocator()->newSymbol(ExpandStep::group_id_mask + std::to_string(distinct_group_id));

            group_id_mask_assignment.emplace(
                group_id_mask,
                makeASTFunction(
                    "equals", std::make_shared<ASTIdentifier>(group_id_symbol), std::make_shared<ASTLiteral>(distinct_group_id)));

            aggs_with_mask.emplace_back(distinctAggWithMask(agg_desc, group_id_mask, new_argument_assignments, rule_context.context));
            distinct_group_id++;
        }
        else
        {
            non_distinct_aggs.emplace_back(agg_desc);
            group_id_value.insert(non_distinct_group_id);

            if (non_distinct_agg_group_id_mask.empty())
            {
                group_id_mask = rule_context.context->getSymbolAllocator()->newSymbol(
                    ExpandStep::group_id_mask + std::to_string(non_distinct_group_id));

                group_id_mask_assignment.emplace(
                    group_id_mask,
                    makeASTFunction(
                        "equals", std::make_shared<ASTIdentifier>(group_id_symbol), std::make_shared<ASTLiteral>(non_distinct_group_id)));

                non_distinct_agg_group_id_mask = group_id_mask;
            }

            if (group_id_non_null_symbol.contains(non_distinct_group_id))
            {
                Names & names = group_id_non_null_symbol[non_distinct_group_id];
                names.insert(names.end(), agg_desc.argument_names.begin(), agg_desc.argument_names.end());
            }
            else
            {
                group_id_non_null_symbol[non_distinct_group_id] = agg_desc.argument_names;
            }

            aggs_with_mask.emplace_back(nonDistinctAggWithMask(agg_desc, non_distinct_agg_group_id_mask));
        }
    }

    /// each group should reserve group by symbol
    for (const auto & id : group_id_value)
    {
        Names & names = group_id_non_null_symbol[id];
        names.insert(names.end(), step.getKeys().begin(), step.getKeys().end());
    }

    PlanNodePtr expand_node;
    if (rule_context.context->getSettings().expand_mode == ExpandMode::EXPAND)
    {
        auto expand_step = std::make_shared<ExpandStep>(
            child->getStep()->getOutputStream(), assignments, name_type, group_id_symbol, group_id_value, group_id_non_null_symbol);
        expand_node = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(expand_step), {child});
    }
    if (rule_context.context->getSettings().expand_mode == ExpandMode::UNION)
    {
        expand_node = makeUnionNode(
            rule_context, group_id_value, assignments, name_type, group_id_non_null_symbol, group_id_symbol, output_stream, child);
    }
    if (rule_context.context->getSettings().expand_mode == ExpandMode::CTE)
    {
        expand_node = makeCTENode(
            rule_context, group_id_value, assignments, name_type, group_id_non_null_symbol, group_id_symbol, output_stream, child);
    }

    // step 2 : add pre-compute aggregate
    std::set<String> keyset;
    for (const String & key : step.getKeys())
    {
        keyset.insert(key);
    }
    keyset.insert(group_id_symbol);
    for (const String & distinct : distinct_arguments)
    {
        keyset.insert(distinct);
    }

    // make sure keys remove duplicated value.
    Names keys;
    keys.insert(keys.end(), keyset.begin(), keyset.end());

    auto pre_agg_step = std::make_shared<AggregatingStep>(
        expand_node->getStep()->getOutputStream(),
        keys,
        step.getKeysNotHashed(),
        non_distinct_aggs,
        step.getGroupingSetsParams(),
        true,
        step.getGroupBySortDescription(),
        step.getGroupings(),
        step.needOverflowRow(),
        step.shouldProduceResultsInOrderOfBucketNumber());

    auto pre_agg_node = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(pre_agg_step), {expand_node});

    // step 3 : calculate mask value.
    Assignments mask_assignments;
    NameToType mask_null_name_to_type;
    for (const auto & column : pre_agg_node->getStep()->getOutputStream().header)
    {
        Assignment assignment{column.name, std::make_shared<ASTIdentifier>(column.name)};
        mask_assignments.emplace_back(assignment);
        mask_null_name_to_type[column.name] = column.type;
    }

    for (auto & ass : group_id_mask_assignment)
    {
        mask_assignments.emplace_back(ass);
        mask_null_name_to_type[ass.first] = std::make_shared<DataTypeUInt8>();
    }

    auto mask_step = std::make_shared<ProjectionStep>(pre_agg_node->getStep()->getOutputStream(), mask_assignments, mask_null_name_to_type);
    child = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(mask_step), PlanNodes{pre_agg_node});

    if (!new_argument_assignments.empty())
    {
        NameToType name_to_type;
        for (const auto & assignment : new_argument_assignments)
            name_to_type.emplace(assignment.first, std::make_shared<DataTypeUInt8>());

        for (const auto & input_column : child->getStep()->getOutputStream().header)
        {
            new_argument_assignments.emplace(input_column.name, makeASTIdentifier(input_column.name));
            name_to_type.emplace(input_column.name, input_column.type);
        }
        auto new_argument_projection_step
            = std::make_shared<ProjectionStep>(child->getStep()->getOutputStream(), new_argument_assignments, name_to_type);
        child = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(new_argument_projection_step), {child});
    }

    // step 4 : final aggregate
    auto count_agg_step = std::make_shared<AggregatingStep>(
        child->getStep()->getOutputStream(),
        step.getKeys(),
        step.getKeysNotHashed(),
        aggs_with_mask,
        step.getGroupingSetsParams(),
        step.isFinal(),
        step.getGroupBySortDescription(),
        step.getGroupings(),
        step.needOverflowRow(),
        step.shouldProduceResultsInOrderOfBucketNumber(),
        step.isNoShuffle(),
        step.isStreamingForCache(),
        step.getHints());
    auto count_agg_node = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(count_agg_step), {child});

    return count_agg_node;
}

AggregateDescription MultipleDistinctAggregationToExpandAggregate::distinctAggWithMask(
    const AggregateDescription & agg_desc, String & mask_column, Assignments & new_argument_assignments, ContextMutablePtr context)
{
    String fun_remove_distinct = distinct_func_normal_func.at(Poco::toLower(agg_desc.function->getName()));
    Names argument_names;
    DataTypes data_types;
    if (fun_remove_distinct == "countIf" && agg_desc.argument_names.size() > 1)
    {
        // countDistinct(arg1, arg2) cannot convert to count(arg1, arg2), because clickhousedon't support count multi arguments.
        // As an alternative we can rewrite it to count(IF(arg1 is null, null, arg2 is null, null, 1)),
        // or sum((arg1 is not null) AND (arg2 is not null))
        fun_remove_distinct = "sumIf";

        ASTs argument_functions;
        for (const auto & argument : agg_desc.argument_names)
            argument_functions.emplace_back(makeASTFunction("isNotNull", makeASTIdentifier(argument)));
        auto new_argument = PredicateUtils::combineConjuncts(argument_functions);
        auto new_argument_name = context->getSymbolAllocator()->newSymbol(new_argument);
        new_argument_assignments.emplace_back(new_argument_name, new_argument);

        argument_names.emplace_back(new_argument_name);
        data_types.emplace_back(std::make_shared<DataTypeUInt8>());
    }
    else
    {
        argument_names = agg_desc.argument_names;
        data_types = agg_desc.function->getArgumentTypes();
    }

    argument_names.emplace_back(mask_column);
    data_types.emplace_back(std::make_shared<DataTypeUInt8>());

    Array parameters = agg_desc.function->getParameters();
    AggregateFunctionProperties properties;
    AggregateFunctionPtr new_agg_fun = AggregateFunctionFactory::instance().get(fun_remove_distinct, data_types, parameters, properties);

    AggregateDescription agg_with_mask;

    agg_with_mask.mask_column = mask_column;
    agg_with_mask.function = new_agg_fun;
    agg_with_mask.parameters = agg_desc.parameters;
    agg_with_mask.column_name = agg_desc.column_name;
    agg_with_mask.argument_names = argument_names;
    agg_with_mask.parameters = agg_desc.parameters;
    agg_with_mask.arguments = agg_desc.arguments;

    return agg_with_mask;
}

AggregateDescription
MultipleDistinctAggregationToExpandAggregate::nonDistinctAggWithMask(const AggregateDescription & agg_desc, String & mask_column)
{
    DataTypes data_types;
    data_types.emplace_back(agg_desc.function->getReturnType());
    data_types.emplace_back(std::make_shared<DataTypeUInt8>());

    Array parameters;
    AggregateFunctionProperties properties;

    String fun = "anyIf";

    /// in case count(*), agg_desc.function->getArgumentTypes() returns empty.
    /// anyIf requires 2 arguments
    if (Poco::toLower(agg_desc.function->getName()) == "count" && data_types.size() == 1)
    {
        data_types.emplace_back(std::make_shared<DataTypeUInt8>());
    }

    AggregateFunctionPtr new_agg_fun = AggregateFunctionFactory::instance().get(fun, data_types, parameters, properties);
    Names argument_names;
    argument_names.emplace_back(agg_desc.column_name);
    argument_names.emplace_back(mask_column);

    AggregateDescription agg_with_mask;

    agg_with_mask.mask_column = mask_column;
    agg_with_mask.function = new_agg_fun;
    agg_with_mask.parameters = parameters;
    agg_with_mask.column_name = agg_desc.column_name;
    agg_with_mask.argument_names = argument_names;

    return agg_with_mask;
}

PlanNodePtr MultipleDistinctAggregationToExpandAggregate::makeUnionNode(
    RuleContext & rule_context,
    std::set<Int32> & group_id_value,
    Assignments & assignments,
    NameToType & name_type,
    std::map<Int32, Names> & group_id_non_null_symbol,
    String & group_id_symbol,
    DataStream & output_stream,
    PlanNodePtr child)
{
    DataStreams input_streams;
    OutputToInputs output_to_inputs;
    PlanNodes children;
    for (const auto & id : group_id_value)
    {
        Assignments assignments_pre_group;
        NameToType name_to_type_pre_group;
        for (const auto & assignment : assignments)
        {
            Names non_nulls = group_id_non_null_symbol.at(id);
            auto non_nulls_exist = std::find(non_nulls.begin(), non_nulls.end(), assignment.first);

            /// if symbol exists in non_nulls list, then we need project it.
            String column_symbol = rule_context.context->getSymbolAllocator()->newSymbol(assignment.first);
            if (non_nulls_exist != non_nulls.end())
            {
                assignments_pre_group.emplace(
                    column_symbol,
                    makeASTFunction(
                        "cast",
                        std::make_shared<ASTIdentifier>(assignment.first),
                        std::make_shared<ASTLiteral>(name_type[assignment.first]->getName())));
            }
            else /// otherwise, use null replace origin value.
            {
                assignments_pre_group.emplace(column_symbol, assignment.second);
            }
            name_to_type_pre_group[column_symbol] = name_type[assignment.first];

            String output = assignment.first;
            if (output_to_inputs.contains(output))
            {
                std::vector<String> & inputs = output_to_inputs[output];
                inputs.emplace_back(column_symbol);
            }
            else
            {
                output_to_inputs[output] = {column_symbol};
            }
        }

        String group_id_pre_group = rule_context.context->getSymbolAllocator()->newSymbol("group_id");
        assignments_pre_group.emplace(group_id_pre_group, std::make_shared<ASTLiteral>(id));
        name_to_type_pre_group[group_id_pre_group] = std::make_shared<DataTypeInt32>();

        if (output_to_inputs.contains(group_id_symbol))
        {
            std::vector<String> & inputs = output_to_inputs[group_id_symbol];
            inputs.emplace_back(group_id_pre_group);
        }
        else
        {
            output_to_inputs[group_id_symbol] = {group_id_pre_group};
        }

        auto expand_step
            = std::make_shared<ProjectionStep>(child->getStep()->getOutputStream(), assignments_pre_group, name_to_type_pre_group);
        auto expand_node = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(expand_step), {child});
        children.emplace_back(expand_node);
        input_streams.emplace_back(expand_node->getStep()->getOutputStream());
    }
    auto step = std::make_shared<UnionStep>(input_streams, output_stream, output_to_inputs);
    return PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(step), children);
}

PlanNodePtr MultipleDistinctAggregationToExpandAggregate::makeCTENode(
    RuleContext & rule_context,
    std::set<Int32> & group_id_value,
    Assignments & assignments,
    NameToType & name_type,
    std::map<Int32, Names> & group_id_non_null_symbol,
    String & group_id_symbol,
    DataStream & output_stream,
    PlanNodePtr child)
{
    CTEInfo & cte_info = rule_context.cte_info;
    auto cte_id = cte_info.nextCTEId();
    cte_info.add(cte_id, child);

    // DataStream output_stream = child->getStep()->getOutputStream();
    std::unordered_map<String, String> output_columns;
    for (auto & column : output_stream.header)
    {
        output_columns[column.name] = column.name;
    }

    DataStreams input_streams;
    OutputToInputs output_to_inputs;
    PlanNodes children;

    for (const auto & id : group_id_value)
    {
        Assignments assignments_pre_group;
        NameToType name_to_type_pre_group;
        for (const auto & assignment : assignments)
        {
            Names non_nulls = group_id_non_null_symbol.at(id);
            auto non_nulls_exist = std::find(non_nulls.begin(), non_nulls.end(), assignment.first);

            /// if symbol exists in non_nulls list, then we need project it.
            String column_symbol = rule_context.context->getSymbolAllocator()->newSymbol(assignment.first);
            if (non_nulls_exist != non_nulls.end())
            {
                assignments_pre_group.emplace(
                    column_symbol,
                    makeASTFunction(
                        "cast",
                        std::make_shared<ASTIdentifier>(assignment.first),
                        std::make_shared<ASTLiteral>(name_type[assignment.first]->getName())));
            }
            else /// otherwise, use null replace origin value.
            {
                assignments_pre_group.emplace(column_symbol, assignment.second);
            }
            name_to_type_pre_group[column_symbol] = name_type[assignment.first];

            String output = assignment.first;
            if (output_to_inputs.contains(output))
            {
                std::vector<String> & inputs = output_to_inputs[output];
                inputs.emplace_back(column_symbol);
            }
            else
            {
                output_to_inputs[output] = {column_symbol};
            }
        }

        String group_id_pre_group = rule_context.context->getSymbolAllocator()->newSymbol(ExpandStep::group_id);
        assignments_pre_group.emplace(group_id_pre_group, std::make_shared<ASTLiteral>(id));
        name_to_type_pre_group[group_id_pre_group] = std::make_shared<DataTypeInt32>();

        if (output_to_inputs.contains(group_id_symbol))
        {
            std::vector<String> & inputs = output_to_inputs[group_id_symbol];
            inputs.emplace_back(group_id_pre_group);
        }
        else
        {
            output_to_inputs[group_id_symbol] = {group_id_pre_group};
        }

        PlanNodePtr cte_ref = PlanNodeBase::createPlanNode(
            rule_context.context->nextNodeId(), std::make_shared<CTERefStep>(output_stream, cte_id, output_columns, false));

        auto expand_step
            = std::make_shared<ProjectionStep>(cte_ref->getStep()->getOutputStream(), assignments_pre_group, name_to_type_pre_group);
        auto expand_node = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(expand_step), {cte_ref});
        children.emplace_back(expand_node);
        input_streams.emplace_back(expand_node->getStep()->getOutputStream());
    }
    auto step = std::make_shared<UnionStep>(input_streams, output_stream, output_to_inputs);
    return PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(step), children);
}

}
