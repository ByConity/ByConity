#include <Optimizer/Rule/Rewrite/ExtractBitmapImplicitFilter.h>

#include <AggregateFunctions/AggregateBitmapExpression_fwd.h>
#include <Interpreters/convertFieldToType.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/SymbolTransformMap.h>
#include <Parsers/ASTTableColumnReference.h>
#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>

#include <Poco/String.h>

namespace DB
{

namespace
{
    void collectionParameterValueInFunctionOptimizer(
        const AggregateDescription * aggregate,
        std::unordered_map<String, std::set<Field>> & parameters_map,
        SymbolTransformMap & symbol_transform)
    {
        const auto & agg_arguments = aggregate->argument_names;
        const auto & agg_parameters = aggregate->parameters;

        if (agg_arguments.size() < 2 || agg_parameters.empty())
            return;

        const String & index_arg = agg_arguments.front();
        auto index_arg_inlined_expr = symbol_transform.inlineReferences(index_arg);
        assert(index_arg_inlined_expr);

        const auto * table_column_ref = index_arg_inlined_expr->as<ASTTableColumnReference>();
        if (!table_column_ref)
            return;

        assert(table_column_ref->storage);
        DataTypePtr data_type{nullptr};
        data_type = table_column_ref->storage->getInMemoryMetadataPtr()
                        ->getColumns()
                        .tryGetPhysical(table_column_ref->column_name)
                        .value_or(NameAndTypePair{})
                        .type;

        if (!data_type)
            return;

        auto & parameters = parameters_map[index_arg];
        String exp;

        for (const auto & param : agg_parameters)
        {
            if (param.tryGet<String>(exp))
            {
                Names values = getBitMapParameterValues(exp);
                for (auto & node : values)
                {
                    Field field(node);
                    field = convertFieldToType(field, *data_type);
                    parameters.emplace(field);
                }
            }
        }
    }
}

ConstRefPatternPtr ExtractBitmapImplicitFilter::getPattern() const
{
    static auto pattern = Patterns::aggregating()
        .matchingStep<AggregatingStep>([](const AggregatingStep & step, Captures & captures) {
            std::vector<const AggregateDescription *> target_aggregates;

            for (const auto & agg : step.getAggregates())
                if (BITMAP_EXPRESSION_AGGREGATE_FUNCTIONS.count(Poco::toLower(agg.function->getName())))
                    target_aggregates.emplace_back(&agg);

            bool matched = !target_aggregates.empty();
            captures.emplace(aggregate_capture, std::move(target_aggregates));
            return matched;
        })
        .result();
    return pattern;
}

TransformResult ExtractBitmapImplicitFilter::transformImpl(PlanNodePtr node, const Captures & captures, RuleContext & rule_context)
{
    const auto & target_aggregates = captures.at<const std::vector<const AggregateDescription *> &>(aggregate_capture);
    auto symbol_transform = SymbolTransformMap::buildFrom(*node);
    if (!symbol_transform)
        return {};

    std::unordered_map<String, std::set<Field>> parameters_map;
    for (const auto * aggregate : target_aggregates)
        collectionParameterValueInFunctionOptimizer(aggregate, parameters_map, *symbol_transform);

    if (parameters_map.empty())
        return {};

    ASTs functions;
    /// create in function for those parametered values
    for (const auto & parameter : parameters_map)
    {
        auto [in_ast, elem_size] = createInFunctionForBitMapParameter(parameter.first, parameter.second);
        functions.push_back(in_ast);
    }

    auto implicit_filter = PredicateUtils::combineDisjunctsWithDefault<false>(functions, PredicateConst::TRUE_VALUE);

    if (PredicateUtils::isTruePredicate(implicit_filter))
        return {};

    LOG_DEBUG(
        &Poco::Logger::get("ExtractBitmapImplicitFilter"),
        "Extract bitmap implicit filter for plan node {}, extracted filter: {}",
        node->getId(),
        serializeAST(*implicit_filter));

    auto filter_step = std::make_shared<FilterStep>(node->getChildren().front()->getCurrentDataStream(), implicit_filter);
    auto filter_node = PlanNodeBase::createPlanNode(rule_context.context->nextNodeId(), std::move(filter_step), node->getChildren());

    node->replaceChildren({filter_node});
    return node;
}

}
