#include <Optimizer/Rule/Rewrite/PushUnionThroughJoin.h>

#include <Core/ColumnWithTypeAndName.h>
#include <Core/NameToType.h>
#include <Core/Names.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/ExpressionRewriter.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Signature/PlanSignature.h>
#include <Optimizer/SymbolsExtractor.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/AggregatingStep.h>
#include <QueryPlan/Assignment.h>
#include <QueryPlan/CTEInfo.h>
#include <QueryPlan/CTERefStep.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/QueryPlan.h>
#include <QueryPlan/SymbolMapper.h>
#include <QueryPlan/UnionStep.h>

namespace DB
{
ConstRefPatternPtr PushUnionThroughJoin::getPattern() const
{
    static auto pattern
        = Patterns::unionn()
              .withAll(Patterns::join().matchingStep<JoinStep>([](const JoinStep & step) {
                  return (step.getKind() == ASTTableJoin::Kind::Inner || step.getKind() == ASTTableJoin::Kind::Left)
                      && (step.getStrictness() == ASTTableJoin::Strictness::All || step.getStrictness() == ASTTableJoin::Strictness::Semi);
              }))
              .result();
    return pattern;
}

PlanNodePtr createNodeForSignature(PlanNodePtr node, Names join_keys, ContextMutablePtr context)
{
    Assignments assigments;
    NameToType name_to_type;
    for (auto col : join_keys)
    {
        assigments.emplace_back(col, std::make_shared<ASTIdentifier>(col));
        name_to_type[col] = node->getOutputNamesToTypes()[col];
    }
    return PlanNodeBase::createPlanNode(
        context->nextNodeId(), std::make_shared<ProjectionStep>(node->getCurrentDataStream(), assigments, name_to_type), {node});
}

TransformResult PushUnionThroughJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto & union_step = dynamic_cast<const UnionStep &>(*node->getStep());

    PlanSignatureProvider signature{context.cte_info, context.context};

    PlanSignature first_signature = 0;
    PlanNodes lefts;
    std::vector<Names> left_keys;
    std::vector<Names> left_keys_mapped;

    size_t index = 0;

    auto first_join_step = dynamic_cast<const JoinStep &>(*node->getChildren()[0]->getStep());
    auto kind = first_join_step.getKind();
    auto strictness = first_join_step.getStrictness();

    for (auto & union_child : node->getChildren())
    {
        auto join_step = dynamic_cast<const JoinStep &>(*union_child->getStep());
        if (join_step.getKind() != kind)
        {
            LOG_DEBUG(log, "kind");
            return {};
        }
        if (join_step.getStrictness() != strictness)
        {
            LOG_DEBUG(log, "strictness");
            return {};
        }

        Names right_columns = join_step.getRightKeys();
        for (auto col : SymbolsExtractor::extractVector(join_step.getFilter()))
            if (join_step.getInputStreams()[1].header.has(col))
            {
                right_columns.emplace_back(col);
            }

        auto sig_node = createNodeForSignature(union_child->getChildren()[1], right_columns, context.context);
        PlanNodeToSignatures plan_node_to_signatures = signature.computeSignatures(sig_node);
        if (first_signature != 0 && first_signature != plan_node_to_signatures[sig_node])
        {
            LOG_DEBUG(log, "signature");
            return {};
        }
        first_signature = plan_node_to_signatures[sig_node];
        lefts.emplace_back(union_child->getChildren()[0]);
        left_keys.emplace_back(join_step.getLeftKeys());


        auto in2out = Utils::reverseMap(union_step.getOutToInput(index));
        Names mapped;
        for (const auto & key : join_step.getLeftKeys())
        {
            mapped.emplace_back(in2out[key]);
        }
        left_keys_mapped.emplace_back(mapped);
        index++;
    }

    // check the same left join keys
    auto first_keys = left_keys_mapped[0];
    for (auto & item : left_keys_mapped)
    {
        if (first_keys != item)
        {
            LOG_DEBUG(log, "left key");
            return {};
        }
    }

    OutputToInputs new_out2ins;

    for (const auto & item : union_step.getOutToInputs())
    {
        Names new_input;
        for (size_t child_index = 0; child_index < item.second.size(); child_index++)
        {
            if (lefts[child_index]->getCurrentDataStream().header.has(item.second[child_index]))
            {
                new_input.emplace_back(item.second[child_index]);
            }
        }

        if (!new_input.empty() && new_input.size() != item.second.size())
        {
            LOG_DEBUG(log, "new input size");
            return {};
        }
        if (!new_input.empty()) // when in right, it is empty
            new_out2ins[item.first] = new_input;
    }

    for (size_t key_index = 0; key_index < left_keys_mapped[0].size(); key_index++)
    {
        if (left_keys_mapped[0][key_index].empty())
        {
            Names child_symbols;
            for (size_t child_index = 0; child_index < left_keys_mapped.size(); child_index++)
            {
                child_symbols.emplace_back(left_keys[child_index][key_index]);
            }
            auto out_symbol = context.context->getSymbolAllocator()->newSymbol(left_keys[0][key_index]);
            new_out2ins[out_symbol] = child_symbols;
            left_keys_mapped[0][key_index] = out_symbol;
        }
    }

    DataStreams inputs;
    for (const auto & left : lefts)
    {
        inputs.emplace_back(left->getCurrentDataStream());
    }
    DataStream output;
    for (auto item : new_out2ins)
    {
        output.header.insert(ColumnWithTypeAndName{inputs[0].header.getByName(item.second[0]).type, item.first});
    }


    ASTPtr filter;
    size_t filter_count = 0;
    std::unordered_map<size_t, String> symbol_index_to_out;
    std::unordered_map<String, String> symbol_to_out;
    ConstASTMap id_to_output;
    for (const auto & item : new_out2ins)
    {
        for (const auto & col : item.second)
            symbol_to_out[col] = item.first;
    }
    for (size_t i = 0; i < node->getChildren().size(); i++)
    {
        auto join_step = dynamic_cast<const JoinStep &>(*node->getChildren()[i]->getStep());
        if (!PredicateUtils::isTruePredicate(join_step.getFilter()))
        {
            auto ast = join_step.getFilter()->clone();
            auto symbols = SymbolsExtractor::extractVector(ast);

            ConstASTMap expression_map;
            size_t symbol_index = 0;
            for (auto symbol : symbols)
                if (lefts[i]->getCurrentDataStream().header.has(symbol))
                {
                    ASTPtr name = std::make_shared<ASTIdentifier>(symbol);
                    ASTPtr id = std::make_shared<ASTIdentifier>("$" + std::to_string(symbol_index));
                    if (!expression_map.contains(name))
                    {
                        expression_map[name] = ConstHashAST::make(id);
                        if (!symbol_index_to_out.count(symbol_index))
                        {
                            symbol_index_to_out[symbol_index] = symbol_to_out.contains(symbol)
                                ? symbol_to_out[symbol]
                                : context.context->getSymbolAllocator()->newSymbol(symbol);

                            // new symbol
                            if (!symbol_to_out.contains(symbol))
                                output.header.insert(
                                    {lefts[i]->getCurrentDataStream().header.getByName(symbol).type, symbol_index_to_out[symbol_index]});
                        }
                        if (!id_to_output.contains(id))
                            id_to_output[id] = ConstHashAST::make(std::make_shared<ASTIdentifier>(symbol_index_to_out[symbol_index]));

                        if (!symbol_to_out.contains(symbol))
                        {
                            new_out2ins[symbol_index_to_out[symbol_index]].emplace_back(symbol);
                            symbol_to_out[symbol] = symbol_index_to_out[symbol_index];
                        }
                        // one input column mapping to different column
                        if (symbol_index_to_out[symbol_index] != symbol_to_out[symbol])
                            return {};

                        symbol_index++;
                    }
                }
                else
                {
                    ASTPtr name = std::make_shared<ASTIdentifier>(symbol);
                    ASTPtr id = std::make_shared<ASTIdentifier>("$" + std::to_string(symbol_index));
                    expression_map[name] = ConstHashAST::make(id);
                    if (!filter)
                    {
                        if (!id_to_output.contains(id))
                            id_to_output[id] = ConstHashAST::make(name);
                    }
                    symbol_index++;
                }

            auto rewritten = ExpressionRewriter::rewrite(ast, expression_map);
            if (!filter)
            {
                filter = rewritten;
            }

            if (!ASTEquality::compareTree(filter, rewritten))
                return {};
            filter_count++;
        }
    }


    auto new_union_node
        = PlanNodeBase::createPlanNode(context.context->nextNodeId(), std::make_shared<UnionStep>(inputs, output, new_out2ins), lefts);

    if (filter_count != 0 && filter_count != node->getChildren().size())
    {
        LOG_DEBUG(log, "filter count");
        return {};
    }


    DataStream join_output = output;
    for (const auto & item : node->getChildren()[0]->getChildren()[1]->getCurrentDataStream().header)
    {
        join_output.header.insert(item);
    }

    if (filter)
    {
        filter = ExpressionRewriter::rewrite(filter, id_to_output);
    }

    auto new_join_node = PlanNodeBase::createPlanNode(
        context.context->nextNodeId(),
        std::make_shared<JoinStep>(
            DataStreams{new_union_node->getCurrentDataStream(), node->getChildren()[0]->getChildren()[1]->getCurrentDataStream()},
            join_output,
            kind,
            strictness,
            context.context->getSettingsRef().max_threads,
            context.context->getSettingsRef().optimize_read_in_order,
            left_keys_mapped[0],
            dynamic_cast<const JoinStep &>(*node->getChildren()[0]->getStep()).getRightKeys(),
            dynamic_cast<const JoinStep &>(*node->getChildren()[0]->getStep()).getKeyIdsNullSafe(),
            filter ? filter : PredicateConst::TRUE_VALUE),
        {new_union_node, node->getChildren()[0]->getChildren()[1]});


    Assignments assignments;
    NameToType name2type;
    bool all_in_source = true;
    for (const auto & item : union_step.getOutToInputs())
    {
        if (new_join_node->getCurrentDataStream().header.has(item.first))
        {
            assignments.emplace_back(item.first, std::make_shared<ASTIdentifier>(item.first));
            name2type[item.first] = new_join_node->getCurrentDataStream().header.getByName(item.first).type;
        }
        else
        {
            all_in_source = false;
            assignments.emplace_back(item.first, std::make_shared<ASTIdentifier>(item.second[0]));
            name2type[item.first] = new_join_node->getCurrentDataStream().header.getByName(item.second[0]).type;
        }
    }

    if (all_in_source)
        return new_join_node;

    return PlanNodeBase::createPlanNode(
        context.context->nextNodeId(),
        std::make_shared<ProjectionStep>(new_join_node->getCurrentDataStream(), assignments, name2type),
        {new_join_node});
}


ConstRefPatternPtr PushUnionThroughProjection::getPattern() const
{
    static auto pattern = Patterns::unionn().withAll(Patterns::project()).result();
    return pattern;
}

TransformResult PushUnionThroughProjection::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto & union_step = dynamic_cast<const UnionStep &>(*node->getStep());

    std::vector<ProjectionStep *> projections;
    PlanNodes new_children;
    DataStreams input_streams;
    for (auto & union_child : node->getChildren())
    {
        projections.emplace_back(dynamic_cast<ProjectionStep *>(union_child->getStep().get()));
        new_children.emplace_back(union_child->getChildren()[0]);
        input_streams.emplace_back(union_child->getChildren()[0]->getCurrentDataStream());
    }

    Assignments new_assigment;
    OutputToInputs new_out2ins;
    DataStream output_stream;
    NameToType name2type;
    std::unordered_map<String, String> symbol_to_out;
    for (const auto & item : union_step.getOutToInputs())
    {
        ASTPtr first_expr;
        std::unordered_map<size_t, String> symbol_index_to_out;
        for (size_t index = 0; index < item.second.size(); index++)
        {
            auto ast = projections[index]->getAssignments().at(item.second[index]);
            auto symbols = SymbolsExtractor::extractVector(ast);

            ConstASTMap expression_map;
            ConstASTMap id_to_output;
            size_t symbol_index = 0;

            for (auto symbol : symbols)
            {
                ASTPtr name = std::make_shared<ASTIdentifier>(symbol);
                ASTPtr id = std::make_shared<ASTIdentifier>("$" + std::to_string(symbol_index));
                if (!expression_map.contains(name))
                {
                    expression_map[name] = ConstHashAST::make(id);
                    if (!symbol_index_to_out.count(symbol_index))
                    {
                        symbol_index_to_out[symbol_index] = symbol_to_out.contains(symbol)
                            ? symbol_to_out[symbol]
                            : context.context->getSymbolAllocator()->newSymbol(symbol);

                        // new symbol
                        if (!symbol_to_out.contains(symbol))
                            output_stream.header.insert(
                                {projections[index]->getInputStreams()[0].header.getByName(symbol).type,
                                 symbol_index_to_out[symbol_index]});
                    }
                    id_to_output[id] = ConstHashAST::make(std::make_shared<ASTIdentifier>(symbol_index_to_out[symbol_index]));

                    if (!symbol_to_out.contains(symbol))
                    {
                        new_out2ins[symbol_index_to_out[symbol_index]].emplace_back(symbol);
                        symbol_to_out[symbol] = symbol_index_to_out[symbol_index];
                    }
                    // one input column mapping to different column
                    if (symbol_index_to_out[symbol_index] != symbol_to_out[symbol])
                        return {};

                    symbol_index++;
                }
            }

            auto rewritten = ExpressionRewriter::rewrite(ast, expression_map);
            if (!first_expr)
            {
                first_expr = rewritten;
                new_assigment[item.first] = ExpressionRewriter::rewrite(rewritten, id_to_output);
                name2type[item.first] = union_step.getOutputStream().header.getByName(item.first).type;
            }

            if (!ASTEquality::compareTree(first_expr, rewritten))
                return {};
        }
    }

    auto new_union_node = PlanNodeBase::createPlanNode(
        context.context->nextNodeId(), std::make_shared<UnionStep>(input_streams, output_stream, new_out2ins), new_children);

    return PlanNodeBase::createPlanNode(
        context.context->nextNodeId(),
        std::make_shared<ProjectionStep>(new_union_node->getCurrentDataStream(), new_assigment, name2type),
        {new_union_node});
}

ConstRefPatternPtr PushUnionThroughAgg::getPattern() const
{
    static auto pattern = Patterns::unionn().withAll(Patterns::aggregating()).result();
    return pattern;
}

TransformResult PushUnionThroughAgg::transformImpl(PlanNodePtr, const Captures &, RuleContext &)
{
    return {};
    // const auto & union_step = dynamic_cast<const UnionStep &>(*node->getStep());

    // std::vector<AggregatingStep *> aggs;
    // PlanNodes new_children;
    // DataStreams input_streams;
    // for (auto & union_child : node->getChildren())
    // {
    //     auto * agg = dynamic_cast<AggregatingStep *>(union_child->getStep().get());
    //     if (!agg->isFinal() || agg->isGroupingSet())
    //         return {};
    //     aggs.emplace_back(agg);
    //     new_children.emplace_back(union_child->getChildren()[0]);
    //     input_streams.emplace_back(union_child->getChildren()[0]->getCurrentDataStream());
    // }

    // std::vector<Names> before_symbols(union_step.getInputStreams().size());

    // // union
    // OutputToInputs new_out2ins;
    // DataStream output_stream;

    // // agg
    // AggregateDescriptions new_aggs;
    // Names new_keys;

    // size_t key_size = aggs[0]->getKeys().size();
    // size_t agg_size = aggs[0]->getAggregates().size();

    // auto keys = aggs[0]->getKeys();
    // NameSet first_keys{keys.begin(), keys.end()};
    // for (const auto & item : union_step.getOutToInputs())
    // {
    //     // if this column is group by key, check whether the group ky key index is the same
    //     if (first_keys.contains(item.second[0]))
    //     {
    //         size_t first_key_index = key_size;
    //         new_keys.emplace_back(item.first);
    //         for (size_t index = 0; index < item.second.size(); index++)
    //         {
    //             if (key_size != aggs[index]->getKeys().size())
    //                 return {};
    //             for (size_t key_index = 0; key_index < key_size; key_index++)
    //             {
    //                 if (item.second[index] == aggs[index]->getKeys()[key_index])
    //                 {
    //                     if (first_key_index == key_size)
    //                     {
    //                         first_key_index = key_index;
    //                     }
    //                     else if (first_key_index != key_index)
    //                     {
    //                         return {};
    //                     }
    //                 }
    //             }
    //         }
    //         new_out2ins[item.first] = item.second;
    //         output_stream.header.insert({union_step.getOutputStream().header.getByName(item.first).type, item.first});
    //     }
    //     else
    //     {
    //         // if this column is agg result
    //         size_t first_agg_index = agg_size;
    //         String func_name;
    //         std::unordered_map<size_t, String> symbol_index_to_out;
    //         size_t argument_size = 0;
    //         AggregateDescription new_agg;
    //         Names new_argments;
    //         for (size_t index = 0; index < item.second.size(); index++)
    //         {
    //             if (agg_size != aggs[index]->getAggregates().size())
    //                 return {};
    //             for (size_t func_index = 0; func_index < agg_size; func_index++)
    //             {
    //                 if (!aggs[index]->getAggregates()[func_index].mask_column.empty())
    //                     return {};
    //                 if (!aggs[index]->getAggregates()[func_index].parameters.empty())
    //                     return {};

    //                 if (item.second[index] == aggs[index]->getAggregates()[func_index].column_name)
    //                 {
    //                     if (first_agg_index == agg_size)
    //                     {
    //                         first_agg_index = func_index;
    //                         func_name = aggs[index]->getAggregates()[func_index].function->getName();
    //                         argument_size = aggs[index]->getAggregates()[func_index].argument_names.size();
    //                         new_agg = aggs[index]->getAggregates()[func_index];
    //                     }
    //                     else if (
    //                         first_agg_index != func_index || func_name != aggs[index]->getAggregates()[func_index].function->getName()
    //                         || argument_size != aggs[index]->getAggregates()[func_index].argument_names.size())
    //                     {
    //                         return {};
    //                     }


    //                     auto symbols = aggs[index]->getAggregates()[func_index].argument_names;

    //                     size_t symbol_index = 0;
    //                     for (auto symbol : symbols)
    //                     {
    //                         // expression_map[name] = ConstHashAST::make(id);
    //                         if (!symbol_index_to_out.count(symbol_index))
    //                         {
    //                             symbol_index_to_out[symbol_index] = context.context->getSymbolAllocator()->newSymbol(symbol);
    //                             output_stream.header.insert(
    //                                 {aggs[index]->getInputStreams()[0].header.getByName(symbol).type, symbol_index_to_out[symbol_index]});
    //                         }
    //                         if (index == 0)
    //                         {
    //                             new_argments.emplace_back(symbol_index_to_out[symbol_index]);
    //                         }
    //                         new_out2ins[symbol_index_to_out[symbol_index]].emplace_back(symbol);
    //                         symbol_index++;
    //                     }
    //                 }
    //             }
    //         }
    //         if (first_agg_index == agg_size)
    //             return {};
    //         new_agg.argument_names = new_argments;
    //         new_agg.column_name = item.first;
    //         new_aggs.emplace_back(new_agg);
    //     }
    // }

    // auto new_union_node = PlanNodeBase::createPlanNode(
    //     context.context->nextNodeId(), std::make_shared<UnionStep>(input_streams, output_stream, new_out2ins), new_children);

    // return PlanNodeBase::createPlanNode(
    //     context.context->nextNodeId(),
    //     std::make_shared<AggregatingStep>(
    //         new_union_node->getCurrentDataStream(),
    //         new_keys,
    //         NameSet{},
    //         new_aggs,
    //         GroupingSetsParamsList{},
    //         true,
    //         AggregateStagePolicy::DEFAULT,
    //         context.context),
    //     {new_union_node});
}


}
