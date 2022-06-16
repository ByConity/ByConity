//#include <Optimizer/Rule/Rewrite/PushThroughExchangeRules.h>
//
//#include <Optimizer/ExpressionDeterminism.h>
//#include <Optimizer/PlanNodeCardinality.h>
//#include <Optimizer/Rule/Pattern.h>
//#include <Optimizer/Rule/Patterns.h>
//#include <Optimizer/SymbolUtils.h>
//#include <Optimizer/SymbolsExtractor.h>
//#include <Processors/Transforms/AggregatingTransform.h>
//#include <QueryPlan/ExchangeStep.h>
//#include <QueryPlan/QueryPlan.h>
//
//namespace DB
//{
//PatternPtr PushProjectionThroughExchange::getPattern() const
//{
//    return Patterns::project()->withSingle(Patterns::exchange());
//}
//
//TransformResult PushProjectionThroughExchange::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
//{
//    const auto * step = dynamic_cast<const PartialSortingStep *>(node->getStep().get());
//    auto old_exchange_node = node->getChildren()[0];
//    const auto * old_exchange_step = dynamic_cast<const ExchangeStep *>(old_exchange_node->getStep().get());
//
//    PlanNodes exchange_children;
//    for (size_t index = 0; index < old_exchange_node->getChildren().size(); index++)
//    {
//        auto exchange_child = old_exchange_node->getChildren()[index];
//        if (dynamic_cast<MergeSortingNode *>(exchange_child.get()))
//        {
//            return {};
//        }
//
//        SortDescription new_sort_desc;
//        for (const auto & desc : step->getSortDescription())
//        {
//            auto new_desc = desc;
//            new_desc.column_name = old_exchange_step->getOutToInputs().at(desc.column_name).at(index);
//            new_sort_desc.emplace_back(new_desc);
//        }
//
//        auto before_exchange_partial_sort
//            = std::make_unique<PartialSortingStep>(exchange_child->getStep()->getOutputStream(), new_sort_desc, step->getLimit());
//        PlanNodes children{exchange_child};
//        auto before_exchange_partial_sort_node = PlanNodeBase::createPlanNode(
//            context.context->nextNodeId(), std::move(before_exchange_partial_sort), children, node->getStatistics());
//        auto before_exchange_merge_sort = std::make_unique<MergeSortingStep>(
//            before_exchange_partial_sort_node->getStep()->getOutputStream(), new_sort_desc, step->getLimit());
//        auto before_exchange_merge_sort_node = PlanNodeBase::createPlanNode(
//            context.context->nextNodeId(),
//            std::move(before_exchange_merge_sort),
//            PlanNodes{before_exchange_partial_sort_node},
//            node->getStatistics());
//        exchange_children.emplace_back(before_exchange_merge_sort_node);
//    }
//
//    auto exchange_step = old_exchange_step->copy(context.context);
//    auto exchange_node = PlanNodeBase::createPlanNode(
//        context.context->nextNodeId(), std::move(exchange_step), exchange_children, old_exchange_node->getStatistics());
//
//    QueryPlanStepPtr new_partial_sort = step->copy(context.context);
//    PlanNodes exchange{exchange_node};
//    auto new_partial_sort_node
//        = PlanNodeBase::createPlanNode(context.context->nextNodeId(), std::move(new_partial_sort), exchange, node->getStatistics());
//    return new_partial_sort_node;
//}
//}
