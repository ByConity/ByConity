#include <Optimizer/Rule/Rewrite/SimplifyExpressionRules.h>

#include <Optimizer/ExpressionInterpreter.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/SimplifyExpressions.h>
#include <Optimizer/UnwrapCastInComparison.h>
#include <Optimizer/LiteralEncoder.h>
#include <Optimizer/Utils.h>
#include <Parsers/formatAST.h>
#include <QueryPlan/FilterStep.h>
#include <QueryPlan/JoinStep.h>

namespace DB
{
PatternPtr CommonPredicateRewriteRule::getPattern() const
{
    return Patterns::filter();
}

TransformResult CommonPredicateRewriteRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    if (!context->getSettingsRef().enable_common_predicate_rewrite)
    {
        return {};
    }

    auto * old_filter_node = dynamic_cast<FilterNode *>(node.get());
    if (!old_filter_node)
        return {};

    const auto & step = *old_filter_node->getStep();
    auto predicate = step.getFilter();

    ConstASTPtr rewritten = CommonPredicatesRewriter::rewrite(predicate, context);
    if (rewritten->getColumnName() == predicate->getColumnName())
    {
        return {};
    }

    auto filter_step
        = std::make_shared<FilterStep>(node->getChildren()[0]->getStep()->getOutputStream(), rewritten, step.removesFilterColumn());
    auto filter_node = FilterNode::createPlanNode(context->nextNodeId(), std::move(filter_step), PlanNodes{node->getChildren()[0]});

    return filter_node;
}

PatternPtr SwapPredicateRewriteRule::getPattern() const
{
    return Patterns::filter();
}

TransformResult SwapPredicateRewriteRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    if (!context->getSettingsRef().enable_swap_predicate_rewrite)
    {
        return {};
    }
    auto * old_filter_node = dynamic_cast<FilterNode *>(node.get());
    if (!old_filter_node)
        return {};

    const auto & step = *old_filter_node->getStep();
    const auto & predicate = step.getFilter();

    ConstASTPtr rewritten = SwapPredicateRewriter::rewrite(predicate, context);
    if (rewritten->getColumnName() == predicate->getColumnName())
    {
        return {};
    }

    auto filter_step
        = std::make_shared<FilterStep>(node->getChildren()[0]->getStep()->getOutputStream(), rewritten, step.removesFilterColumn());
    auto filter_node = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(filter_step), PlanNodes{node->getChildren()[0]});

    return filter_node;
}

PatternPtr SimplifyPredicateRewriteRule::getPattern() const
{
    return Patterns::filter();
}

TransformResult SimplifyPredicateRewriteRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    auto * old_filter_node = dynamic_cast<FilterNode *>(node.get());
    if (!old_filter_node)
        return {};

    const auto & step = *old_filter_node->getStep();
    auto predicate = step.getFilter();

    ConstASTPtr rewritten = ExpressionInterpreter::optimizePredicate(predicate, context, step.getOutputStream().header.getNamesAndTypes());

    if (PredicateUtils::isTruePredicate(rewritten))
        return node->getChildren()[0];

    if (rewritten->getColumnName() == predicate->getColumnName())
        return {};

    auto filter_step
        = std::make_shared<FilterStep>(node->getChildren()[0]->getStep()->getOutputStream(), rewritten, step.removesFilterColumn());
    auto filter_node = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(filter_step), PlanNodes{node->getChildren()[0]});

    return filter_node;
}

PatternPtr UnWarpCastInPredicateRewriteRule::getPattern() const
{
    return Patterns::filter();
}

TransformResult UnWarpCastInPredicateRewriteRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;
    if (!context->getSettingsRef().enable_unwarp_cast_in)
    {
        return {};
    }
    auto * old_filter_node = dynamic_cast<FilterNode *>(node.get());
    if (!old_filter_node)
        return {};

    const auto & step = *old_filter_node->getStep();
    auto predicate = step.getFilter();

    NamesAndTypes column_types = step.getOutputStream().header.getNamesAndTypes();
    ASTPtr rewritten = unwrapCastInComparison(predicate, context, column_types);
    if (!rewritten)
    {
        rewritten = predicate->clone();
    }

    if (rewritten->getColumnName() == predicate->getColumnName())
    {
        return {};
    }

    auto filter_step
        = std::make_shared<FilterStep>(node->getChildren()[0]->getStep()->getOutputStream(), rewritten, step.removesFilterColumn());
    auto filter_node = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(filter_step), PlanNodes{node->getChildren()[0]});

    return filter_node;
}

PatternPtr SimplifyJoinFilterRewriteRule::getPattern() const
{
    return Patterns::join()->matchingStep<JoinStep>([&](const JoinStep & s) { return !PredicateUtils::isTruePredicate(s.getFilter()); });
}

TransformResult SimplifyJoinFilterRewriteRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;

    auto * old_join_node = dynamic_cast<JoinNode *>(node.get());
    if (!old_join_node)
        return {};

    const auto & step = *old_join_node->getStep();
    auto filter = step.getFilter();

    NamesAndTypes column_types;

    bool has_outer_join_semantic = isAny(step.getStrictness()) || isAll(step.getStrictness()) || isAsof(step.getStrictness());
    bool make_nullable_for_left = has_outer_join_semantic && isRightOrFull(step.getKind());
    bool make_nullable_for_right = has_outer_join_semantic && isLeftOrFull(step.getKind());

    auto type_with_nullable = [&](bool make_nullable, const NamesAndTypes & header) {
        if (make_nullable)
        {
            for (const auto & column : header)
            {
                if (column.type->canBeInsideNullable())
                {
                    NameAndTypePair name_and_type{column.name, makeNullable(column.type)};
                    column_types.emplace_back(name_and_type);
                }
                else
                {
                    column_types.emplace_back(column);
                }
            }
        }
        else
        {
            column_types.insert(column_types.end(), header.begin(), header.end());
        }
    };

    type_with_nullable(make_nullable_for_left, step.getInputStreams()[0].header.getNamesAndTypes());
    type_with_nullable(make_nullable_for_right, step.getInputStreams()[1].header.getNamesAndTypes());

    ASTPtr rewritten = ExpressionInterpreter::optimizePredicate(filter, context, column_types);

    if (rewritten->getColumnName() == filter->getColumnName())
    {
        return {};
    }

    QueryPlanStepPtr join_step = std::make_shared<JoinStep>(
        step.getInputStreams(),
        step.getOutputStream(),
        step.getKind(),
        step.getStrictness(),
        step.getLeftKeys(),
        step.getRightKeys(),
        rewritten,
        step.isHasUsing(),
        step.getRequireRightKeys(),
        step.getAsofInequality(),
        step.getDistributionType());

    PlanNodePtr join_node = PlanNodeBase::createPlanNode(context->nextNodeId(), std::move(join_step), node->getChildren());
    return join_node;
}

PatternPtr SimplifyExpressionRewriteRule::getPattern() const
{
    return Patterns::project();
}

TransformResult SimplifyExpressionRewriteRule::transformImpl(PlanNodePtr node, const Captures &, RuleContext & rule_context)
{
    auto & context = rule_context.context;

    const auto * project = dynamic_cast<const ProjectionStep *>(node->getStep().get());
    if (!project)
        return {};

    Assignments assignments;
    NameToType name_to_type;
    ExpressionInterpreterSettings settings{.identifier_resolver = ExpressionInterpreter::no_op_resolver};
    auto column_types = node->getChildren()[0]->getCurrentDataStream().header.getNamesAndTypes();
    auto type_analyzer = TypeAnalyzer::create(rule_context.context, column_types);

    bool rewrite = false;
    for (const auto & assignment : project->getAssignments())
    {
        auto res = ExpressionInterpreter::evaluate(
            assignment.second, rule_context.context, type_analyzer, settings);
        if (!res.first)
        {
            assignments.emplace_back(assignment);
            name_to_type.emplace(assignment.first, project->getNameToType().at(assignment.first));
            continue;
        }
        name_to_type.emplace(assignment.first, res.first);
        if (std::holds_alternative<ASTPtr>(res.second))
            assignments.emplace_back(assignment.first, std::get<ASTPtr>(res.second));
        else if (std::holds_alternative<Field>(res.second))
            assignments.emplace_back(assignment.first,  LiteralEncoder::encode(std::get<Field>(res.second), res.first, context));
        else
            throw Exception("Unexpected result of ExpressionInterpreter::evaluate", ErrorCodes::LOGICAL_ERROR);

        if (!ASTEquality::compareTree(assignments.back().second, assignment.second))
            rewrite = true;
    }
    if (!rewrite)
        return {};

    return PlanNodeBase::createPlanNode(
        context->nextNodeId(),
        std::make_shared<ProjectionStep>(
            node->getChildren()[0]->getStep()->getOutputStream(),
            assignments,
            name_to_type,
            project->isFinalProject(),
            project->getDynamicFilters()),
        PlanNodes{node->getChildren()[0]});
}
}
