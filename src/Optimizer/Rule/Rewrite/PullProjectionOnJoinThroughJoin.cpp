#include <Optimizer/Rule/Rewrite/PullProjectionOnJoinThroughJoin.h>

#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/Utils.h>
#include <QueryPlan/JoinStep.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/SymbolMapper.h>

namespace DB
{

static std::optional<PlanNodePtr> tryPushJoinThroughLeftProjection(
    const PlanNodePtr & join,
    const PlanNodePtr & join_left_project,
    const PlanNodePtr & join_right_node,
    const PlanNodePtr & children,
    Context & context)
{
    const auto * join_step = dynamic_cast<const JoinStep *>(join->getStep().get());
    const auto * project_step = dynamic_cast<const ProjectionStep *>(join_left_project->getStep().get());

    NameSet left_names = join_step->getInputStreams()[0].header.getNameSet();

    std::unordered_map<String, String> projection_mapping;
    for (const auto & assignment : project_step->getAssignments())
    {
        if (assignment.second->getType() == ASTType::ASTIdentifier)
        {
            const ASTIdentifier & identifier = assignment.second->as<ASTIdentifier &>();
            if (left_names.contains(assignment.first))
            {
                projection_mapping.emplace(assignment.first, identifier.name());
            }
        }
    }

    if (left_names.size() != projection_mapping.size())
    {
        return {};
    }

    // construct join
    SymbolMapper symbol_mapper = SymbolMapper::symbolMapper(projection_mapping);
    auto mapped_join = symbol_mapper.map(*join_step);

    // construct projection
    Assignments assignments = project_step->getAssignments();
    NameToType name_to_type = project_step->getNameToType();
    for (const auto & name_and_type : join_right_node->getStep()->getOutputStream().header)
    {
        assignments.emplace_back(name_and_type.name, std::make_shared<ASTIdentifier>(name_and_type.name));
        name_to_type.emplace(name_and_type.name, name_and_type.type);
    }
    auto new_project_step
        = std::make_shared<ProjectionStep>(join_step->getOutputStream(), assignments, name_to_type, project_step->isFinalProject());

    return PlanNodeBase::createPlanNode(
        context.nextNodeId(), new_project_step, {PlanNodeBase::createPlanNode(context.nextNodeId(), mapped_join, {children, join_right_node})});
}

static std::optional<PlanNodePtr> tryPushJoinThroughRightProjection(
    const PlanNodePtr & join,
    const PlanNodePtr & join_right_project,
    const PlanNodePtr & join_left_node,
    const PlanNodePtr & children,
    Context & context)
{
    const auto * join_step = dynamic_cast<const JoinStep *>(join->getStep().get());
    const auto * project_step = dynamic_cast<const ProjectionStep *>(join_right_project->getStep().get());

    NameSet right_names = join_step->getInputStreams()[1].header.getNameSet();

    std::unordered_map<String, String> projection_mapping;
    for (const auto & assignment : project_step->getAssignments())
    {
        if (assignment.second->getType() == ASTType::ASTIdentifier)
        {
            const ASTIdentifier & identifier = assignment.second->as<ASTIdentifier &>();
            if (right_names.contains(assignment.first))
            {
                projection_mapping.emplace(assignment.first, identifier.name());
            }
        }
    }

    if (right_names.size() != projection_mapping.size())
    {
        return {};
    }

    // construct join
    SymbolMapper symbol_mapper = SymbolMapper::symbolMapper(projection_mapping);
    auto mapped_join = symbol_mapper.map(*join_step);

    // construct projection
    Assignments assignments{project_step->getAssignments().begin(), project_step->getAssignments().end()};
    NameToType name_to_type{project_step->getNameToType().begin(), project_step->getNameToType().end()};
    for (const auto & name_and_type : join_left_node->getStep()->getOutputStream().header)
    {
        assignments.emplace_back(name_and_type.name, std::make_shared<ASTIdentifier>(name_and_type.name));
        name_to_type.emplace(name_and_type.name, name_and_type.type);
    }
    auto new_project_step
        = std::make_shared<ProjectionStep>(join_step->getOutputStream(), assignments, name_to_type, project_step->isFinalProject());

    return PlanNodeBase::createPlanNode(
        context.nextNodeId(), new_project_step, {PlanNodeBase::createPlanNode(context.nextNodeId(), mapped_join, {join_left_node, children})});
}

static bool isProjectionWithJoin(const PlanNodePtr & node)
{
    return node->getStep()->getType() == IQueryPlanStep::Type::Projection
        && node->getChildren()[0]->getStep()->getType() == IQueryPlanStep::Type::Join;
}

PatternPtr PullProjectionOnJoinThroughJoin::getPattern() const
{
    return Patterns::join()->withAny(
        Patterns::project()
            // identity projection will be inlined into join
            ->matchingStep<ProjectionStep>([](const auto & step) { return !Utils::isIdentity(step.getAssignments()); })
            ->withSingle(Patterns::join()));
}

TransformResult PullProjectionOnJoinThroughJoin::transformImpl(PlanNodePtr node, const Captures &, RuleContext & context)
{
    const auto & join_step = dynamic_cast<const JoinStep &>(*node->getStep());

    if (isProjectionWithJoin(node->getChildren()[0])
        && (join_step.getKind() == ASTTableJoin::Kind::Inner || join_step.getKind() == ASTTableJoin::Kind::Left))
    {
        auto ret = tryPushJoinThroughLeftProjection(
            node, node->getChildren()[0], node->getChildren()[1], node->getChildren()[0]->getChildren()[0], *context.context);
        return TransformResult::of(ret);
    }

    if (isProjectionWithJoin(node->getChildren()[1])
        && (join_step.getKind() == ASTTableJoin::Kind::Inner || join_step.getKind() == ASTTableJoin::Kind::Right))
    {
        auto ret = tryPushJoinThroughRightProjection(
            node, node->getChildren()[1], node->getChildren()[0], node->getChildren()[1]->getChildren()[0], *context.context);
        return TransformResult::of(ret);
    }

    return {};
}

}
