#include <Optimizer/Equivalences.h>
#include <Optimizer/Utils.h>

namespace DB
{
EquivalencesPtr EquivalencesDerive::deriveEquivalences(ConstQueryPlanStepPtr step, std::vector<EquivalencesPtr> children_equivalences)
{
    static EquivalencesDeriveVisitor derive;
    return VisitorUtil::accept(step, derive, children_equivalences);
}

EquivalencesPtr EquivalencesDeriveVisitor::visitStep(const IQueryPlanStep &, std::vector<EquivalencesPtr> &)
{
    return std::make_shared<Equivalences>();
}

EquivalencesPtr EquivalencesDeriveVisitor::visitJoinStep(const JoinStep & step, std::vector<EquivalencesPtr> & context)
{
    auto result = std::make_shared<Equivalences>(*context[0], *context[1]);

    if (step.getKind() == ASTTableJoin::Kind::Inner)
    {
        for (size_t index = 0; index < step.getLeftKeys().size(); index++)
        {
            result->add(step.getLeftKeys().at(index), step.getRightKeys().at(index));
        }
    }
    return result;
}

EquivalencesPtr EquivalencesDeriveVisitor::visitFilterStep(const FilterStep &, std::vector<EquivalencesPtr> & context)
{
    return context[0];
}

EquivalencesPtr EquivalencesDeriveVisitor::visitProjectionStep(const ProjectionStep & step, std::vector<EquivalencesPtr> & context)
{
    auto assignments = step.getAssignments();
    std::unordered_map<String, String> identities = Utils::computeIdentityTranslations(assignments);
    std::unordered_map<String, String> revert_identifies;

    for (auto & item : identities)
    {
        revert_identifies[item.second] = item.first;
    }

    return context[0]->translate(identities);
}

EquivalencesPtr EquivalencesDeriveVisitor::visitAggregatingStep(const AggregatingStep & step, std::vector<EquivalencesPtr> & context)
{
    NameSet set{step.getKeys().begin(), step.getKeys().end()};
    return context[0]->translate(set);
}
EquivalencesPtr EquivalencesDeriveVisitor::visitExchangeStep(const ExchangeStep &, std::vector<EquivalencesPtr> & context)
{
    return context[0];
}

}
