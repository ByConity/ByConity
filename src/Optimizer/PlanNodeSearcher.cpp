#include <Optimizer/PlanNodeSearcher.h>

#include <Optimizer/Utils.h>
#include <QueryPlan/PlanNode.h>

namespace DB
{
std::optional<PlanNodePtr> PlanNodeSearcher::findFirstRecursive(const PlanNodePtr & curr) // NOLINT(misc-no-recursion)
{
    if (!predicate || predicate(*curr))
        return curr;

    if (!recurse_only_when || recurse_only_when(*curr))
        for (auto & child : curr->getChildren())
            if (auto found = findFirstRecursive(child))
                return found;
    return {};
}

std::optional<PlanNodePtr> PlanNodeSearcher::findSingle()
{
    auto all = findAll();
    if (all.empty())
        return {};
    Utils::checkArgument(all.size() == 1);
    return all.at(0);
}

PlanNodePtr PlanNodeSearcher::findOnlyElement()
{
    auto all = findAll();
    Utils::checkArgument(all.size() == 1);
    return all.at(0);
}

PlanNodePtr PlanNodeSearcher::findOnlyElementOr(const PlanNodePtr & default_)
{
    auto all = findAll();
    if (all.empty())
        return default_;
    Utils::checkArgument(all.size() == 1);
    return all.at(0);
}

void PlanNodeSearcher::findAllRecursive(const PlanNodePtr & curr, std::vector<PlanNodePtr> & result) // NOLINT(misc-no-recursion)
{
    if (!predicate || predicate(*curr))
    {
        result.emplace_back(curr);
    }
    if (!recurse_only_when || recurse_only_when(*curr))
        for (auto & child : curr->getChildren())
            findAllRecursive(child, result);
}

void PlanNodeSearcher::collectRecursive( // NOLINT(misc-no-recursion)
    const PlanNodePtr & curr, const std::function<void(PlanNodeBase &)> & consumer)
{
    if (!predicate || predicate(*curr))
        consumer(*curr);

    if (!recurse_only_when || recurse_only_when(*curr))
        for (const auto & child : curr->getChildren())
            collectRecursive(child, consumer);
}

}
