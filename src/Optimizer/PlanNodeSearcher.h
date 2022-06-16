#pragma once

#include <QueryPlan/QueryPlan.h>

namespace DB
{

class PlanNodeSearcher
{
public:
    static PlanNodeSearcher searchFrom(const PlanNodePtr & node) { return PlanNodeSearcher{node}; }

    static PlanNodeSearcher searchFrom(QueryPlan & plan)
    {
        PlanNodes nodes;
        nodes.emplace_back(plan.getPlanNode());
        for (auto & cte : plan.getCTEInfo().getCTEs())
            nodes.emplace_back(cte.second);
        return PlanNodeSearcher{nodes};
    }

    PlanNodeSearcher & where(std::function<bool(PlanNodeBase &)> && predicate_)
    {
        predicate = predicate_;
        return *this;
    }

    PlanNodeSearcher & recurseOnlyWhen(std::function<bool(PlanNodeBase &)> && recurse_only_when_)
    {
        recurse_only_when = recurse_only_when_;
        return *this;
    }

    std::optional<PlanNodePtr> findFirst()
    {
        for (const auto & node : nodes)
            return findFirstRecursive(node);
        return {};
    }

    std::optional<PlanNodePtr> findSingle();

    /**
     * Return a list of matching nodes ordered as in pre-order traversal of the plan tree.
     */
    std::vector<PlanNodePtr> findAll()
    {
        std::vector<PlanNodePtr> result;
        for (const auto & node : nodes)
            findAllRecursive(node, result);
        return result;
    }

    PlanNodePtr findOnlyElement();

    PlanNodePtr findOnlyElementOr(const PlanNodePtr & default_);

    void findAllRecursive(const PlanNodePtr & curr, std::vector<PlanNodePtr> & nodes);

    bool matches() { return findFirst().has_value(); }

    size_t count() { return findAll().size(); }

    void collect(const std::function<void(PlanNodeBase &)> & consumer)
    {
        for (const auto & node : nodes)
            collectRecursive(node, consumer);
    }

private:
    explicit PlanNodeSearcher(const PlanNodePtr & node) : nodes({node}) { }
    explicit PlanNodeSearcher(PlanNodes nodes_) : nodes(std::move(nodes_)) { }

    std::optional<PlanNodePtr> findFirstRecursive(const PlanNodePtr & curr);
    void collectRecursive(const PlanNodePtr & curr, const std::function<void(PlanNodeBase &)> & consumer);

    PlanNodes nodes;
    std::function<bool(PlanNodeBase &)> predicate; // always true if not set
    std::function<bool(PlanNodeBase &)> recurse_only_when; // always true if not set
};

}
