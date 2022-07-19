#pragma once

#include <Optimizer/Rule/Transformation/JoinEnumOnGraph.h>
#include <QueryPlan/PlanVisitor.h>

#include <utility>

namespace DB
{
class Equivalences;
using EquivalencesPtr = std::shared_ptr<Equivalences>;

class Equivalences
{
public:
    Equivalences() { }
    Equivalences(Equivalences & left, Equivalences & right) : union_find(left.union_find, right.union_find) { }

    void add(String first, String second) { union_find.add(std::move(first), std::move(second)); }
    EquivalencesPtr translate(std::unordered_map<String, String> & identities)
    {
        auto result = std::make_shared<Equivalences>();
        std::unordered_map<String, std::unordered_set<String>> str_to_set;
        for (auto & item : union_find.parent)
        {
            if (identities.contains(item.first))
            {
                str_to_set[item.second].insert(identities[item.first]);
            }
        }

        for (auto & item : str_to_set)
        {
            auto & set = item.second;
            if (set.size() > 1)
            {
                auto first = *set.begin();
                for (auto iter = set.begin()++; iter != set.end(); iter++)
                {
                    result->add(first, *iter);
                }
            }
        }
        return result;
    }

    EquivalencesPtr translate(std::unordered_set<String> & identities)
    {
        auto result = std::make_shared<Equivalences>();
        std::unordered_map<String, std::unordered_set<String>> str_to_set;
        for (auto & item : union_find.parent)
        {
            if (identities.contains(item.first))
            {
                str_to_set[item.second].insert(item.first);
            }
        }

        for (auto & item : str_to_set)
        {
            auto & set = item.second;
            if (set.size() > 1)
            {
                auto first = *set.begin();
                for (auto iter = set.begin()++; iter != set.end(); iter++)
                {
                    result->add(first, *iter);
                }
            }
        }
        return result;
    }

    NameToNameMap representMap() const
    {
        std::unordered_map<String, std::unordered_set<String>> str_to_set;
        for (auto & item : union_find.parent)
        {
            str_to_set[item.second].insert(item.first);
        }


        NameToNameMap result;
        for (auto & item : str_to_set)
        {
            auto & set = item.second;
            auto min = *std::min_element(set.begin(), set.end());
            for (auto & str : set)
            {
                result[str] = min;
            }
        }
        return result;
    }

private:
    UnionFind union_find;
};


class EquivalencesDerive
{
public:
    static EquivalencesPtr deriveEquivalences(ConstQueryPlanStepPtr step, std::vector<EquivalencesPtr> children_equivalences);
};

class EquivalencesDeriveVisitor : public StepVisitor<EquivalencesPtr, std::vector<EquivalencesPtr>>
{
public:
    EquivalencesPtr visitStep(const IQueryPlanStep & step, std::vector<EquivalencesPtr> & c) override;
    EquivalencesPtr visitJoinStep(const JoinStep & step, std::vector<EquivalencesPtr> & context) override;
    EquivalencesPtr visitFilterStep(const FilterStep & step, std::vector<EquivalencesPtr> & context) override;
    EquivalencesPtr visitProjectionStep(const ProjectionStep & step, std::vector<EquivalencesPtr> & context) override;
    EquivalencesPtr visitAggregatingStep(const AggregatingStep & step, std::vector<EquivalencesPtr> & context) override;
    EquivalencesPtr visitExchangeStep(const ExchangeStep & step, std::vector<EquivalencesPtr> & context) override;
};
}
