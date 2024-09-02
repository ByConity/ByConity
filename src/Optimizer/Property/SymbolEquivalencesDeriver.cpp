/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Optimizer/Property/SymbolEquivalencesDeriver.h>

#include <Optimizer/Utils.h>

namespace DB
{
SymbolEquivalencesPtr
SymbolEquivalencesDeriver::deriveEquivalences(QueryPlanStepPtr step, std::vector<SymbolEquivalencesPtr> children_equivalences)
{
    static SymbolEquivalencesDeriverVisitor derive;
    auto output_set = step->getOutputStream().header.getNameSet();

    // size_t index = 0;
    // if (step->getInputStreams().size() == children_equivalences.size())
    // {
    //     for (auto & item : children_equivalences)
    //     {
    //         auto symbols_set = step->getInputStreams()[index++].header.getNameSet();
    //         item = item->translate(symbols_set);
    //     }
    // }

    auto result = VisitorUtil::accept(step, derive, children_equivalences);
    result->createRepresentMap(output_set);
    return result;
}

SymbolEquivalencesPtr SymbolEquivalencesDeriverVisitor::visitStep(const IQueryPlanStep &, std::vector<SymbolEquivalencesPtr> &)
{
    return std::make_shared<SymbolEquivalences>();
}

SymbolEquivalencesPtr SymbolEquivalencesDeriverVisitor::visitJoinStep(const JoinStep & step, std::vector<SymbolEquivalencesPtr> & context)
{
    auto result = std::make_shared<SymbolEquivalences>(*context[0], *context[1]);

    if (step.getKind() == ASTTableJoin::Kind::Inner)
    {
        for (size_t index = 0; index < step.getLeftKeys().size(); index++)
        {
            result->add(step.getLeftKeys().at(index), step.getRightKeys().at(index));
        }
    }
    return result;
}

SymbolEquivalencesPtr SymbolEquivalencesDeriverVisitor::visitFilterStep(const FilterStep &, std::vector<SymbolEquivalencesPtr> & context)
{
    return context[0];
}

SymbolEquivalencesPtr
SymbolEquivalencesDeriverVisitor::visitProjectionStep(const ProjectionStep & step, std::vector<SymbolEquivalencesPtr> & context)
{
    const auto & assignments = step.getAssignments();
    std::unordered_map<String, String> identities = Utils::computeIdentityTranslations(assignments);
    for (auto & item : identities)
        context[0]->add(item.second, item.first);
    return context[0];
}

SymbolEquivalencesPtr
SymbolEquivalencesDeriverVisitor::visitAggregatingStep(const AggregatingStep &, std::vector<SymbolEquivalencesPtr> & context)
{
    return context[0];
}
SymbolEquivalencesPtr
SymbolEquivalencesDeriverVisitor::visitExchangeStep(const ExchangeStep &, std::vector<SymbolEquivalencesPtr> & context)
{
    return context[0];
}

SymbolEquivalencesPtr
SymbolEquivalencesDeriverVisitor::visitCTERefStep(const CTERefStep & step, std::vector<SymbolEquivalencesPtr> & context)
{
    if (!context.empty() && context[0])
    {
        auto mappings = step.getOutputColumns();
        for (const auto & mapping : mappings)
            context[0]->add(mapping.first, mapping.second);
        return context[0];
    }
    return std::make_shared<SymbolEquivalences>();
}

}
