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

#pragma once

#include <Optimizer/Property/Equivalences.h>
#include <QueryPlan/PlanVisitor.h>

#include <utility>

namespace DB
{
using SymbolEquivalences = Equivalences<String>;
using SymbolEquivalencesPtr = std::shared_ptr<SymbolEquivalences>;

class SymbolEquivalencesDeriver
{
public:
    static SymbolEquivalencesPtr deriveEquivalences(QueryPlanStepPtr step, std::vector<SymbolEquivalencesPtr> children_equivalences);
};

class SymbolEquivalencesDeriverVisitor : public StepVisitor<SymbolEquivalencesPtr, std::vector<SymbolEquivalencesPtr>>
{
public:
    SymbolEquivalencesPtr visitStep(const IQueryPlanStep & step, std::vector<SymbolEquivalencesPtr> & c) override;
    SymbolEquivalencesPtr visitJoinStep(const JoinStep & step, std::vector<SymbolEquivalencesPtr> & context) override;
    SymbolEquivalencesPtr visitFilterStep(const FilterStep & step, std::vector<SymbolEquivalencesPtr> & context) override;
    SymbolEquivalencesPtr visitProjectionStep(const ProjectionStep & step, std::vector<SymbolEquivalencesPtr> & context) override;
    SymbolEquivalencesPtr visitAggregatingStep(const AggregatingStep & step, std::vector<SymbolEquivalencesPtr> & context) override;
    SymbolEquivalencesPtr visitExchangeStep(const ExchangeStep & step, std::vector<SymbolEquivalencesPtr> & context) override;
SymbolEquivalencesPtr visitCTERefStep(const CTERefStep & step, std::vector<SymbolEquivalencesPtr> & context) override;
};
}
