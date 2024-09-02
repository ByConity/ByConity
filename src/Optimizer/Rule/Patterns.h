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

#include <Optimizer/Rule/Pattern.h>
#include <QueryPlan/IQueryPlanStep.h>
#include "QueryPlan/QueryPlan.h"

#include <boost/hana.hpp>

#include <sstream>

namespace DB::Patterns
{
class PatternBuilder
{
public:
    explicit PatternBuilder(PatternPtr init): current(std::move(init)) {}
    PatternPtr result() const { return std::move(current); }

    PatternBuilder & capturedAs(const Capture & capture);
    PatternBuilder & capturedAs(const Capture & capture, const PatternProperty & property);
    PatternBuilder & capturedAs(const Capture & capture, const PatternProperty & property, const std::string & name);
    template <typename T>
    PatternBuilder & capturedStepAs(const Capture & capture, const std::function<std::any(const T &)> & step_property)
    {
        return capturedStepAs(capture, step_property, "unknown");
    }
    template <typename T>
    PatternBuilder & capturedStepAs(const Capture & capture, const std::function<std::any(const T &)> & step_property, const std::string & name)
    {
        static_assert(std::is_base_of<IQueryPlanStep, T>::value, "T must inherit from IQueryPlanStep");

        PatternProperty property = [step_property](const PlanNodePtr & node) -> std::any {
            auto * step = dynamic_cast<const T *>(node->getStep().get());

            if (!step)
                throw Exception("Unexpected plan step found in pattern matching", ErrorCodes::LOGICAL_ERROR);

            return step_property(*step);
        };
        return capturedAs(capture, property, name);
    }
    PatternBuilder & matching(PatternPredicate predicate);
    PatternBuilder & matching(PatternPredicate predicate, const std::string & name);
    PatternBuilder & matchingCapture(std::function<bool(const Captures &)> capture_predicate);
    PatternBuilder & matchingCapture(std::function<bool(const Captures &)> capture_predicate, const std::string & name);
    template <typename T, typename F>
    PatternBuilder & matchingStep(F step_predicate)
    {
        return matchingStep<T>(std::move(step_predicate), "unknown");
    }
    template <typename T, typename F>
    PatternBuilder & matchingStep(F step_predicate, const std::string & name)
    {
        static_assert(std::is_base_of<const IQueryPlanStep, T>::value, "T must inherit from const IQueryPlanStep");

        PatternPredicate predicate = [step_predicate = std::move(step_predicate)](const QueryPlanStepPtr & istep, Captures & captures) -> bool {
            auto * step = dynamic_cast<const T *>(istep.get());

            if (!step)
                throw Exception("Unexpected plan step found in pattern matching", ErrorCodes::LOGICAL_ERROR);

            constexpr auto func_type1 = boost::hana::is_valid([](auto && x) -> decltype(x(*step)) {});

            if constexpr (decltype(func_type1(step_predicate))::value)
                return step_predicate(*step);
            else
                return step_predicate(*step, captures);
        };

        return matching(std::move(predicate), name);
    }

    PatternBuilder & withEmpty();
    PatternBuilder & withSingle(const PatternBuilder & sub_builder) { return withSingle(sub_builder.result());}
    PatternBuilder & withAny(const PatternBuilder & sub_builder) { return withAny(sub_builder.result());}
    PatternBuilder & withAll(const PatternBuilder & sub_builder) { return withAll(sub_builder.result());}
    template <typename ... T>
    PatternBuilder & with(const T &... sub_builders)
    {
        PatternPtrs sub_patterns;
        ( (sub_patterns.emplace_back(sub_builders.result())), ...);
        return with(std::move(sub_patterns));
    }

    template <typename... T>
    PatternBuilder & oneOf(const T &... sub_builders)
    {
        PatternPtrs sub_patterns;
        ((sub_patterns.emplace_back(sub_builders.result())), ...);
        return oneOf(std::move(sub_patterns));
    }

private:
    PatternBuilder & withSingle(PatternPtr sub_pattern);
    PatternBuilder & withAny(PatternPtr sub_pattern);
    PatternBuilder & withAll(PatternPtr sub_pattern);
    PatternBuilder & with(PatternPtrs sub_patterns);
    PatternBuilder & oneOf(PatternPtrs sub_patterns);

    mutable PatternPtr current;
};

// typeOf patterns
inline PatternBuilder typeOf(IQueryPlanStep::Type type) { return PatternBuilder(std::make_unique<TypeOfPattern>(type)); }
inline PatternBuilder any() { return typeOf(IQueryPlanStep::Type::Any); }
inline PatternBuilder tree() { return typeOf(IQueryPlanStep::Type::Tree); }

inline PatternBuilder project() { return typeOf(IQueryPlanStep::Type::Projection); }
inline PatternBuilder filter() { return typeOf(IQueryPlanStep::Type::Filter); }
inline PatternBuilder join() { return typeOf(IQueryPlanStep::Type::Join); }
inline PatternBuilder multiJoin() { return typeOf(IQueryPlanStep::Type::MultiJoin); }
inline PatternBuilder aggregating() { return typeOf(IQueryPlanStep::Type::Aggregating); }
inline PatternBuilder window() { return typeOf(IQueryPlanStep::Type::Window); }
inline PatternBuilder mergingAggregated() { return typeOf(IQueryPlanStep::Type::MergingAggregated); }
inline PatternBuilder unionn() { return typeOf(IQueryPlanStep::Type::Union); }
inline PatternBuilder intersect() { return typeOf(IQueryPlanStep::Type::Intersect); }
inline PatternBuilder except() { return typeOf(IQueryPlanStep::Type::Except); }
inline PatternBuilder exchange() { return typeOf(IQueryPlanStep::Type::Exchange); }
inline PatternBuilder remoteSource() { return typeOf(IQueryPlanStep::Type::RemoteExchangeSource); }
inline PatternBuilder tableScan() { return typeOf(IQueryPlanStep::Type::TableScan); }
inline PatternBuilder readNothing() { return typeOf(IQueryPlanStep::Type::ReadNothing); }
inline PatternBuilder limit() { return typeOf(IQueryPlanStep::Type::Limit); }
inline PatternBuilder limitBy() { return typeOf(IQueryPlanStep::Type::LimitBy); }
inline PatternBuilder sorting() { return typeOf(IQueryPlanStep::Type::Sorting); }
inline PatternBuilder mergeSorting() { return typeOf(IQueryPlanStep::Type::MergeSorting); }
inline PatternBuilder partialSorting() { return typeOf(IQueryPlanStep::Type::PartialSorting); }
inline PatternBuilder mergingSorted() { return typeOf(IQueryPlanStep::Type::MergingSorted); }
//inline PatternPtr materializing() { return typeOf(IQueryPlanStep::Type::Materializing); }
inline PatternBuilder distinct() { return typeOf(IQueryPlanStep::Type::Distinct); }
inline PatternBuilder extremes() { return typeOf(IQueryPlanStep::Type::Extremes); }
inline PatternBuilder apply() { return typeOf(IQueryPlanStep::Type::Apply); }
inline PatternBuilder enforceSingleRow() { return typeOf(IQueryPlanStep::Type::EnforceSingleRow); }
inline PatternBuilder assignUniqueId() { return typeOf(IQueryPlanStep::Type::AssignUniqueId); }
inline PatternBuilder cte() { return typeOf(IQueryPlanStep::Type::CTERef); }
inline PatternBuilder buffer() { return typeOf(IQueryPlanStep::Type::Buffer); }
PatternBuilder topN();
inline PatternBuilder topNFiltering() { return typeOf(IQueryPlanStep::Type::TopNFiltering); }
inline PatternBuilder explainAnalyze() { return typeOf(IQueryPlanStep::Type::ExplainAnalyze); }

template <typename... T>
PatternBuilder oneOf(const T &... sub_builders)
{
    PatternPtrs sub_patterns;
    ((sub_patterns.emplace_back(sub_builders.result())), ...);
    return PatternBuilder(std::make_unique<OneOfPattern>(std::move(sub_patterns)));
}

// miscellaneous
inline PatternPredicate predicateNot(const PatternPredicate & predicate)
{
    return [=](const QueryPlanStepPtr & node, Captures & captures) -> bool {return !predicate(node, captures);};
}

}
