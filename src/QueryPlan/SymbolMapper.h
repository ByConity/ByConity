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

#include <memory>
#include <Core/Names.h>
#include <Functions/IFunction.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/WindowDescription.h>
#include <Optimizer/Property/Property.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/Assignment.h>
#include <QueryPlan/PlanVisitor.h>

#include <memory>
#include <string>
#include <unordered_map>

namespace DB
{

/**
 * Copy a step and replace its all symbol using mapping_function.
 */
class SymbolMapper
{
public:
    using Symbol = std::string;
    using MappingFunction = std::function<Symbol(const Symbol &)>;

    explicit SymbolMapper(MappingFunction mapping_function_) : mapping_function(std::move(mapping_function_)) { }

    /**
     * replace symbol using mapping
     * eg, if mapping: [a => a_1, a_1 => a_2], input: a, output: a_1
     */
    static SymbolMapper simpleMapper(std::unordered_map<Symbol, Symbol> & mapping);

    /**
     * replace symbol recursively using mapping
     * eg, if mapping: [a => a_1, a_1 => a_2], input: a, output: a_2
     */
    static SymbolMapper symbolMapper(std::unordered_map<Symbol, Symbol> & mapping);

    /**
     * replace symbol recursively using mapping.
     * if symbol is not record in mapping, create a new symbol using symbolAllocator.
     * eg, if mapping: [a => a_1, a_1 => a_2], input: b, output: b_2; input: a, output: a_2.
     */
    static SymbolMapper symbolReallocator(std::unordered_map<Symbol, Symbol> & mapping, SymbolAllocator & symbolAllocator);

    std::string map(const Symbol & symbol) { return mapping_function(symbol); }
    template <typename T>
    std::vector<T> map(const std::vector<T> & items)
    {
        std::vector<T> ret;
        std::transform(items.begin(), items.end(), std::back_inserter(ret), [&](const auto & param) { return map(param); });
        return ret;
    }

    static Names distinct(const Names & items)
    {
        Names ret;
        std::unordered_set<String> set;
        for (const auto & item : items)
            if (set.emplace(item).second)
                ret.emplace_back(item);
        return ret;
    }

    NameSet mapToDistinct(const Names & symbols);
    NamesAndTypes map(const NamesAndTypes & name_and_types);
    NameSet map(const NameSet & names);

    NameToType map(const NameToType & name_to_type);
    NamesWithAliases map(const NamesWithAliases & name_with_aliases);
    Assignments map(const Assignments & assignments);
    Assignment map(const Assignment & assignment);
    Block map(const Block & name_and_types);
    DataStream map(const DataStream & data_stream);
    ASTPtr map(const ASTPtr & expr);
    ASTPtr map(const ConstASTPtr & expr);
    Partitioning map(const Partitioning & partition);
    AggregateDescription map(const AggregateDescription & desc);
    GroupingSetsParams map(const GroupingSetsParams & param);
    WindowFunctionDescription map(const WindowFunctionDescription & desc);
    WindowDescription map(const WindowDescription & desc);
    SortColumnDescription map(const SortColumnDescription & desc);
    Aggregator::Params map(const Aggregator::Params & params);
    AggregatingTransformParamsPtr map(const AggregatingTransformParamsPtr & param);
    ArrayJoinActionPtr map(const ArrayJoinActionPtr & array_join_action);
    GroupingDescription map(const GroupingDescription & desc);
    SortDescription map(const SortDescription & sort_desc);
    std::map<Int32, Names> map(const std::map<Int32, Names> & group_id_non_null_symbol);

    LinkedHashMap<String, RuntimeFilterBuildInfos> map(const LinkedHashMap<String, RuntimeFilterBuildInfos> & infos);
    PlanNodeStatisticsEstimate map(const PlanNodeStatisticsEstimate & estimate);

#define VISITOR_DEF(TYPE) std::shared_ptr<TYPE##Step> map(const TYPE##Step &);
    APPLY_STEP_TYPES(VISITOR_DEF)
#undef VISITOR_DEF

    QueryPlanStepPtr map(const IQueryPlanStep & step);

private:
    MappingFunction mapping_function;
    class IdentifierRewriter;
    class SymbolMapperVisitor;
};

}
