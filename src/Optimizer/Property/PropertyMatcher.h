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

#include <Core/SortDescription.h>
#include <Optimizer/Property/Constants.h>
#include <Optimizer/Property/Property.h>
#include <Optimizer/Property/SymbolEquivalencesDeriver.h>

namespace DB
{
class PropertyMatcher
{
public:
    static bool matchNodePartitioning(
        const Context & context, Partitioning & required, const Partitioning & actual, const SymbolEquivalences & equivalences = {}, const Constants & constants = {});

    static bool matchStreamPartitioning(
        const Context & context, const Partitioning & required, const Partitioning & actual, const SymbolEquivalences & equivalences = {}, const Constants & constants = {}, bool match_local_exchange = true);

    static Sorting
    matchSorting(const Context & context, const Sorting & required, const Sorting & actual, const SymbolEquivalences & equivalences = {}, const Constants & constants = {});

    static Sorting matchSorting(
        const Context & context, const SortDescription & required, const Sorting & actual, const SymbolEquivalences & equivalences = {}, const Constants & constants = {});

    static Property compatibleCommonRequiredProperty(const std::unordered_set<Property, PropertyHash> & properties);
};
}
