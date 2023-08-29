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

#include <Analyzers/ASTEquals.h>
#include <Analyzers/Analysis.h>

#include <unordered_set>
#include <unordered_map>

namespace DB
{

namespace ASTEquality
{
    // Equals & hash for semantic comparison. Difference from syntactic comparison:
    //   1. when comparing ASTIdentifiers, check if they refer to a same column
    //   2. when comparing ASTIdentifiers comes from subquery, check them by syntax
    struct ScopeAwareEquals
    {
        Analysis * analysis;
        ScopePtr query_scope;

        ScopeAwareEquals(Analysis * analysis_, ScopePtr query_scope_) : analysis(analysis_), query_scope(query_scope_)
        {
        }

        std::optional<bool> equals(const ASTPtr & left, const ASTPtr & right) const;

        bool operator()(const ASTPtr & left, const ASTPtr & right) const
        {
            return compareTree(left, right, [&](const auto & l, const auto & r) { return equals(l, r); });
        }

        bool operator()(const ConstASTPtr & left, const ConstASTPtr & right) const
        {
            return operator()(std::const_pointer_cast<IAST>(left), std::const_pointer_cast<IAST>(right));
        }
    };

    struct ScopeAwareHash
    {
        Analysis * analysis;
        ScopePtr query_scope;

        ScopeAwareHash(Analysis * analysis_, ScopePtr query_scope_) : analysis(analysis_), query_scope(query_scope_)
        {
        }

        std::optional<size_t> hash(const ASTPtr & ast) const;

        size_t operator()(const ASTPtr & ast) const
        {
            return hashTree(ast, [&](const auto & a) { return hash(a); });
        }

        size_t operator()(const ConstASTPtr & ast) const
        {
            return operator()(std::const_pointer_cast<IAST>(ast));
        }
    };

}

using ScopeAwaredASTSet = std::unordered_set<ASTPtr, ASTEquality::ScopeAwareHash, ASTEquality::ScopeAwareEquals>;
template <typename T>
using ScopeAwaredASTMap = std::unordered_map<ASTPtr, T, ASTEquality::ScopeAwareHash, ASTEquality::ScopeAwareEquals>;

template <typename... Arg>
ScopeAwaredASTSet createScopeAwaredASTSetVariadic(Analysis & analysis, ScopePtr query_scope, Arg &&... arg)
{
    return ScopeAwaredASTSet(
        std::forward<Arg>(arg)...,
        ASTEquality::ScopeAwareHash(&analysis, query_scope),
        ASTEquality::ScopeAwareEquals(&analysis, query_scope));
}

template <typename T, typename... Arg>
ScopeAwaredASTMap<T> createScopeAwaredASTMapVariadic(Analysis & analysis, ScopePtr query_scope, Arg &&... arg)
{
    return ScopeAwaredASTMap<T>(
        std::forward<Arg>(arg)...,
        ASTEquality::ScopeAwareHash(&analysis, query_scope),
        ASTEquality::ScopeAwareEquals(&analysis, query_scope));
}

inline ScopeAwaredASTSet createScopeAwaredASTSet(Analysis & analysis, ScopePtr query_scope)
{
    return createScopeAwaredASTSetVariadic<size_t>(analysis, query_scope, 10);
}

template <typename T>
ScopeAwaredASTMap<T> createScopeAwaredASTMap(Analysis & analysis, ScopePtr query_scope)
{
    return createScopeAwaredASTMapVariadic<T, size_t>(analysis, query_scope, 10);
}
}
