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
    //   1. when comparing ASTIdentifiers which represent column references, check if they are from a same column
    struct ScopeAwareEquals
    {
        Analysis * analysis;

        ScopeAwareEquals(Analysis * analysis_) : analysis(analysis_)
        {}

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

        ScopeAwareHash(Analysis * analysis_) : analysis(analysis_)
        {}

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

inline ScopeAwaredASTSet createScopeAwaredASTSet(Analysis & analysis)
{
    return ScopeAwaredASTSet {10, ASTEquality::ScopeAwareHash(&analysis), ASTEquality::ScopeAwareEquals(&analysis)};
}

template<typename T>
inline ScopeAwaredASTMap<T> createScopeAwaredASTMap(Analysis & analysis)
{
    return ScopeAwaredASTMap<T> {10, ASTEquality::ScopeAwareHash(&analysis), ASTEquality::ScopeAwareEquals(&analysis)};
}

}
