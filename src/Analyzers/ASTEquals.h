#pragma once

#include <Parsers/IAST.h>

#include <functional>
#include <optional>
#include <unordered_set>
#include <unordered_map>

namespace DB
{

namespace ASTEquality
{

    using SubtreeComparator = std::function<std::optional<bool>(const ASTPtr &, const ASTPtr &)>;
    using SubtreeHasher = std::function<std::optional<size_t>(const ASTPtr &)>;

    // compare ASTs by extra comparison strategy first, if not applicable, compare AST by syntax
    bool compareTree(const ASTPtr & left, const ASTPtr & right, const SubtreeComparator & comparator);

    inline bool compareTree(const ASTPtr & left, const ASTPtr & right)
    {
        return compareTree(left, right, [](auto &, auto &) {return std::nullopt;});
    }

    inline bool compareTree(const ConstASTPtr & left, const ConstASTPtr & right)
    {
        return compareTree(std::const_pointer_cast<IAST>(left), std::const_pointer_cast<IAST>(right));
    }

    size_t hashTree(const ASTPtr & ast, const SubtreeHasher & hasher);

    inline size_t hashTree(const ASTPtr & ast)
    {
        return hashTree(ast, [](auto &) {return std::nullopt;});
    }

    // Equals & hash for syntactic comparison
    struct ASTEquals
    {
        bool operator()(const ASTPtr & left, const ASTPtr & right) const
        {
            return compareTree(left, right);
        }

        bool operator()(const ConstASTPtr & left, const ConstASTPtr & right) const
        {
            return compareTree(left, right);
        }
    };

    struct ASTHash
    {
        size_t operator()(const ASTPtr & ast) const
        {
            return hashTree(ast);
        }

        size_t operator()(const ConstASTPtr & ast) const
        {
            return hashTree(std::const_pointer_cast<IAST>(ast));
        }
    };
}

template <typename ASTType = ASTPtr>
using ASTSet = std::unordered_set<ASTType, ASTEquality::ASTHash, ASTEquality::ASTEquals>;

template <typename T, typename ASTType = ASTPtr>
using ASTMap = std::unordered_map<ASTType, T, ASTEquality::ASTHash, ASTEquality::ASTEquals>;

}
