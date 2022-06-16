#pragma once

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <QueryPlan/Assignment.h>

#include <unordered_map>

namespace DB
{
class ProjectionStep;

namespace Utils
{
    void assertIff(bool expression1, bool expression2);
    void checkState(bool expression);
    void checkState(bool expression, const String & msg);
    void checkArgument(bool expression);
    void checkArgument(bool expression, const String & msg);
    bool isIdentity(const Assignments & assignments);
    bool isIdentity(const ProjectionStep & project);
    std::unordered_map<String, String> computeIdentityTranslations(Assignments & assignments);

    // this method is used to deal with function names which are case-insensitive or have an alias to.
    // should be called after `registerFunctions`
    bool checkFunctionName(const ASTFunction & function, const String & expect_name);
    inline bool checkFunctionName(const ASTPtr & function_ptr, const String & expect_name)
    {
        return checkFunctionName(function_ptr->as<ASTFunction &>(), expect_name);
    }

    bool astTreeEquals(const IAST & left, const IAST & right);
    inline bool astTreeEquals(const ASTPtr & left, const ASTPtr & right)
    {
        return (left == right) || (left && right && astTreeEquals(*left, *right));
    }
    inline bool astTreeEquals(const ConstASTPtr & left, const ConstASTPtr & right)
    {
        return (left == right) || (left && right && astTreeEquals(*left, *right));
    }

    bool astNodeEquals(const ASTFunction & left, const ASTFunction & right);
    bool astNodeEquals(const ASTLiteral & left, const ASTLiteral & right);
    bool astNodeEquals(const ASTIdentifier & left, const ASTIdentifier & right);
    bool astNodeEquals(const ASTExpressionList & left, const ASTExpressionList & right);
    bool astNodeEquals(const ASTWindowDefinition & left, const ASTWindowDefinition & right);
    bool astNodeEquals(const ASTSubquery & left, const ASTSubquery & right);
    bool astNodeEquals(const ASTOrderByElement & left, const ASTOrderByElement & right);

    struct ASTHash
    {
        size_t operator()(const ASTPtr & ast) const
        {
            auto hash = ast->getTreeHash();
            return static_cast<size_t>(hash.first ^ hash.second);
        }
    };

    struct ASTEquals
    {
        bool operator()(const ASTPtr & left, const ASTPtr & right) const { return astTreeEquals(left, right); }
    };

    /**
     * Ordering used to determine ASTPtr preference when determining canonicals
     *
     * Current cost heuristic:
     * 1) Prefer fewer input symbols
     * 2) Prefer smaller expression trees
     * 3) Sort the expressions alphabetically - creates a stable consistent ordering (extremely useful for unit testing)
     */
    struct ConstASTPtrOrdering
    {
        bool operator()(const ConstASTPtr & predicate_1, const ConstASTPtr & predicate_2) const;
    };

    struct ConstASTHash
    {
        size_t operator()(const ASTPtr & ast) const
        {
            auto hash = ast->getTreeHash();
            return static_cast<size_t>(hash.first ^ hash.second);
        }

        size_t operator()(const ConstASTPtr & ast) const
        {
            auto hash = ast->getTreeHash();
            return static_cast<size_t>(hash.first ^ hash.second);
        }
    };

    struct ConstASTEquals
    {
        bool operator()(const ConstASTPtr & left, const ConstASTPtr & right) const { return astTreeEquals(left, right); }
    };

}

}
