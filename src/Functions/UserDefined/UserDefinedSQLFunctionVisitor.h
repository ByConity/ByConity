#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;

/** Visits ASTFunction nodes and if it is used defined function replace it with function body.
  * Example:
  *
  * CREATE FUNCTION test_function AS a -> a + 1;
  *
  * Before applying visitor:
  * SELECT test_function(number) FROM system.numbers LIMIT 10;
  *
  * After applying visitor:
  * SELECT number + 1 FROM system.numbers LIMIT 10;
  */
class UserDefinedSQLFunctionMatcher
{
public:
    using Visitor = InDepthNodeVisitor<UserDefinedSQLFunctionMatcher, true>;

    struct Data : public WithContext
    {
    };

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);

private:
    static void visit(ASTFunction & func, const Data & data);

    static ASTPtr tryToReplaceFunction(const ASTFunction & function, std::unordered_set<std::string> & udf_in_replace_process);

    static void loadUDFs(ASTPtr & ast, ContextMutablePtr context);
    static bool isSysFunc(const String & name);
    static bool resolveFuncName(ASTFunction * fn, const String & db, String & name);
    static void checkLocalVersions(
        ASTPtr & ast,
        ContextMutablePtr ctx,
        std::unordered_map<String, size_t> & mismatched_udfs);
    template <typename P, typename T>
    static void traverse(P & ast, ContextMutablePtr ctx, T& arg);
};

/// Visits AST nodes and collect their aliases in one map (with links to source nodes).
using UserDefinedSQLFunctionVisitor = UserDefinedSQLFunctionMatcher::Visitor;

}
