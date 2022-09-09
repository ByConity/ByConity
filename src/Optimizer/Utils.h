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
struct AggregateDescription;

namespace Utils
{
    void assertIff(bool expression1, bool expression2);
    void checkState(bool expression);
    void checkState(bool expression, const String & msg);
    void checkArgument(bool expression);
    void checkArgument(bool expression, const String & msg);
    bool isIdentity(const String & symbol, const ConstASTPtr & expression);
    bool isIdentity(const Assignment & assignment);
    bool isIdentity(const Assignments & assignments);
    bool isIdentity(const ProjectionStep & project);
    std::unordered_map<String, String> computeIdentityTranslations(Assignments & assignments);
    ASTPtr extractAggregateToFunction(const AggregateDescription & agg_descr);

    // this method is used to deal with function names which are case-insensitive or have an alias to.
    // should be called after `registerFunctions`
    bool checkFunctionName(const ASTFunction & function, const String & expect_name);
    inline bool checkFunctionName(const ASTPtr & function_ptr, const String & expect_name)
    {
        return checkFunctionName(function_ptr->as<ASTFunction &>(), expect_name);
    }

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

    //Determine whether it is NAN
    bool isFloatingPointNaN(const DataTypePtr & type, const Field & value);

    String flipOperator(const String & name);
}

}
