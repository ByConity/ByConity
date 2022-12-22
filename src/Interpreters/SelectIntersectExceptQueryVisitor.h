#pragma once

#include <unordered_set>

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

#include <Parsers/ASTSelectIntersectExceptQuery.h>


namespace DB
{

class ASTFunction;
class ASTSelectWithUnionQuery;

class SelectIntersectExceptQueryMatcher
{
public:
    struct Data
    {
        UnionMode intersect_default_mode;
        UnionMode except_default_mode;

        explicit Data(const Settings & settings) :
            intersect_default_mode(settings.intersect_default_mode),
            except_default_mode(settings.except_default_mode)
        {}
    };

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & ast, Data &);
    static void visit(ASTSelectWithUnionQuery &, Data &);
};

/// Visit children first.
using SelectIntersectExceptQueryVisitor
    = InDepthNodeVisitor<SelectIntersectExceptQueryMatcher, false>;
}
