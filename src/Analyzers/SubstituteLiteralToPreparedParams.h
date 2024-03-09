#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/prepared_statement.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

class SubstituteLiteralToPreparedParamsMatcher
{
public:
    class Data
    {
    public:
        explicit Data(ContextPtr context_) : context(std::move(context_))
        {
        }

        ContextPtr context;
        PreparedParameterBindings extracted_binding;
        int next_param_id = 0;
    };

    static bool needChildVisit(ASTPtr &, const ASTPtr &)
    {
        return true;
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * literal = ast->as<ASTLiteral>())
            visit(*literal, ast, data);
    }

    static void visit(ASTLiteral & literal, ASTPtr & ast, Data & data);
};

using SubstituteLiteralToPreparedParamsVisitor = InDepthNodeVisitor<SubstituteLiteralToPreparedParamsMatcher, true>;

}
