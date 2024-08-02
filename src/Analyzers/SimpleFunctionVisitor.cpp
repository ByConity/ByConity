#include <Analyzers/SimpleFunctionVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/convertFieldToType.h>
#include "common/types.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

void SimpleFunctionVisitor::visit(ASTPtr & ast)
{
    if (auto * node_func = ast->as<ASTFunction>())
        visit(node_func);

    for (auto & child : ast->children)
        visit(child);
}

void SimpleFunctionVisitor::visit(ASTFunction * func)
{
    if ((func->name == "like" || func->name == "notLike") &&
        func->arguments->children.size() == 2 && func->arguments->children[1]->as<ASTLiteral>())
    {
        auto & pattern = func->arguments->children[1]->as<ASTLiteral>()->value;
        if (pattern.getType() != Field::Types::String)
            return;

        Field converted = convertFieldToType(pattern, DataTypeString());
        String text = converted.safeGet<String>();

        for (auto & s : text)
        {
            if (s == '%' || s == '_' || s == '\\')
                return;
        }
        if (func->name == "like")
            func->name = "equals";
        else
            func->name = "notEquals";
    }
}

}
