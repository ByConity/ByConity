#pragma once

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

inline void replace_func_with_known_column(ASTPtr & definition_ast, const NameSet & columns)
{
    if (!definition_ast)
            return;

    if (ASTFunction * func = definition_ast->as<ASTFunction>())
    {
        String column_name = func->getColumnName();
        if (columns.count(column_name))
        {
            auto identifier = std::make_shared<ASTIdentifier>(column_name);
            definition_ast = identifier;
        }
        else
        {
            for (auto & child : func->arguments->children)
                replace_func_with_known_column(child, columns);
        }
    }
    else
    {
        for (auto & child : definition_ast->children)
            replace_func_with_known_column(child, columns);
    }
}

}
