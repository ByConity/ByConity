#pragma once

#include <Parsers/IAST.h>

namespace DB
{

struct IdentifierNameNormalizer
{
    template<bool use_lower>
    static void visit(IAST *ast)
    {
        if (!ast)
            return;

        if constexpr(use_lower)
            ast->toLowerCase();
        else
            ast->toUpperCase();

        for (auto & child : ast->children)
            visit<use_lower>(child.get());
    }
};

}
