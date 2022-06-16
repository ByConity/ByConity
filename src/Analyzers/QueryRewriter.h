#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPlan/GraphvizPrinter.h>

namespace DB
{

class QueryRewriter
{
public:
    static ASTPtr rewrite(ASTPtr query, ContextMutablePtr context);
};

}
