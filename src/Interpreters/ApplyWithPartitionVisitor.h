#pragma once

#include <map>

#include <Core/Block.h>
#include <Parsers/IAST.h>

namespace DB
{
class ASTFunction;

class ApplyWithPartitionVisitor
{
public:
    struct Data
    {
        Block partition_sample_block;
    };

    static void visit(ASTPtr & ast, const Data & data);

private:
    static void visit(ASTFunction & func, ASTPtr & node, const Data & data);
};

}
