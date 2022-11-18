#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class CheckAliasVisitor
{
public:        
    ASTs stack;
    
    void visit(ASTPtr & ast);
    bool isSelectItem();
    bool isTableExpressionElement();
};


}
