#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB {

class ExpressionEquivalence
{
public:
    static bool isEquivalent(ASTPtr & left_predicate, ASTPtr & right_predicate);

};

}

