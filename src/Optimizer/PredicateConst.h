#pragma once

#include <Parsers/ASTVisitor.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
namespace PredicateConst
{
    static String AND = "and";
    static String OR = "or";
    static ASTPtr TRUE_VALUE = std::make_shared<ASTLiteral>(true);
    static ASTPtr FALSE_VALUE = std::make_shared<ASTLiteral>(false);

};
}
