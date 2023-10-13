#pragma once

#include <Parsers/ASTBitEngineConstraintDeclaration.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Storages/ConstraintsDescription.h>

namespace DB
{


class BitEngineConstraintsDescription : public ConstraintsDescription
{
public:

    static BitEngineConstraintsDescription parse(const String & str);
    
};

}
