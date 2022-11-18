#pragma once

#include <Analyzers/TypeAnalyzer.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class LiteralEncoder
{
public:
    // create an ASTLiteral by a Field and its desired type
    static ASTPtr encode(Field field, const DataTypePtr & type, ContextMutablePtr context);

    // create an ASTLiteral for a comparison expression e.g. symbol_x = ASTLiteral(`field`),
    // param `type` is the type of symbol_x
    static ASTPtr encodeForComparisonExpr(Field field, const DataTypePtr & type, ContextMutablePtr context);
};
}
