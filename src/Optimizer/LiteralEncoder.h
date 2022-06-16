#pragma once

#include <Analyzers/TypeAnalyzer.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/**
 * LiteralEncoder encode a Field to an AST without losing its type, which means it will add type cast if necessary.
 */
class LiteralEncoder
{
public:
    static ASTPtr encode(const Field & field, const DataTypePtr & type, ContextMutablePtr context);
};
}
