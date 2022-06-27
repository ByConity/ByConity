#pragma once
#include <Parsers/IAST_fwd.h>
#include <DataTypes/IDataType.h>

namespace DB
{

ASTPtr makeCastFunction(const ASTPtr & expr, const DataTypePtr & type);

}
