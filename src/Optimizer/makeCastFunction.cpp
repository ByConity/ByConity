#include <Optimizer/makeCastFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

ASTPtr makeCastFunction(const ASTPtr & expr, const DataTypePtr & type)
{
    if (type->getTypeId() == TypeIndex::LowCardinality)
        return makeASTFunction(
            "toLowCardinality", makeCastFunction(expr, static_cast<const DataTypeLowCardinality &>(*type).getDictionaryType()));

    // special handling for Interval type, which is not support by CAST function
    if (type->getTypeId() == TypeIndex::Interval)
        return makeASTFunction("to" + type->getName(), expr);
    if (type->getTypeId() == TypeIndex::Nullable)
    {
        if (auto nested_type = static_cast<const DataTypeNullable &>(*type).getNestedType();
            nested_type->getTypeId() == TypeIndex::Interval)
            return makeASTFunction("toNullable", makeASTFunction("to" + type->getName(), expr));
    }

    auto type_ast = std::make_shared<ASTLiteral>(type->getName());
    return makeASTFunction("cast", expr, type_ast);
}

}
