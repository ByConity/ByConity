#include <Optimizer/LiteralEncoder.h>

#include <Core/Types.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

static ASTPtr makeCastFunction(ASTPtr expr, const DataTypePtr & type, const ContextMutablePtr & context)
{
    if (type->getTypeId() == TypeIndex::LowCardinality)
        return makeASTFunction(
            "toLowCardinality", makeCastFunction(expr, static_cast<const DataTypeLowCardinality &>(*type).getDictionaryType(), context));

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

ASTPtr LiteralEncoder::encode(const Field & field, const DataTypePtr & type, ContextMutablePtr context)
{
    auto literal_ast = std::make_shared<ASTLiteral>(field);

    if (type->getTypeId() == TypeIndex::ByteMap)
        return literal_ast;

    if (context->getSettingsRef().legacy_column_name_of_tuple_literal)
    {
        if (literal_ast->value.getType() == Field::Types::Tuple)
        {
            literal_ast->use_legacy_column_name_of_tuple = true;
        }
    }

    DataTypePtr result_type = TypeAnalyzer::getType(literal_ast, context, {});

    // use IDataType::getName as an additional check, as IDataType::equals may return unexpected result for data types
    // with custom serialization. e.g. equals(DataTypeUInt32, DataTypeIpv4) => true
    if (type->equals(*result_type) && type->getName() == result_type->getName())
        return literal_ast;

    return makeCastFunction(literal_ast, type, context);
}

}
