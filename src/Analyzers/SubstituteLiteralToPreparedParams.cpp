#include <Analyzers/SubstituteLiteralToPreparedParams.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTPreparedParameter.h>
#include <Common/FieldVisitors.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
};

namespace
{
    class FieldToDataTypeRelaxed : public StaticVisitor<DataTypePtr>
    {
    public:
        DataTypePtr operator()(const Null &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const NegativeInfinity &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const PositiveInfinity &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const UInt64 &) const
        {
            return std::make_shared<DataTypeUInt64>();
        }
        DataTypePtr operator()(const UInt128 &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const Int64 &) const
        {
            return std::make_shared<DataTypeInt64>();
        }
        DataTypePtr operator()(const Int128 &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const UUID &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const IPv4 &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const IPv6 &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const Float64 &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const String &) const
        {
            return std::make_shared<DataTypeString>();
        }
        DataTypePtr operator()(const Array &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const Tuple &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const Map &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const Object &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const DecimalField<Decimal32> &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const DecimalField<Decimal64> &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const DecimalField<Decimal128> &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const DecimalField<Decimal256> &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const AggregateFunctionStateData &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const BitMap64 &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const UInt256 &) const
        {
            return nullptr;
        }
        DataTypePtr operator()(const Int256 &) const
        {
            return nullptr;
        }
#ifdef HAVE_BOOL_TYPE
        DataTypePtr operator()(const bool &) const
        {
            return nullptr;
        }
#endif
        DataTypePtr operator()(const JsonbField &) const
        {
            return nullptr;
        }
    };
};


void SubstituteLiteralToPreparedParamsMatcher::visit(ASTLiteral & literal, ASTPtr & ast, Data & data)
{
    DataTypePtr param_type = applyVisitor(FieldToDataTypeRelaxed(), literal.value);
    if (!param_type)
        return;
    String param_name = "p" + std::to_string(data.next_param_id++);
    data.extracted_binding.emplace(PreparedParameter{param_name, param_type}, literal.value);
    ast = std::make_shared<ASTPreparedParameter>(param_name, param_type->getName());
}

}
