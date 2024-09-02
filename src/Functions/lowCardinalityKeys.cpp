#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnLowCardinality.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

class FunctionLowCardinalityKeys: public IFunction
{
public:
    static constexpr auto name = "lowCardinalityKeys";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionLowCardinalityKeys>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * type = typeid_cast<const DataTypeLowCardinality *>(arguments[0].get());
        if (!type)
            throw Exception("First argument of function lowCardinalityKeys must be ColumnLowCardinality, but got "
                            + arguments[0]->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return type->getDictionaryType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const auto & arg = arguments[0];
        const auto * low_cardinality_column = typeid_cast<const ColumnLowCardinality *>(arg.column.get());
        if (low_cardinality_column->isFullState())
            throw Exception("Function lowCardinalityKeys is not supported for full state", ErrorCodes::NOT_IMPLEMENTED);
        return low_cardinality_column->getDictionary().getNestedColumn()->cloneResized(arg.column->size());
    }
};

}

REGISTER_FUNCTION(LowCardinalityKeys)
{
    factory.registerFunction<FunctionLowCardinalityKeys>();
}

}
