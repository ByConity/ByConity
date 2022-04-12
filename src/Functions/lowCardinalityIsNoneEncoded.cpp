#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnLowCardinality.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


class FunctionLowCardinalityIsNoneEncoded: public IFunction
{
public:
    static constexpr auto name = "lowCardinalityIsNoneEncoded";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionLowCardinalityIsNoneEncoded>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & argument, const DataTypePtr &,  size_t) const override
    {
        const ColumnWithTypeAndName & elem = argument[0];
        if (auto const * low_lc = checkAndGetColumn<ColumnLowCardinality>(*elem.column))
        {
            if (low_lc->isFullState())
            {
                return  DataTypeUInt8().createColumnConst(elem.column->size(), 1u);
            }
        }

        return DataTypeUInt8().createColumnConst(elem.column->size(), 0u);
    }
};


void registerFunctionLowCardinalityIsNoneEncoded(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLowCardinalityIsNoneEncoded>();
}

}
