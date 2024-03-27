#include <DataTypes/DataTypesNumber.h>

#include <time.h>
#include <Core/DecimalFunctions.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int SUBQUERY_MULTIPLE_ROWS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionCheckSubQueryReturnSingleRow : public IFunction
{
public:
    static constexpr auto name = "check_subquery_return_single_row";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionCheckSubQueryReturnSingleRow>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override { return std::make_shared<DataTypeUInt8>(); }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForConstantFolding() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        return executeType(arguments);
    }

private:
    ColumnPtr executeType(const ColumnsWithTypeAndName & arguments) const
    {
        const ColumnVector<UInt8> * col_from = checkAndGetColumn<ColumnVector<UInt8>>(arguments[0].column.get());
        if (col_from == nullptr) 
            throw Exception("Illegal argument [NULL] for this function", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT); 
        const typename ColumnVector<UInt8>::Container & vec_from = col_from->getData();
        size_t size = vec_from.size();

        bool distinct = true;
        for (size_t i = 0; i < size; ++i)
        {
            auto value = vec_from[i];
            if (value == 0)
            {
                distinct = false;
                break;
            }
        }

        if (distinct)
        {
            return arguments[0].column;
        }
        else
        {
            throw Exception("failed:Scalar sub-query has returned multiple rows", ErrorCodes::SUBQUERY_MULTIPLE_ROWS);
        }
    }
};

REGISTER_FUNCTION(CheckSubQueryReturnSingleRow)
{
    factory.registerFunction<FunctionCheckSubQueryReturnSingleRow>(FunctionFactory::CaseInsensitive);
}


}
