#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include "Columns/IColumn.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "DataTypes/DataTypesNumber.h"
#include "Interpreters/Context_fwd.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class ExecutableFunctionConv;

class FunctionOct : public IFunction
{
public:
    static constexpr auto name = "oct";

    explicit FunctionOct(ContextPtr _context) : context(_context) {}
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionOct>(context); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isInteger(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &result_type, size_t input_rows_count) const override
    {
        // Here we are using conv(val, 10, 8) for computation of oct.
        ColumnPtr data_column = arguments[0].column;
        data_column = ConvertImplGenericToString::execute(arguments);

        ColumnsWithTypeAndName args {
        ColumnWithTypeAndName(data_column, std::make_shared<DataTypeString>(), ""),
        ColumnWithTypeAndName(ColumnConst::create(ColumnInt8::create(1, 10), input_rows_count), std::make_shared<DataTypeUInt8>(), ""),
        ColumnWithTypeAndName(ColumnConst::create(ColumnInt8::create(1, 8), input_rows_count), std::make_shared<DataTypeUInt8>(), ""),
        };

        auto func = FunctionFactory::instance().get("conv", context);
        auto impl = func->build(args);

        return impl->execute(args, result_type, input_rows_count);
    }
private:
    ContextPtr context;
};

REGISTER_FUNCTION(Oct)
{
    factory.registerFunction<FunctionOct>(FunctionFactory::CaseInsensitive);
}

}
