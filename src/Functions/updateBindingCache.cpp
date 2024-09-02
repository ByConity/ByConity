#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Common/FieldVisitors.h>
#include <Interpreters/SQLBinding/SQLBindingCache.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_SLOW;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

class FunctionUpdateBindingCache : public IFunction
{
public:
    static constexpr auto name = "updateBindingCache";
    explicit FunctionUpdateBindingCache(ContextPtr context_) : context(context_) { }
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionUpdateBindingCache>(context_); }

    /// Get the name of the function.
    String getName() const override { return name; }

    /// Do not execute during query analysis.
    bool isSuitableForConstantFolding() const override { return false; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & /*arguments*/, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        try 
        {
            BindingCacheManager::updateGlobalBindingsFromCatalog(context);
        }
        catch (...)
        {
            return result_type->createColumnConst(input_rows_count, 0);
        }
        return result_type->createColumnConst(input_rows_count, 1);
    }

private:
    ContextPtr context;
};

REGISTER_FUNCTION(UpdateBindingCache)
{
    factory.registerFunction<FunctionUpdateBindingCache>();
}

}
