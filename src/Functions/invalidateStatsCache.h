#include <unistd.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Statistics/CacheManager.h>
#include <Common/FieldVisitors.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_SLOW;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

class FunctionInvalidateStatsCache : public IFunction
{
public:
    static constexpr auto name = "invalidateStatsCache";
    explicit FunctionInvalidateStatsCache(ContextPtr context_) : context(context_) { }
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionInvalidateStatsCache>(context_); }

    /// Get the name of the function.
    String getName() const override { return name; }

    /// Do not execute during query analysis.
    bool isSuitableForConstantFolding() const override { return false; }

    // TODO support variadic, i.e. invalidate full database/cluster
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (auto & arg : arguments)
        {
            if (!WhichDataType(arg).isString())
            {
                throw Exception("Function " + getName() + " requires string arguments", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

private:
    ContextPtr context;
};

}
