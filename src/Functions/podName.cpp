#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Common/DNSResolver.h>
#include <cstdlib>

namespace DB
{

/// Get the pod name. Is is constant on single server, but is not constant in distributed queries.
class FunctionPodName : public IFunction
{
public:
    static constexpr auto name = "podName";
    FunctionPodName()
    {
        if (const char * env_p = std::getenv("MY_POD_NAME"))
            pod_name = env_p;
    }
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionPodName>();
    }

    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    /** convertToFullColumn needed because in distributed query processing,
      *    each server returns its own value.
      */
    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConst(input_rows_count, pod_name)->convertToFullColumnIfConst();
    }

private:
    String pod_name;
};


REGISTER_FUNCTION(PodName)
{
    factory.registerFunction<FunctionPodName>();
}

}
