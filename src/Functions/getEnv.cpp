#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <cstdlib>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

/// Get the Env. Is is constant on single server, but is not constant in distributed queries.
class FunctionGetEnv : public IFunction
{
public:
    static constexpr auto name = "getEnv";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionGetEnv>();
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
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isString(arguments[0]))
            throw Exception("The argument for function " + getName() + " (unit) must be String",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto *env_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!env_column)
            throw Exception("The argument for function " + getName() + " must be constant String", ErrorCodes::ILLEGAL_COLUMN);
        String env_variable = env_column->getValue<String>();
        String env;
        if(const char* env_p = std::getenv(env_variable.c_str()))
            env = env_p;
        return result_type->createColumnConst(input_rows_count, env)->convertToFullColumnIfConst();
    }

private:
    String host_ports;
};


REGISTER_FUNCTION(GetEnv)
{
    factory.registerFunction<FunctionGetEnv>();
}

}
