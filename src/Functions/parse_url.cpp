#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

static const std::unordered_map<String, String> VALID_FUNC{
    {"PATH", "path"}, {"HOST", "domain"}, {"QUERY", "queryString"}, {"REF", "fragment"}, {"PROTOCOL", "protocol"}, {"FILE", "pathFull"}};

class FunctionParseUrl : public IFunction
{
public:
    static constexpr auto name = "parse_url";

    explicit FunctionParseUrl(ContextPtr context_) : context(context_) { }

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionParseUrl>(context_); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + std::to_string(arguments.size())
                    + ", should be 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isString(arguments[0]))
            throw Exception("First argument for function " + getName() + " must be String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isString(arguments[1]))
            throw Exception("Second argument for function " + getName() + " must be String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * param_col = arguments[1].column.get();
        if (!isColumnConst(*param_col))
            throw Exception("Second argument of function " + getName() + " must be constant.", ErrorCodes::BAD_ARGUMENTS);

        StringRef param_str = param_col->getDataAt(0);
        ColumnsWithTypeAndName new_arg{arguments[0]};
        auto function_ptr = makeFunction(param_str, new_arg);
        return function_ptr->execute(new_arg, std::make_shared<DataTypeString>(), input_rows_count, false);
    }

private:
    /// parse_url only second params only support 'HOST','PATH','QUERY','REF','PROTOCOL','FILE'

    ExecutableFunctionPtr makeFunction(StringRef param, const ColumnsWithTypeAndName & args) const
    {
        auto it = VALID_FUNC.find(param.toString());
        if (it == VALID_FUNC.end())
            throw Exception("Unknown url type, should be 'HOST', 'PATH', 'QUERY', 'REF', 'PROTOCOL', 'FILE'", ErrorCodes::ILLEGAL_COLUMN);

        auto func = FunctionFactory::instance().get(it->second, context);
        auto builder = func->build(args);
        return builder->prepare(args);
    }

    ContextPtr context;
};

void registerFunctionParseUrl(FunctionFactory & factory)
{
    factory.registerFunction<FunctionParseUrl>();
}

}
