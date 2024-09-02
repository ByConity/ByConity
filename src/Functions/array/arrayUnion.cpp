#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionMySql.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TYPE_MISMATCH;
}


class FunctionArrayUnion : public IFunction
{
public:
    static constexpr auto name = "arrayUnion";
    static FunctionPtr create(ContextPtr context) {
        if (context && context->getSettingsRef().enable_implicit_arg_type_convert)
            return std::make_shared<IFunctionMySql>(std::make_unique<FunctionArrayUnion>(context));
        return std::make_shared<FunctionArrayUnion>(context);
    }

    explicit FunctionArrayUnion(ContextPtr context_) : context(context_) { }

    String getName() const override { return name; }

    ArgType getArgumentsType() const override { return ArgType::ARRAY_COMMON; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception{"Function " + getName() + " requires at least one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        DataTypes nested_types;
        for (auto i : collections::range(0, arguments.size()))
        {
            const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[i].get());
            if (!array_type)
                throw Exception("Argument " + std::to_string(i) + " for function " + getName() + " must be an array but it has type "
                                + arguments[i]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            nested_types.emplace_back(array_type->getNestedType());
        }

        auto common_type = getLeastSupertype(nested_types);

        if (!(context && context->getSettingsRef().enable_implicit_arg_type_convert))
            common_type = removeNullable(common_type);
        return std::make_shared<DataTypeArray>(common_type);
    }


    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const override
    {
        auto concat_func = FunctionFactory::instance().get("arrayConcat", context);
        auto concat_ret_type = concat_func->getReturnType(arguments);
        ColumnPtr concated = concat_func->build(arguments)->execute(arguments, concat_ret_type, input_rows_count);
        ColumnWithTypeAndName argument{concated, concat_ret_type, name};
        auto distinct_func = FunctionFactory::instance().get("arrayDistinct", context);
        auto distinct_ret_type = distinct_func->getReturnType({argument});
        if (!distinct_ret_type->equals(*return_type))
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "The return type of array union should be the same as distinct; however they are {} vs {}",
                    distinct_ret_type->getName(), return_type->getName());
        auto ret = distinct_func->build({argument})->execute({argument}, return_type, input_rows_count);
        return ret;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    ContextPtr context;
};


REGISTER_FUNCTION(ArrayUnion)
{
    factory.registerFunction<FunctionArrayUnion>();
    factory.registerAlias("array_union", FunctionArrayUnion::name, FunctionFactory::CaseInsensitive);
}

}
