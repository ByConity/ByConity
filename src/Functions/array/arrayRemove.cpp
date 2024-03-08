#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnFunction.h>
#include <DataTypes/DataTypeFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TYPE_MISMATCH;
}

/// remove elements from each array.
/// array_remove([1,2,2,3], 2) => [1, 3]
/// array_remove([1,2,2,3], '2') => [1, 3]
class FunctionArrayRemove : public IFunction
{

public:
    static constexpr auto name = "arrayRemove";

    static FunctionPtr create(ContextPtr context)
    {
        if (context && context->getSettingsRef().enable_implicit_arg_type_convert)
            return std::make_shared<IFunctionMySql>(std::make_unique<FunctionArrayRemove>(context));
        return std::make_shared<FunctionArrayRemove>(context);
    }

    explicit FunctionArrayRemove(ContextPtr context_) : context(context_)
    {
    }

    ArgType getArgumentsType() const override { return ArgType::ARRAY_FIRST; }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return true; }

    bool allowArguments(const DataTypePtr & array_inner_type, const DataTypePtr & arg) const
    {
        auto inner_type_decayed = removeNullable(removeLowCardinality(array_inner_type));
        auto arg_decayed = removeNullable(removeLowCardinality(arg));

        if (inner_type_decayed->equals(*arg_decayed))
            return true;

        if (context && context->getSettingsRef().enable_implicit_arg_type_convert)
            return bool(getLeastSupertype(DataTypes{inner_type_decayed, arg_decayed}));
        else
            return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        const auto * nullable_type = checkAndGetDataType<DataTypeNullable>(arguments[0].get());
        if (nullable_type)
            array_type = checkAndGetDataType<DataTypeArray>(nullable_type->getNestedType().get());

        if (!array_type)
            throw Exception("First argument for function " + getName() + " must be an array.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->onlyNull() && !allowArguments(array_type->getNestedType(), arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Types of array and 2nd argument of function `{}` must be identical up to nullability, cardinality, "
                "numeric types, or Enum and numeric type. Passed: {} and {}.",
                getName(), arguments[0]->getName(), arguments[1]->getName());

        return (nullable_type || arguments[1]->isNullable()) ? makeNullable(arguments[0]) : arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (arguments[1].column->onlyNull())
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
        const auto * nullable_type = checkAndGetDataType<DataTypeNullable>(arguments[0].type.get());
        if (nullable_type)
            array_type = checkAndGetDataType<DataTypeArray>(nullable_type->getNestedType().get());

        /// extract the nested type of array for creating notequal func
        /// e.g., convert ColumnConst(ColumnNullable(ColumnArray)) to ColumnConst(ColumnArray),
        /// convert ColumnNullable(ColumnArray) to ColumnArray
        const auto * arr_col = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        const auto * const_col = checkAndGetColumn<ColumnConst>(arguments[0].column.get());
        const ColumnNullable * null_col = nullptr;
        if (const_col)
        {
            null_col = checkAndGetColumn<ColumnNullable>(const_col->getDataColumnPtr().get());
            if (null_col)
                arr_col = assert_cast<const ColumnArray *>(null_col->getNestedColumnPtr().get());
            else
                arr_col = assert_cast<const ColumnArray *>(const_col->getDataColumnPtr().get());
        }
        else if (!arr_col && arguments[0].column)
        {
            null_col = assert_cast<const ColumnNullable *>(arguments[0].column.get());
            arr_col = assert_cast<const ColumnArray *>(null_col->getNestedColumnPtr().get());
        }

        auto array_nested_type_no_lc = removeLowCardinality(array_type->getNestedType());
        auto not_equal_func = FunctionFactory::instance().get(NameBitNotEquals::name, context);
        auto not_equal_func_args = ColumnsWithTypeAndName{arguments[1],
                ColumnWithTypeAndName{arr_col ? arr_col->getDataPtr() : nullptr, array_nested_type_no_lc, ""}};
        auto not_equal_func_ret_type = not_equal_func->getReturnType(not_equal_func_args);
        auto not_equal_func_col = ColumnFunction::create(input_rows_count, not_equal_func->build(not_equal_func_args), ColumnsWithTypeAndName{});
        not_equal_func_col->appendArguments({arguments[1]});

        ColumnsWithTypeAndName args{ColumnWithTypeAndName{std::move(not_equal_func_col),
            std::make_shared<const DataTypeFunction>(DataTypes{arguments[1].type, array_nested_type_no_lc}, not_equal_func_ret_type),
            "notequal_func_col"}};
        if (null_col)
            args.emplace_back(null_col->getNestedColumnPtr(), nullable_type->getNestedType(), arguments[0].name);
        else
            args.emplace_back(arguments[0]);

        auto filter_func = FunctionFactory::instance().get("arrayFilter", context);
        auto filter_ret_type = filter_func->getReturnType(args);
        if (!filter_ret_type->equals(*removeNullable(result_type)))
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "The return type of array_remove should be the same as filter; however they are {} vs {}",
                    filter_ret_type->getName(), result_type->getName());
        auto ret = filter_func->build(args)->execute(args, result_type, input_rows_count);

        if (null_col)
        {
            auto null_ret = ColumnNullable::create(ret->assumeMutable(), null_col->getNullMapColumnPtr()->assumeMutable());
            if (const auto * null_arg_col = checkAndGetColumn<ColumnNullable>(arguments[1].column.get()); null_arg_col)
                null_ret->applyNullMap(*null_arg_col);
            return null_ret;
        }
        return ret;
    }

private:
    ContextPtr context;
};

REGISTER_FUNCTION(ArrayRemove)
{
    factory.registerFunction<FunctionArrayRemove>();
    factory.registerAlias("array_remove", FunctionArrayRemove::name, FunctionFactory::CaseInsensitive);
}

}
