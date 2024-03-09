#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// Get the max or min element from each array, e.g., array_max([1,2,3])=>3
/// Different to arrayMax/arrayMin in handling NULL elements and and empty array:
///   * array_max/array_min returns NULL for empty array or arrays containing NULL.
///   * arrayMax/arrayMin cannot handle array(nullable), and returns a default value for empty array(T)
/// Implemented by combining array_sort and element_at func.
template<bool is_max, typename Name>
class FunctionArrayMinMax : public IFunction
{
public:
    static constexpr auto name = Name::name;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionArrayMinMax>(context);
    }

    explicit FunctionArrayMinMax(ContextPtr context_) : context(context_) { }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must provide one argument");
        if (!isArray(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "You must provide one array column as the argument");
        return makeNullable(removeLowCardinality(assert_cast<const DataTypeArray *>(arguments[0].get())->getNestedType()));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto array_sort_func = FunctionFactory::instance().getImpl("arraySort", context)->build(arguments);
        auto sort_ret_type = array_sort_func->getResultType();
        auto sort_ret = array_sort_func->execute(arguments, sort_ret_type, input_rows_count);

        ColumnPtr index_col = nullptr;
        if constexpr (is_max)
        {
            auto col = ColumnInt64::create();
            col->insertValue(-1);
            index_col = std::move(col);
        }
        else
        {
            auto col = ColumnUInt64::create();
            col->insertValue(1);
            index_col = std::move(col);
        }

        auto index_col_const = ColumnConst::create(std::move(index_col), input_rows_count);

        ColumnsWithTypeAndName args;
        args.emplace_back(sort_ret, sort_ret_type, "sort_ret");
        args.emplace_back(std::move(index_col_const), std::make_shared<DataTypeInt32>(), "index");
        auto array_get_func = FunctionFactory::instance().getImpl("element_at", context)->build(args);
        auto get_ret_type = array_get_func->getResultType();

        auto ret = array_get_func->execute(args, get_ret_type, input_rows_count);

        /// if there is a NULL value in the source array, set the result to be NULL
        /// by setting the null map value to 1
        if (const auto * array_col = checkAndGetColumn<ColumnArray>(*arguments[0].column); array_col)
        {
            if (const auto *array_null_col = checkAndGetColumn<ColumnNullable>(*array_col->getDataPtr()); array_null_col)
            {
                const auto & src_null_map = array_null_col->getNullMapData();

                auto null_map_col = ColumnUInt8::create(input_rows_count);
                auto & null_map = null_map_col->getData();

                const auto & offsets = array_col->getOffsets();
                size_t prev_offset = 0;
                for (size_t i = 0; i < input_rows_count; i++)
                {
                    size_t len = offsets[i] - prev_offset;
                    null_map[i] = 0;
                    for (size_t j = 0; j < len; j++)
                    {
                        if (src_null_map[prev_offset + j])
                        {
                            null_map[i] = 1;
                            break;
                        }
                    }
                }

                auto * ret_null = assert_cast<ColumnNullable *>(ret->assumeMutable().get());
                ret_null->applyNullMap(*null_map_col);
            }
        }

        return ret;
    }


private:
    ContextPtr context;
};

struct ArrayMaxNameMySQL { static constexpr auto name = "array_max"; };
struct ArrayMinNameMySQL { static constexpr auto name = "array_min"; };

using FunctionArrayMaxMySQL = FunctionArrayMinMax<true, ArrayMaxNameMySQL>;
using FunctionArrayMinMySQL = FunctionArrayMinMax<false, ArrayMinNameMySQL>;

REGISTER_FUNCTION(ArrayMinMax)
{
    factory.registerFunction<FunctionArrayMinMySQL>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionArrayMaxMySQL>(FunctionFactory::CaseInsensitive);
}

}
