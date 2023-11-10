#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRuntimeFilter.h>

namespace DB
{

REGISTER_FUNCTION(RuntimeFilter)
{
    factory.registerFunction<RuntimeFilterBloomFilterExists>();
}

template <typename T>
static bool procNumericBlock(const BloomFilterWithRange & bf_with_range, const IColumn * column, ColumnUInt8::Container & res,
                             size_t input_rows_count)
{
    const auto * nullable = checkAndGetColumn<ColumnNullable>(column);

    if (nullable)
    {
        const auto col = checkAndGetColumn<ColumnVector<T>>(nullable->getNestedColumn());
        if (!col)
            return false;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (nullable->isNullAt(i))
                res[i] = bf_with_range.has_null ? 1 : 0;
            else
                res[i] = bf_with_range.probeKey(col->getData()[i]);
        }
    }
    else
    {
        const auto col = checkAndGetColumn<ColumnVector<T>>(column);
        if (!col)
            return false;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            res[i] = bf_with_range.probeKey(col->getData()[i]);
        }
    }

    return true;
}


static void procOneBlock(const BloomFilterWithRange & bf_with_range, WhichDataType which, const IColumn * column, ColumnUInt8::Container & res,
                          size_t input_rows_count)
{
    bool ret = false;
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) ret = procNumericBlock<TYPE>(bf_with_range, column, res, input_rows_count);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        ret = procNumericBlock<UInt16>(bf_with_range, column, res, input_rows_count);
    else if (which.idx == TypeIndex::Date32)
        ret = procNumericBlock<Int32>(bf_with_range, column, res, input_rows_count);
    else if (which.idx == TypeIndex::DateTime)
        ret = procNumericBlock<UInt32>(bf_with_range, column, res, input_rows_count);

    if (!ret)
        throw Exception("buildOneBlock unexpected type of column: " + column->getName(), ErrorCodes::LOGICAL_ERROR);
}


ColumnPtr RuntimeFilterBloomFilterExists::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    if (arguments[0].column->empty())
    {
        // dry run
        Field field = UInt8{1};
        return DataTypeUInt8().createColumnConst(input_rows_count, field);
    }

    if (!ready)
    {
        bool is_ready = prepare(arguments);
        if (!is_ready)
        {
            Field field = UInt8{1};
            return DataTypeUInt8().createColumnConst(input_rows_count, field);
        }
    }

    if (bypass == BypassType::BYPASS_EMPTY_HT)
    {
        Field field = UInt8{0};
        return DataTypeUInt8().createColumnConst(input_rows_count, field);
    }

    if (bypass == BypassType::BYPASS_LARGE_HT)
    {
        Field field = UInt8{1};
        return DataTypeUInt8().createColumnConst(input_rows_count, field);
    }

    const ColumnWithTypeAndName & target = arguments[1];

    auto col_to = ColumnVector<UInt8>::create();
    auto & vec_to = col_to->getData();
    vec_to.resize(input_rows_count);

    const auto * nullable = checkAndGetColumn<ColumnNullable>(target.column.get());
    WhichDataType which(target.type);
    if (nullable)
    {
        const auto * nest_col = nullable->getNestedColumnPtr().get();
        if (nest_col->isNumeric())
        {
            procOneBlock(*bloom_filter, which, nullable, vec_to, input_rows_count);
        }
        else
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if (nullable->isNullAt(i))
                {
                    vec_to[i] = bloom_filter->has_null ? 1 : 0;
                }
                else
                {
                    vec_to[i] = bloom_filter->probeKey(nullable->getNestedColumn().getDataAt(i));
                }
            }
        }
    }
    else
    {
        if (target.column->isNumeric())
        {
            procOneBlock(*bloom_filter, which, target.column.get(), vec_to, input_rows_count);
        }
        else
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                vec_to[i] = bloom_filter->probeKey(target.column->getDataAt(i));
            }
        }
    }

    // only check 10 time
    if (total_count.load(std::memory_order_relaxed) < 10 && (eval_count.fetch_add(1, std::memory_order_relaxed) & 3) == 0)
    {
        total_count.fetch_add(1, std::memory_order_relaxed);
        double num_filtered_rows = countBytesInFilter(vec_to);
        double ratio =  num_filtered_rows / input_rows_count;
        if (ratio > 0.5)
            bypass = BypassType::BYPASS_LARGE_HT;
    }



    return col_to;
}

}
