#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/IFunctionMySql.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int NOT_INITIALIZED;
}


/// A wrapper for the real agg functions.
/// Used to do implicit argument type conversion under mysql dialect for batch methods.
/// It calls the internal agg funcs after type conversion.
/// Currently, only support unary agg functions. mysql agg funcs are all unary
/// Note if the agg func's add() method is called directly instead of via batch methods,
/// e.g., addBatch(), type conversion is skipped. Such calls only used by AggregatingSortedAlgorithm
/// LLVM based function compilation is disabled if type conversion is necessary
class IAggregateFunctionMySql : public IAggregateFunction
{
public:
    AggregateFunctionPtr function;

private:
    ColumnPtr checkAndConvert(ColumnPtr col, size_t input_rows_count) const
    {
        ColumnPtr null_map{nullptr};
        auto type = argument_types[0];
        /// for a nullable column, check and convert its nested column type
        if (const auto * column_nullable = checkAndGetColumn<const ColumnNullable>(*col))
        {
            col = column_nullable->getNestedColumnPtr();
            null_map = column_nullable->getNullMapColumnPtr();

            if (auto const * null_type = dynamic_cast<const DataTypeNullable*>(type.get()))
                type = null_type->getNestedType();
            else
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Column type and argument type mismatch for aggregate function {}, {} vs {}", getName(), getTypeName(col->getDataType()), type->getName());
        }

        ColumnWithTypeAndName input{col, type, col->getName()};
        ColumnPtr new_col;
        if (function->getMySqlArgumentTypes() == ArgType::NUMBERS)
        {
            /// for sum, avg, std, var functions
            auto res_type = std::make_shared<DataTypeFloat64>();
            new_col = IFunctionMySql::convertToTypeStatic<DataTypeFloat64>(input, res_type, input_rows_count);
        }
        else if (function->getMySqlArgumentTypes() == ArgType::UINTS)
        {
            /// for bitwise functions
            auto res_type = std::make_shared<DataTypeUInt64>();
            new_col = IFunctionMySql::convertToTypeStatic<DataTypeUInt64>(input, res_type, input_rows_count);
        }

        if (null_map)
            return ColumnNullable::create(new_col, null_map);
        else
            return new_col;
    }

    ColumnPtr checkAndConvert(const IColumn **columns, size_t input_rows_count) const
    {
        auto col = columns[0]->getPtr();
        return checkAndConvert(col, input_rows_count);
    }

    ColumnPtr checkAndConvert(const IColumn **columns, size_t , size_t ) const
    {
        auto col = columns[0]->getPtr();
        return checkAndConvert(col, col->size());
    }

    ColumnRawPtrs getColumnPtrs(ColumnPtr column, const IColumn **columns, ssize_t if_argument_pos) const
    {
        MutableColumnPtr mutable_col = column->assumeMutable();

        ColumnRawPtrs cols;
        cols.push_back(mutable_col.get());
        if (if_argument_pos >= 0 )
        {
            chassert(if_argument_pos == 1);
            cols.push_back(columns[1]);
        }
        return cols;
    }

public:
    explicit IAggregateFunctionMySql(std::unique_ptr<IAggregateFunction> function_)
        : IAggregateFunction(function_->getArgumentTypes(), function_->getParameters())
    {
        function = std::move(function_);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        function->add(place, columns, row_num, arena);
    }

    void addBatch(
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        ColumnPtr col = checkAndConvert(columns, batch_size);
        auto new_cols = getColumnPtrs(col, columns, if_argument_pos);
        function->addBatch(batch_size, places, place_offset, &new_cols[0], arena, if_argument_pos);
    }

    void addBatchSinglePlace(
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos = -1) const override
    {
        ColumnPtr col = checkAndConvert(columns, batch_size);
        auto new_cols = getColumnPtrs(col, columns, if_argument_pos);
        function->addBatchSinglePlace(batch_size, place, &new_cols[0], arena, if_argument_pos);
    }

    void addBatchSinglePlaceNotNull(
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        ColumnPtr col = checkAndConvert(columns, batch_size);
        auto new_cols = getColumnPtrs(col, columns, if_argument_pos);
        function->addBatchSinglePlaceNotNull(batch_size, place, &new_cols[0], null_map, arena, if_argument_pos);
    }

    void addBatchSinglePlaceFromInterval(
        size_t batch_begin, size_t batch_end, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos = -1)
        const override
    {
        ColumnPtr col = checkAndConvert(columns, batch_begin, batch_end);
        auto new_cols = getColumnPtrs(col, columns, if_argument_pos);
        function->addBatchSinglePlaceFromInterval(batch_begin, batch_end, place, &new_cols[0], arena, if_argument_pos);
    }

    void addBatchArray(
        size_t batch_size, AggregateDataPtr * places, size_t place_offset, const IColumn ** columns, const UInt64 * offsets, Arena * arena)
        const override
    {
        ColumnPtr col = checkAndConvert(columns, batch_size);
        auto new_cols = getColumnPtrs(col, columns, -1);
        function->addBatchArray(batch_size, places, place_offset, &new_cols[0], offsets, arena);
    }

    void addBatchLookupTable8(
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        std::function<void(AggregateDataPtr &)> init,
        const UInt8 * key,
        const IColumn ** columns,
        Arena * arena) const override
    {
        ColumnPtr col = checkAndConvert(columns, batch_size);
        auto new_cols = getColumnPtrs(col, columns, -1);

        function->addBatchLookupTable8(batch_size, places, place_offset, init, key, &new_cols[0], arena);
    }

    /// for the rest functions, call the actual agg func directly without any type conversion

    ArgType getMySqlArgumentTypes() const override { return function->getMySqlArgumentTypes(); }
    DataTypePtr getReturnType() const override { return function->getReturnType(); }
    String getName() const override { return function->getName(); }
    bool handleNullItSelf() const override { return function->handleNullItSelf(); }
    DataTypePtr getStateType() const override { return function->getStateType(); }
    DataTypePtr getReturnTypeToPredict() const override { return function->getReturnTypeToPredict(); }
    bool isVersioned() const override { return function->isVersioned(); }
    size_t getVersionFromRevision(size_t revision) const override { return function->getVersionFromRevision(revision); }
    size_t getDefaultVersion() const override { return function->getDefaultVersion(); }
    void create(AggregateDataPtr __restrict place) const override { function->create(place); }
    void destroy(AggregateDataPtr __restrict place) const noexcept override { function->destroy(place); }
    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override { function->destroyUpToState(place); }
    bool hasTrivialDestructor() const override { return function->hasTrivialDestructor(); }
    size_t sizeOfData() const override { return function->sizeOfData(); }
    size_t alignOfData() const override { return function->alignOfData(); }
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        function->merge(place, rhs, arena);
    }
    bool isAbleToParallelizeMerge() const override { return function->isAbleToParallelizeMerge(); }
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const override
    {
        function->merge(place, rhs, thread_pool, is_cancelled, arena);
    }
    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override { function->serialize(place, buf); }
    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        function->deserialize(place, buf, arena);
    }
    bool allocatesMemoryInArena() const override { return function->allocatesMemoryInArena(); }
    inline bool needCalculateStep(AggregateDataPtr place) const override { return function->needCalculateStep(place); }
    void calculateStepResult(AggregateDataPtr place, size_t a, size_t b, bool c, Arena * arena) const override
    {
        function->calculateStepResult(place, a, b, c, arena);
    }
    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        function->insertResultInto(place, to, arena);
    }
    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        function->insertMergeResultInto(place, to, arena);
    }
    void predictValues(
        ConstAggregateDataPtr place,
        IColumn & to,
        const ColumnsWithTypeAndName & arguments,
        size_t offset,
        size_t limit,
        ContextPtr context_) const override
    {
        function->predictValues(place, to, arguments, offset, limit, context_);
    }
    bool isState() const override { return function->isState(); }
    using AddFunc = void (*)(const IAggregateFunction *, AggregateDataPtr, const IColumn **, size_t, Arena *);
    AddFunc getAddressOfAddFunction() const override { return function->getAddressOfAddFunction(); }

    void mergeBatch(
        size_t batch_size, AggregateDataPtr * places, size_t place_offset, const AggregateDataPtr * rhs, Arena * arena) const override
    {
        function->mergeBatch(batch_size, places, place_offset, rhs, arena);
    }
    void
    insertResultIntoBatch(size_t batch_size, AggregateDataPtr * places, size_t place_offset, IColumn & to, Arena * arena) const override
    {
        function->insertResultIntoBatch(batch_size, places, place_offset, to, arena);
    }

    void destroyBatch(size_t batch_size, AggregateDataPtr * places, size_t place_offset) const noexcept override
    {
        function->destroyBatch(batch_size, places, place_offset);
    }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function,
        const DataTypes & arguments,
        const Array & params,
        const AggregateFunctionProperties & properties) const override
    {
        return function->getOwnNullAdapter(nested_function, arguments, params, properties);
    }

    AggregateFunctionPtr getNestedFunction() const override { return function->getNestedFunction(); }

    bool isOnlyWindowFunction() const override { return function->isOnlyWindowFunction(); }


#if USE_EMBEDDED_COMPILER
    bool isCompilable() const override
    {
        return function->isCompilable();
    }
    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        function->compileCreate(builder, aggregate_data_ptr);
    }

    void compileAdd(
        llvm::IRBuilderBase & builder,
        llvm::Value * aggregate_data_ptr,
        const DataTypes & arguments_types,
        const std::vector<llvm::Value *> & arguments_values) const override
    {
        function->compileAdd(builder, aggregate_data_ptr, arguments_types, arguments_values);
    }

    void
    compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        function->compileMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        return function->compileGetResult(builder, aggregate_data_ptr);
    }

#endif
};

}
