#include <Common/Exception.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemNumbers.h>

#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Pipe.h>
#include <Processors/LimitTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
}

namespace
{

class NumbersSource : public SourceWithProgress
{
public:
    NumbersSource(UInt64 block_size_, UInt64 offset_, UInt64 step_, std::optional<int> hard_limit_)
        : SourceWithProgress(createHeader()), block_size(block_size_), next(offset_), step(step_), hard_limit(hard_limit_) {}

    String getName() const override { return "Numbers"; }

protected:
    Chunk generate() override
    {
        auto column = ColumnUInt64::create(block_size);
        ColumnUInt64::Container & vec = column->getData();

        if (hard_limit)
        {
            if (*hard_limit <= 0)
                throw Exception("LIMIT must be less than 10000000", ErrorCodes::LIMIT_EXCEEDED);

            *hard_limit -= block_size;
        }

        size_t curr = next;     /// The local variable for some reason works faster (>20%) than member of class.
        UInt64 * pos = vec.data(); /// This also accelerates the code.
        UInt64 * end = &vec[block_size];
        while (pos < end)
            *pos++ = curr++;

        next += step;

        progress({column->size(), column->byteSize()});

        return { Columns {std::move(column)}, block_size };
    }

private:
    UInt64 block_size;
    UInt64 next;
    UInt64 step;
    std::optional<int> hard_limit;

    static Block createHeader()
    {
        return { ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number") };
    }
};


struct NumbersMultiThreadedState
{
    std::atomic<UInt64> counter;
    explicit NumbersMultiThreadedState(UInt64 offset) : counter(offset) {}
};

using NumbersMultiThreadedStatePtr = std::shared_ptr<NumbersMultiThreadedState>;

class NumbersMultiThreadedSource : public SourceWithProgress
{
public:
    NumbersMultiThreadedSource(NumbersMultiThreadedStatePtr state_, UInt64 block_size_, UInt64 max_counter_)
        : SourceWithProgress(createHeader())
        , state(std::move(state_))
        , block_size(block_size_)
        , max_counter(max_counter_) {}

    String getName() const override { return "NumbersMt"; }

protected:
    Chunk generate() override
    {
        if (block_size == 0)
            return {};

        UInt64 curr = state->counter.fetch_add(block_size, std::memory_order_acquire);

        if (curr >= max_counter)
            return {};

        if (curr + block_size > max_counter)
            block_size = max_counter - curr;

        auto column = ColumnUInt64::create(block_size);
        ColumnUInt64::Container & vec = column->getData();

        UInt64 * pos = vec.data();
        UInt64 * end = &vec[block_size];
        while (pos < end)
            *pos++ = curr++;

        progress({column->size(), column->byteSize()});

        return { Columns {std::move(column)}, block_size };
    }

private:
    NumbersMultiThreadedStatePtr state;

    UInt64 block_size;
    UInt64 max_counter;

    static Block createHeader()
    {
        return { ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "number") };
    }
};

}


StorageSystemNumbers::StorageSystemNumbers(const StorageID & table_id, bool multithreaded_, std::optional<UInt64> limit_, UInt64 offset_, bool even_distribution_)
    : IStorage(table_id), multithreaded(multithreaded_), even_distribution(even_distribution_), limit(limit_), offset(offset_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({{"number", std::make_shared<DataTypeUInt64>()}}));
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemNumbers::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo &,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    storage_snapshot->check(column_names);

    if (limit && *limit < max_block_size)
    {
        max_block_size = static_cast<size_t>(*limit);
        multithreaded = false;
    }

    if (!multithreaded)
        num_streams = 1;

    Pipe pipe;

    if (num_streams > 1 && !even_distribution && limit)
    {
        if (context && context->is_tenant_user() && *limit > 10000000)
            throw Exception("LIMIT must be less than 100000", ErrorCodes::LIMIT_EXCEEDED);

        auto state = std::make_shared<NumbersMultiThreadedState>(offset);
        UInt64 max_counter = offset + *limit;

        for (size_t i = 0; i < num_streams; ++i)
            pipe.addSource(std::make_shared<NumbersMultiThreadedSource>(state, max_block_size, max_counter));

        return pipe;
    }

    for (size_t i = 0; i < num_streams; ++i)
    {
        std::optional<int> hard_limit;

        if (context && context->is_tenant_user())
            hard_limit = (10000000 + num_streams - 1) / num_streams;

        auto source = std::make_shared<NumbersSource>(max_block_size, offset + i * max_block_size, num_streams * max_block_size, hard_limit);

        if (limit && i == 0)
            source->addTotalRowsApprox(*limit);

        pipe.addSource(std::move(source));
    }

    if (limit)
    {
        size_t i = 0;
        /// This formula is how to split 'limit' elements to 'num_streams' chunks almost uniformly.
        pipe.addSimpleTransform([&](const Block & header)
        {
            ++i;
            return std::make_shared<LimitTransform>(
                header, *limit * i / num_streams - *limit * (i - 1) / num_streams, 0);
        });
    }

    return pipe;
}

}
