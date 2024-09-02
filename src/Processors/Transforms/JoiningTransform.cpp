#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/join_common.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <thread>
#include <chrono>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_FROM_SOCKET;
}

Block JoiningTransform::transformHeader(Block header, const JoinPtr & join)
{
    ExtraBlockPtr tmp;
    join->initialize(header);
    if (auto * concurrent_hash_join = dynamic_cast<ConcurrentHashJoin*>(join.get()))
    {
        BlocksPtr tmp_blocks;
        concurrent_hash_join->joinBlock(header, tmp, nullptr, tmp_blocks);
    }
    else
        join->joinBlock(header, tmp);
    return header;
}

JoiningTransform::JoiningTransform(
    Block input_header,
    JoinPtr join_,
    size_t max_block_size_,
    bool on_totals_,
    bool default_totals_,
    bool join_parallel_left_right_,
    FinishCounterPtr finish_counter_,
    size_t total_size_,
    size_t index_,
    FinishPipePtr finish_pipe_)
    : IProcessor({input_header}, {transformHeader(input_header, join_)})
    , join(std::move(join_))
    , on_totals(on_totals_)
    , default_totals(default_totals_)
    , join_parallel_left_right(join_parallel_left_right_)
    , finish_counter(std::move(finish_counter_))
    , max_block_size(max_block_size_)
    , total_size(total_size_)
    , index(index_)
    , finish_pipe(std::move(finish_pipe_))
{
    if (!join->isFilled())
        inputs.emplace_back(Block(), this);
    if (dynamic_cast<ConcurrentHashJoin *>(join.get()))
        is_concurrent_hash = true;
}

IProcessor::Status JoiningTransform::prepare()
{
    auto & output = outputs.front();
    auto & on_finish_output = outputs.back();

    /// Check can output.
    if (output.isFinished() || stop_reading)
    {
        output.finish();
        on_finish_output.finish();
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & input : inputs)
            input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    if (has_output)
    {
        output.push(std::move(output_chunk));

        if (dispatched_blocks && !dispatched_blocks->empty())
        {
            auto & current_block = dispatched_blocks->back();
            auto rows = current_block.rows();
            output_chunk.setColumns(current_block.getColumns(), rows);

            dispatched_blocks->pop_back();
        }
        else
        {
            has_output = false;
        }
        return Status::PortFull;
    }

    if (inputs.size() > 1)
    {
        auto & last_in = inputs.back();
        auto & left_in = inputs.front();
        if (!last_in.isFinished())
        {
            last_in.setNeeded();
            if (last_in.hasData())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No data is expected from second JoiningTransform port");
            
            // parallel run for left input.
            // join.prepare -> left_in.setNeeded() -> left_in.to.prepare().
            if (join_parallel_left_right && !left_in.hasData())
            {
                left_in.setNeeded();
            }
            return Status::NeedData;
        }
    }

    if (has_input)
        return Status::Ready;

    auto & input = inputs.front();
    if (input.isFinished())
    {
        if (!input_finish && is_concurrent_hash)
        {
            input_finish = true;
            return Status::Ready;
        }

        if (process_non_joined)
        {
            if (non_joined_stream && !finish_counter_finish && join->getType() == JoinType::PARALLEL_HASH)
                return Status::Async;
            return Status::Ready;
        }

        output.finish();
        on_finish_output.finish();
        return Status::Finished;
    }

    input.setNeeded();

    if (!input.hasData())
        return Status::NeedData;

    input_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void JoiningTransform::work()
{
    if (input_finish && !dispatched_columns_cache.finished)
    {
        dispatched_columns_cache.finished = true;
        if (auto * concurrent_hash_join = reinterpret_cast<ConcurrentHashJoin*>(join.get()))
        {
            Block empty_block;
            concurrent_hash_join->joinBlock(empty_block, not_processed, &dispatched_columns_cache, dispatched_blocks);
            auto num_rows = empty_block.rows();
            output_chunk.setColumns(empty_block.getColumns(), num_rows);
            has_output = !output_chunk.empty();
        }
    }
    else if (has_input)
    {
        transform(input_chunk);
        output_chunk.swap(input_chunk);
        has_input = not_processed != nullptr;
        has_output = !output_chunk.empty();
    }
    else
    {
        if (!non_joined_stream)
        {
            if (join->getType() == JoinType::PARALLEL_HASH)
            {
                if (!finish_counter)
                {
                    process_non_joined = false;
                    return;
                }
                if (finish_counter->isLast())
                {
                    for (size_t i = 0; i < total_size; i++)
                    {
                        /// Send something to pipe to wake worker.
                        uint64_t buf = 1;
                        while (-1 == write((*finish_pipe)[i].event_fd, &buf, sizeof(buf)))
                        {
                            if (errno == EAGAIN)
                                break;

                            if (errno != EINTR)
                                throwFromErrno("Cannot write to pipe", ErrorCodes::CANNOT_READ_FROM_SOCKET);
                        }
                    }
                    finish_counter_finish = true;
                }
                non_joined_stream = join->createStreamWithNonJoinedRows(outputs.front().getHeader(), max_block_size, total_size, index);
                if (!non_joined_stream)
                {
                    process_non_joined = false;
                }
                return;
            }
            if (!finish_counter || !finish_counter->isLast())
            {
                process_non_joined = false;
                return;
            }

            non_joined_stream = join->createStreamWithNonJoinedRows(outputs.front().getHeader(), max_block_size, 1, 0);
            if (!non_joined_stream)
            {
                process_non_joined = false;
                return;
            }
        }

        if (!finish_counter_finish && !finish_counter->isFinished())
        {
            return;
        }
        else
        {
            finish_counter_finish = true;
        }

        auto block = non_joined_stream->read();
        if (!block)
        {
            process_non_joined = false;
            return;
        }

        auto rows = block.rows();
        output_chunk.setColumns(block.getColumns(), rows);
        has_output = true;
    }
}

void JoiningTransform::transform(Chunk & chunk)
{
    if (!initialized)
    {
        initialized = true;

        if (join->alwaysReturnsEmptySet() && !on_totals)
        {
            stop_reading = true;
            chunk.clear();
            return;
        }
    }

    Block block;
    if (on_totals)
    {
        const auto & left_totals = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());
        const auto & right_totals = join->getTotals();

        /// Drop totals if both out stream and joined stream doesn't have ones.
        /// See comment in ExpressionTransform.h
        if (default_totals && !right_totals)
            return;

        block = outputs.front().getHeader().cloneEmpty();
        JoinCommon::joinTotals(left_totals, right_totals, join->getTableJoin(), block);
    }
    else
        block = readExecute(chunk);

    auto num_rows = block.rows();
    Columns columns;
    if (chunk.getSideBlock())
        chunk.getSideBlock()->clear();

    /// Block may include: (1) columns and (2) bitmap index columns, we need to split them
    for (size_t i = 0; i < block.columns(); ++i)
    {
        if (outputs.front().getHeader().has(block.getByPosition(i).name))
            columns.emplace_back(std::move(block.getByPosition(i).column));
        else
            chunk.addColumnToSideBlock(std::move(block.getByPosition(i)));
    }

    chunk.setColumns(std::move(columns), num_rows);
}

Block JoiningTransform::readExecute(Chunk & chunk)
{
    Block res;

    if (!not_processed)
    {
        if (chunk.hasColumns())
            res = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());

        /// Sometimes, predicate is not pushdown to storage, so filtering can happens after
        /// JOIN. In this case, we need to bring the bitmap index columns to the next step.
        if (auto * side_block = chunk.getSideBlock())
            for (size_t i = 0; i < side_block->columns(); ++i)
                res.insert(side_block->getByPosition(i));

        if (res)
        {
            if (is_concurrent_hash)
                static_cast<ConcurrentHashJoin *>(join.get())->joinBlock(res, not_processed, &dispatched_columns_cache, dispatched_blocks);
            else
                join->joinBlock(res, not_processed);
        }
    }
    else if (not_processed->empty()) /// There's not processed data inside expression.
    {
        if (chunk.hasColumns())
            res = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());

        if (auto * side_block = chunk.getSideBlock())
            for (size_t i = 0; i < side_block->columns(); ++i)
                res.insert(side_block->getByPosition(i));

        not_processed.reset();
        if (is_concurrent_hash)
            static_cast<ConcurrentHashJoin *>(join.get())->joinBlock(res, not_processed, &dispatched_columns_cache, dispatched_blocks);
        else
            join->joinBlock(res, not_processed);
    }
    else
    {
        res = std::move(not_processed->block);
        if (is_concurrent_hash)
            static_cast<ConcurrentHashJoin *>(join.get())->joinBlock(res, not_processed, &dispatched_columns_cache, dispatched_blocks);
        else
            join->joinBlock(res, not_processed);
    }

    return res;
}

JoiningTransform::~JoiningTransform()
{
    if (finish_pipe && finish_pipe->size() == total_size && (*finish_pipe)[index].event_fd != -1)
        close((*finish_pipe)[index].event_fd);
}

FillingRightJoinSideTransform::FillingRightJoinSideTransform(
    Block input_header, JoinPtr join_, JoiningTransform::FinishCounterPtr finish_counter_)
    : IProcessor({input_header}, {Block()}), join(std::move(join_)), finish_counter(std::move(finish_counter_))
{
    if (dynamic_cast<ConcurrentHashJoin *>(join.get()))
        is_current_hash_join = true;
}

InputPort * FillingRightJoinSideTransform::addTotalsPort()
{
    if (inputs.size() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Totals port was already added to FillingRightJoinSideTransform");

    return &inputs.emplace_back(inputs.front().getHeader(), this);
}

IProcessor::Status FillingRightJoinSideTransform::prepare()
{
    auto & output = outputs.front();

    /// Check can output.
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & input : inputs)
            input.setNotNeeded();
        return Status::PortFull;
    }

    auto & input = inputs.front();

    if (stop_reading)
    {
        input.close();
    }
    else if (!input.isFinished())
    {
        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        chunk = input.pull(true);
        return Status::Ready;
    }

    if (inputs.size() > 1)
    {
        auto & totals_input = inputs.back();
        if (!totals_input.isFinished())
        {
            totals_input.setNeeded();

            if (!totals_input.hasData())
                return Status::NeedData;

            chunk = totals_input.pull(true);
            for_totals = true;
            return Status::Ready;
        }
    }
    else if (!set_totals)
    {
        chunk.setColumns(inputs.front().getHeader().cloneEmpty().getColumns(), 0);
        for_totals = true;
        return Status::Ready;
    }

    if (!input_finish && is_current_hash_join)
    {
        input_finish = true;
        return Status::Ready;
    }

    if (!build_rf && finish_counter && finish_counter->isLast())
    {
        build_rf = true;
        return Status::Ready;
    }

    output.finish();
    return Status::Finished;
}

void FillingRightJoinSideTransform::work()
{
    if (build_rf)
    {
        join->tryBuildRuntimeFilters();
        return;
    }

    if (!input_finish)
    {
        auto block = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());

        if (for_totals)
            join->setTotals(block);
        else if (is_current_hash_join)
        {
            auto * concurrent_hash_join = static_cast<ConcurrentHashJoin *>(join.get());
            stop_reading = !concurrent_hash_join->addJoinedBlock(block, &dispatched_columns_cache, true);
        }
        else
            stop_reading = !join->addJoinedBlock(block);

        set_totals = for_totals;
    }
    else if (is_current_hash_join)
    {
        auto * concurrent_hash_join = static_cast<ConcurrentHashJoin *>(join.get());
        concurrent_hash_join->addJoinedBlock({}, &dispatched_columns_cache, true);
    }
}

DelayedJoinedBlocksWorkerTransform::DelayedJoinedBlocksWorkerTransform(
    Block left_header_,
    Block output_header_,
    size_t max_block_size_,
    JoinPtr join_)
    : IProcessor(InputPorts{Block()}, OutputPorts{output_header_})
    , left_header(left_header_)
    , output_header(output_header_)
    , max_block_size(max_block_size_)
    , join(join_)
{
}

IProcessor::Status DelayedJoinedBlocksWorkerTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (inputs.size() != 1 && outputs.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DelayedJoinedBlocksWorkerTransform must have exactly one input port");

    if (output_chunk)
    {
        input.setNotNeeded();

        if (!output.canPush())
            return Status::PortFull;

        output.push(std::move(output_chunk));
        output_chunk.clear();
        return Status::PortFull;
    }

    if (!task)
    {
        if (!input.hasData())
        {
            input.setNeeded();
            return Status::NeedData;
        }

        auto data = input.pullData(true);
        if (data.exception)
        {
            //  todo aron
            // output.pushException(data.exception);
            return Status::Finished;
        }

        if (!data.chunk.getChunkInfo())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "DelayedJoinedBlocksWorkerTransform must have chunk info");
        task = std::dynamic_pointer_cast<const DelayedBlocksTask>(data.chunk.getChunkInfo());
    }
    else
    {
        input.setNotNeeded();
    }

    // When delayed_blocks is nullptr, it means that all buckets have been joined.
    if (!task->delayed_blocks)
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    return Status::Ready;
}

void DelayedJoinedBlocksWorkerTransform::work()
{
    if (!task)
        return;

    Block block;
    if (!left_delayed_stream_finished)
    {
        block = task->delayed_blocks->next();
        if (!block)
        {
            left_delayed_stream_finished = true;
            block = nextNonJoinedBlock();
        }
    }
    else
    {
        block = nextNonJoinedBlock();
    }
    if (!block)
    {
        resetTask();
        return;
    }    

    // Add block to the output
    auto rows = block.rows();
    output_chunk.setColumns(block.getColumns(), rows);
}

void DelayedJoinedBlocksWorkerTransform::resetTask()
{
    task.reset();
    left_delayed_stream_finished = false;
    setup_non_joined_stream = false;
    non_joined_delayed_stream = nullptr;
}

Block DelayedJoinedBlocksWorkerTransform::nextNonJoinedBlock()
{
    if (!setup_non_joined_stream)
    {
        setup_non_joined_stream = true;
        // Before read from non-joined stream, all blocks in left file reader must have been joined.
        // For example, in HashJoin, it may return invalid mismatch rows from non-joined stream before
        // the all blocks in left file reader have been finished, since the used flags are incomplete.
        // To make only one processor could read from non-joined stream seems be a easy way.
        if (task && task->left_delayed_stream_finish_counter->isLast())
        {
            if (!non_joined_delayed_stream)
            {
                non_joined_delayed_stream = join->createStreamWithNonJoinedRows(output_header, max_block_size, 1, 0);
            }
        }
    }
    if (non_joined_delayed_stream)
    {
        return non_joined_delayed_stream->read();
    }
    return {};
}

DelayedJoinedBlocksTransform::DelayedJoinedBlocksTransform(size_t num_streams, JoinPtr join_)
    : IProcessor(InputPorts{}, OutputPorts(num_streams, Block()))
    , join(std::move(join_))
{
}

void DelayedJoinedBlocksTransform::work()
{
    if (finished)
        return;

    delayed_blocks = join->getDelayedBlocks();
    finished = finished || delayed_blocks == nullptr;
}

IProcessor::Status DelayedJoinedBlocksTransform::prepare()
{
    for (auto & output : outputs)
    {
        if (output.isFinished())
        {
            /// If at least one output is finished, then we have read all data from buckets.
            /// Some workers can still be busy with joining the last chunk of data in memory,
            /// but after that they also will finish when they will try to get next chunk.
            finished = true;
            continue;
        }
        if (!output.canPush())
            return Status::PortFull;
    }

    if (finished)
    {
        for (auto & output : outputs)
        {
            if (output.isFinished())
                continue;
            Chunk chunk;
            chunk.setChunkInfo(std::make_shared<DelayedBlocksTask>());
            output.push(std::move(chunk));
            output.finish();
        }

        return Status::Finished;
    }

    if (delayed_blocks)
    {
        // This counter is used to ensure that only the last DelayedJoinedBlocksWorkerTransform
        // could read right non-joined blocks from the join.
        auto left_delayed_stream_finished_counter = std::make_shared<JoiningTransform::FinishCounter>(outputs.size());
        for (auto & output : outputs)
        {
            Chunk chunk;
            auto task = std::make_shared<DelayedBlocksTask>(delayed_blocks, left_delayed_stream_finished_counter);
            chunk.setChunkInfo(task);
            output.push(std::move(chunk));
        }
        delayed_blocks = nullptr;
        return Status::PortFull;
    }

    return Status::Ready;
}

}
