#pragma once
#include <Processors/IProcessor.h>


namespace DB
{

struct DispatchedColumnsCache
{
    Block block_head;
    std::vector<MutableColumns> columns_cache;
    bool finished = false;
};

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;
class IBlocksStream;
using IBlocksStreamPtr = std::shared_ptr<IBlocksStream>;

/// Join rows to chunk form left table.
/// This transform usually has two input ports and one output.
/// First input is for data from left table.
/// Second input has empty header and is connected with FillingRightJoinSide.
/// We can process left table only when Join is filled. Second input is used to signal that FillingRightJoinSide is finished.
class JoiningTransform : public IProcessor
{
public:

    /// Count streams and check which is last.
    /// The last one should process non-joined rows.
    class FinishCounter
    {
    public:
        explicit FinishCounter(size_t total_) : total(total_) {}

        bool isLast()
        {
            return finished.fetch_add(1) + 1 >= total;
        }

        bool isFinished()
        {
            return finished.load() >= total;
        }

        size_t getTotal() const
        {
            return total;
        }
    private:
        const size_t total;
        std::atomic<size_t> finished{0};
    };

    struct EventFdStruct { int event_fd;};

    using FinishCounterPtr = std::shared_ptr<FinishCounter>;
    using FinishPipePtr = std::shared_ptr<std::vector<EventFdStruct>>;

    JoiningTransform(
        Block input_header,
        JoinPtr join_,
        size_t max_block_size_,
        bool on_totals_ = false,
        bool default_totals_ = false,
        bool join_parallel_left_right_ = true,
        FinishCounterPtr finish_counter_ = nullptr,
        size_t total_size_ = 0,
        size_t index_ = 0,
        FinishPipePtr finish_pipe_ = nullptr);
    ~JoiningTransform() override;

    String getName() const override
    {
        if (is_concurrent_hash)
            return "JoiningTransformConcur";
        else
            return "JoiningTransform";
    }

    static Block transformHeader(Block header, const JoinPtr & join);

    Status prepare() override;
    void work() override;
    int schedule() override { return (*finish_pipe)[index].event_fd; }

protected:
    void transform(Chunk & chunk);

private:
    Chunk input_chunk;
    Chunk output_chunk;
    bool has_input = false;
    bool has_output = false;
    bool stop_reading = false;
    bool process_non_joined = true;

    JoinPtr join;
    bool on_totals;
    /// This flag means that we have manually added totals to our pipeline.
    /// It may happen in case if joined subquery has totals, but out string doesn't.
    /// We need to join default values with subquery totals if we have them, or return empty chunk is haven't.
    bool default_totals;

    // should we parallel execute left input and right input
    bool join_parallel_left_right;

    bool initialized = false;

    ExtraBlockPtr not_processed;

    FinishCounterPtr finish_counter;
    bool finish_counter_finish = false;
    BlockInputStreamPtr non_joined_stream;
    size_t max_block_size;
    size_t total_size = 0;
    size_t index = 0;
    FinishPipePtr finish_pipe;

    bool input_finish = false;
    DispatchedColumnsCache dispatched_columns_cache;
    BlocksPtr dispatched_blocks;
    bool is_concurrent_hash = false;


    Block readExecute(Chunk & chunk);
};

/// Fills Join with block from right table.
/// Has single input and single output port.
/// Output port has empty header. It is closed when al data is inserted in join.
class FillingRightJoinSideTransform : public IProcessor
{
public:
    FillingRightJoinSideTransform(Block input_header, JoinPtr join_, JoiningTransform::FinishCounterPtr finish_counter_ = nullptr);
    String getName() const override
    {
        if (is_current_hash_join)
            return "FillingRightJoinSideConcur";
        else
            return "FillingRightJoinSide";
    }

    InputPort * addTotalsPort();

    Status prepare() override;
    void work() override;

private:
    JoinPtr join;
    Chunk chunk;
    JoiningTransform::FinishCounterPtr finish_counter = nullptr;
    bool stop_reading = false;
    bool for_totals = false;
    bool set_totals = false;
    bool build_rf = false; // after output finish, let's start build rf

    bool input_finish = false;
    DispatchedColumnsCache dispatched_columns_cache;
    bool is_current_hash_join = false;
};

class DelayedBlocksTask : public ChunkInfo
{
public:

    explicit DelayedBlocksTask() : finished(true) {}
    explicit DelayedBlocksTask(IBlocksStreamPtr delayed_blocks_, JoiningTransform::FinishCounterPtr left_delayed_stream_finish_counter_)
        : delayed_blocks(std::move(delayed_blocks_))
        , left_delayed_stream_finish_counter(left_delayed_stream_finish_counter_)
    {
    }

    IBlocksStreamPtr delayed_blocks = nullptr;
    JoiningTransform::FinishCounterPtr left_delayed_stream_finish_counter = nullptr;

    bool finished = false;
};

using DelayedBlocksTaskPtr = std::shared_ptr<const DelayedBlocksTask>;


/// Reads delayed joined blocks from Join
class DelayedJoinedBlocksTransform : public IProcessor
{
public:
    explicit DelayedJoinedBlocksTransform(size_t num_streams, JoinPtr join_);

    String getName() const override { return "DelayedJoinedBlocksTransform"; }

    Status prepare() override;
    void work() override;

private:
    JoinPtr join;

    IBlocksStreamPtr delayed_blocks = nullptr;
    bool finished = false;
};

class DelayedJoinedBlocksWorkerTransform : public IProcessor
{
public:
    explicit DelayedJoinedBlocksWorkerTransform(
        Block left_header_,
        Block output_header_,
        size_t max_block_size_,
        JoinPtr join_);

    String getName() const override { return "DelayedJoinedBlocksWorkerTransform"; }

    Status prepare() override;
    void work() override;

private:
    Block left_header;
    Block output_header;
    size_t max_block_size;
    JoinPtr join;
    DelayedBlocksTaskPtr task;
    Chunk output_chunk;

    /// All joined and non-joined rows from left stream are emitted, only right non-joined rows are left
    bool left_delayed_stream_finished = false;
    bool setup_non_joined_stream = false;
    BlockInputStreamPtr non_joined_delayed_stream = nullptr;

    void resetTask();
    Block nextNonJoinedBlock();
};

}
