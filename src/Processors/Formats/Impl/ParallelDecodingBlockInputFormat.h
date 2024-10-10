#pragma once

#include <Formats/FormatSettings.h>
#include <bthread/condition_variable.h>
#include "Formats/SharedParsingThreadPool.h"
#include "Processors/Formats/IInputFormat.h"
#include "Processors/Sources/ConstChunkGenerator.h"
#include "Storages/MergeTree/KeyCondition.h"
#include "Common/ThreadPool.h"

namespace DB
{
class TaskTracker;

class ParallelDecodingBlockInputFormat : public IInputFormat
{
public:
    ParallelDecodingBlockInputFormat(
        ReadBuffer & buf,
        const Block & header,
        const FormatSettings & format_settings,
        size_t max_download_threads,
        size_t max_parsing_threads,
        bool preserve_order,
        std::unordered_set<int> skip_row_groups,
        SharedParsingThreadPoolPtr parsing_thread_pool = nullptr);

    ~ParallelDecodingBlockInputFormat() override;

    void resetParser() override;
    const BlockMissingValues & getMissingValues() const override;
    void setQueryInfo(const SelectQueryInfo & query_info, ContextPtr context) override;

protected:
    using Mutex = std::mutex;
    using ConditionVariable = std::condition_variable;

    Chunk generate() override;

    void close();

    void onCancel() override
    {
        is_stopped = 1;
    }


    virtual size_t getNumberOfRowGroups() = 0;
    void initializeFileReaderIfNeeded();
    virtual void initializeFileReader() = 0;
    virtual void initializeRowGroupReaderIfNeeded(size_t row_group_idx) = 0;
    virtual void resetRowGroupReader(size_t row_group_idx) = 0;
    struct PendingChunk;
    virtual std::optional<PendingChunk> readBatch(size_t row_group_idx) = 0;
    virtual size_t getRowCount() = 0;

    void decodeOneChunk(size_t row_group_idx, std::unique_lock<Mutex> & lock);

    void scheduleMoreWorkIfNeeded(std::optional<size_t> row_group_touched = std::nullopt);
    void scheduleRowGroup(size_t row_group_idx);
    virtual void prefetchRowGroup(size_t /*row_group_idx*/) {}

    void threadFunction(size_t row_group_idx);

    struct RowGroupState
    {
        // Transitions:
        //
        // NotStarted -> Running -> Complete
        //                  Ʌ
        //                  V
        //               Paused
        //
        // If max_decoding_threads <= 1: NotStarted -> Complete.
        enum class Status
        {
            NotStarted,
            Running,
            // Paused decoding because too many chunks are pending.
            Paused,
            // Decoded everything.
            Done,
        };

        Status status = Status::NotStarted;

        // Window of chunks that were decoded but not returned from generate():
        //
        // (delivered)            next_chunk_idx
        //   v   v                       v
        // +---+---+---+---+---+---+---+---+---+---+
        // | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |    <-- all chunks
        // +---+---+---+---+---+---+---+---+---+---+
        //           ^   ^   ^   ^   ^
        //           num_pending_chunks
        //           (in pending_chunks)
        //  (at most max_pending_chunks_per_row_group)
        size_t next_chunk_idx = 0;
        size_t num_pending_chunks = 0;

        size_t row_group_bytes_uncompressed = 0;
        size_t row_group_rows = 0;
    };

    // Chunk ready to be delivered by generate().
    struct PendingChunk
    {
        Chunk chunk;
        BlockMissingValues block_missing_values;
        size_t chunk_idx; // within row group
        size_t row_group_idx;
        size_t approx_original_chunk_size;

        // For priority_queue.
        // In ordered mode we deliver strictly in order of increasing row group idx,
        // in unordered mode we prefer to interleave chunks from different row groups.
        struct Compare
        {
            bool row_group_first = false;

            bool operator()(const PendingChunk & a, const PendingChunk & b) const
            {
                auto tuplificate = [this](const PendingChunk & c) {
                    return row_group_first ? std::tie(c.row_group_idx, c.chunk_idx) : std::tie(c.chunk_idx, c.row_group_idx);
                };
                return tuplificate(a) > tuplificate(b);
            }
        };
    };

    FormatSettings format_settings;

    size_t max_download_threads;
    size_t max_parsing_threads;
    bool preserve_order = false;
    std::unordered_set<int> skip_row_groups;

    const size_t max_pending_chunks_per_row_group = 8;


    // Window of active row groups:
    //
    // row_groups_completed   row_groups_started
    //          v                   v
    //  +---+---+---+---+---+---+---+---+---+---+
    //  | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |   <-- all row groups
    //  +---+---+---+---+---+---+---+---+---+---+
    //    ^   ^                       ^   ^   ^
    //    Done                        NotStarted

    Mutex mutex;
    // Wakes up the generate() call, if any.
    ConditionVariable condvar;

    std::vector<RowGroupState> row_groups;
    std::priority_queue<PendingChunk, std::vector<PendingChunk>, PendingChunk::Compare> pending_chunks;
    size_t row_groups_completed = 0;

    // These are only used when max_decoding_threads > 1.
    size_t row_groups_started = 0;

    std::shared_ptr<ThreadPool> pool;
    std::unique_ptr<TaskTracker> task_tracker;
    SharedParsingThreadPoolPtr shared_pool;
    size_t additional_parsing_threads = 0;

    BlockMissingValues previous_block_missing_values;
    size_t previous_approx_bytes_read_for_chunk = 0;

    std::exception_ptr background_exception = nullptr;
    std::atomic<int> is_stopped{0};
    bool is_initialized = false;

    std::optional<KeyCondition> key_condition;
    std::optional<ConstChunkGenerator> const_chunk_source;
};

}
