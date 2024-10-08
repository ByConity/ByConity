#include <Core/Joins.h>

#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinUtils.h>

#include <Formats/NativeWriter.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/Exchange/DataTrans/BoundedDataQueue.h>
#include <QueryPlan/PlanSerDerHelper.h>
#include <Common/MemoryTracker.h>
#include <Common/thread_local_rng.h>

#include <atomic>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <fmt/format.h>
#include "common/FnTraits.h"
#include "Storages/MergeTree/MergeTreeIOSettings.h"



namespace CurrentMetrics
{
extern const Metric TemporaryFilesForJoin;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    class AccumulatedBlockReader
    {
    public:
        AccumulatedBlockReader(std::shared_ptr<TemporaryFileStreams> readers_, size_t result_block_size_, size_t read_block_bytes_)
            : readers(readers_)
            , total_reader(readers->size())
            , result_block_size(result_block_size_)
            , max_read_block_bytes(read_block_bytes_)
            , reader_queue(std::make_shared<BoundedDataQueue<size_t>>(readers->size()))
        {
            size_t i = 0;
            for (const auto & r : *readers_)
            {
                if (!r->isWriteFinished())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Reading not finished file");
                reader_queue->push(i);
                i++;
            }
        }

        Block read()
        {
            size_t index = 0;
            do
            {
                try
                {
                    if (!reader_queue->tryPop(index, UINT_MAX))
                    {
                        LOG_INFO(getLogger("GraceHashJoin"), "all file read done");
                        return {};
                    }
                    bool finished = false;
                    Block block = readThisReader(readers->at(index), finished);
                    if (finished)
                    {
                        std::unique_lock<std::mutex> lock(eof_cnt_mutex);
                        LOG_TRACE(getLogger("GraceHashJoin"), "file " + std::to_string(index) + " read done");
                        eof_cnt ++;
                        if (eof_cnt == total_reader)
                        {
                            LOG_INFO(getLogger("GraceHashJoin"), "close all readers");
                            reader_queue->close();
                        }
                    }
                    else
                        reader_queue->tryPush(index);

                    if (block)
                        return block;
                }
                catch (...)
                {
                    tryLogCurrentException(getLogger("GraceHashJoin"), "Fail to read file");
                    reader_queue->close();
                    throw;
                }

            } while (true);
        }

        Block readThisReader(std::shared_ptr<TemporaryFileStream> reader, bool &fin) const
        {
            Blocks blocks;
            size_t rows_read = 0, bytes_read = 0;
            do
            {
                Block block = reader->read();
                rows_read += block.rows();
                bytes_read += block.bytes();
                if (!block)
                {
                    fin = true;
                    break;
                }
                blocks.push_back(std::move(block));
            } while (rows_read < result_block_size && bytes_read < max_read_block_bytes);

            if (blocks.size() == 1)
                return blocks.front();
            return concatenateBlocks(blocks);
        }

    private:
        std::shared_ptr<TemporaryFileStreams> readers;
        const size_t total_reader;
        const size_t result_block_size;
        const size_t max_read_block_bytes;
        std::mutex eof_cnt_mutex;
        size_t eof_cnt = 0;
        std::shared_ptr<BoundedDataQueue<size_t>> reader_queue;
    };

    std::deque<size_t> generateRandomPermutation(size_t from, size_t to)
    {
        size_t size = to - from;
        std::deque<size_t> indices(size);
        std::iota(indices.begin(), indices.end(), from);
        std::shuffle(indices.begin(), indices.end(), thread_local_rng);
        return indices;
    }

    // Try to apply @callback in the order specified in @indices
    // Until it returns true for each index in the @indices.
    void retryForEach(std::deque<size_t> indices, Fn<bool(size_t)> auto callback)
    {
        while (!indices.empty())
        {
            size_t bucket_index = indices.front();
            indices.pop_front();

            if (!callback(bucket_index))
                indices.push_back(bucket_index);
        }
    }
}

class GraceHashJoin::FileBucket : boost::noncopyable
{
    enum class State : int
    {
        WRITING_BLOCKS,
        JOINING_BLOCKS,
        FINISHED,
    };

public:
    using BucketLock = std::unique_lock<std::mutex>;

    explicit FileBucket(size_t bucket_index_, std::shared_ptr<TemporaryFileStreams> & left_files_, TemporaryFileStreamShardPtr & right_file_, LoggerPtr log_, size_t read_result_block_size_, size_t read_block_bytes_)
        : idx{bucket_index_}, left_files{left_files_}, right_file{right_file_}, state{State::WRITING_BLOCKS}, log{log_}, read_result_block_size{read_result_block_size_}, read_block_bytes(read_block_bytes_)
    {
        left_side_parallel = left_files_->size();
        for (int i = 0; i < left_side_parallel; i++)
            left_file_mutexs.push_back(std::make_shared<std::mutex>());
    }

    void addLeftBlock(const Block & block)
    {
        int index = (left_blk_id++) % left_side_parallel;
        std::unique_lock<std::mutex> lock(*left_file_mutexs.at(index));
        addBlockImpl(block, *left_files->at(index), lock);
    }

    void addRightBlock(const Block & block)
    {
        std::unique_lock<std::mutex> lock(right_file_mutex);
        addBlockImpl(block, *right_file, lock);
    }

    bool tryAddLeftBlock(const Block & block)
    {
        int index = (left_blk_id++) % left_side_parallel;
        std::unique_lock<std::mutex> lock(*left_file_mutexs.at(index));
        return addBlockImpl(block, *left_files->at(index), lock);
    }

    bool tryAddRightBlock(const Block & block)
    {
        std::unique_lock<std::mutex> lock(right_file_mutex, std::try_to_lock);
        return addBlockImpl(block, *right_file, lock);
    }

    bool finished() const
    {
        auto is_finished = true;
        int i = 0;
        for (const auto & file : *left_files)
        {
            std::unique_lock<std::mutex> lock(*left_file_mutexs.at(i));
            if (!file->isEof())
            {
                return false;
            }
            i++;
        }
        return is_finished;
    }

    bool empty() const { return is_empty.load(); }

    AccumulatedBlockReader startJoining()
    {
        LOG_DEBUG(log, "Joining file bucket {}", idx);
        {
            std::unique_lock<std::mutex> right_lock(right_file_mutex);

            int i = 0;
            for (const auto & file : *left_files)
            {
                std::unique_lock<std::mutex> lock(*left_file_mutexs.at(i));
                file->finishWriting();
                i++;
            }
            right_file->finishWriting();

            state = State::JOINING_BLOCKS;
        }
        std::shared_ptr<TemporaryFileStreams> right_files = std::make_shared<TemporaryFileStreams>();
        right_files->push_back(right_file);
        return AccumulatedBlockReader(right_files, read_result_block_size, read_block_bytes);
    }

    AccumulatedBlockReader getLeftTableReader()
    {
        ensureState(State::JOINING_BLOCKS);
        return AccumulatedBlockReader(left_files, read_result_block_size, read_block_bytes);
    }

    const size_t idx;

private:
    bool addBlockImpl(const Block & block, TemporaryFileStream & writer, std::unique_lock<std::mutex> & lock)
    {
        ensureState(State::WRITING_BLOCKS);

        if (!lock.owns_lock())
            return false;

        if (block.rows())
            is_empty = false;

        writer.write(block);
        return true;
    }

    void transition(State expected, State desired)
    {
        State prev = state.exchange(desired);
        if (prev != expected)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid state transition from {} (got {}) to {}", expected, prev, desired);
    }

    void ensureState(State expected) const
    {
        State cur_state = state.load();
        if (cur_state != expected)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid state transition, expected {}, got {}", expected, state.load());
    }

    std::shared_ptr<TemporaryFileStreams> left_files;
    TemporaryFileStreamShardPtr right_file;
    mutable std::vector<std::shared_ptr<std::mutex>> left_file_mutexs;
    mutable std::mutex right_file_mutex;
    int left_side_parallel;
    std::atomic_bool is_empty = true;

    std::atomic<State> state;
    std::atomic<size_t> left_blk_id{0};

    LoggerPtr log;
    size_t read_result_block_size;
    size_t read_block_bytes;

};

namespace
{

template <JoinTableSide table_side>
void flushBlocksToBuckets(Blocks & blocks, const GraceHashJoin::Buckets & buckets, size_t except_index = 0)
{
    chassert(blocks.size() == buckets.size());
    retryForEach(
        generateRandomPermutation(1, buckets.size()), // skipping 0 block, since we join it in memory w/o spilling on disk
        [&](size_t i)
        {
            /// Skip empty and current bucket
            if (!blocks[i].rows() || i == except_index)
                return true;

            bool flushed = false;
            if constexpr (table_side == JoinTableSide::Left)
                flushed = buckets[i]->tryAddLeftBlock(blocks[i]);
            if constexpr (table_side == JoinTableSide::Right)
                flushed = buckets[i]->tryAddRightBlock(blocks[i]);

            if (flushed)
                blocks[i].clear();

            return flushed;
        });
}
}

GraceHashJoin::GraceHashJoin(
    ContextPtr context_, std::shared_ptr<TableJoin> table_join_,
    const Block & left_sample_block_,
    const Block & right_sample_block_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    int left_side_parallel_,
    bool enable_adaptive_spill,
    bool any_take_last_row_,
    int num_streams_)
    : log{getLogger("GraceHashJoin")}
    , context{context_}
    , adaptive_spill_mode(enable_adaptive_spill)
    , table_join{std::move(table_join_)}
    , left_sample_block{left_sample_block_}
    , right_sample_block{right_sample_block_}
    , any_take_last_row{any_take_last_row_}
    , max_num_buckets{context->getSettingsRef().grace_hash_join_max_buckets}
    , max_block_size{context->getSettingsRef().max_block_size}
    // todo aron support table_join->getOnlyClause().key_names_left
    , left_key_names(table_join->keyNamesLeft())
    , right_key_names(table_join->keyNamesRight())
    , tmp_data(std::make_unique<TemporaryDataOnDisk>(tmp_data_, CurrentMetrics::TemporaryFilesForJoin))
    , hash_join(makeInMemoryJoin())
    , hash_join_sample_block(hash_join->savedBlockSample())
    , max_allowed_mem_size_in_spill(context->getSettingsRef().max_allowed_mem_size_in_join_spill)
    , num_streams(num_streams_)
    , setting_max_joined_block_rows(context->getSettingsRef().max_joined_block_size_rows? context->getSettingsRef().max_joined_block_size_rows: kMaxAllowedJoinedBlockRows)
{
    if (left_side_parallel_ == 0)
        left_side_parallel = num_streams;
    else
        left_side_parallel = left_side_parallel_;

    initMaxJoinedBlockBytesInSpill();

    LOG_DEBUG(log, "Grace hash join left side parallel:{}, max threads:{}", left_side_parallel, num_streams);
    if (!GraceHashJoin::isSupported(table_join))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "GraceHashJoin is not supported for this join type");
}

void GraceHashJoin::initBuckets()
{
    if (!buckets.empty())
        return;

    const auto & settings = context->getSettingsRef();

    size_t initial_num_buckets = roundUpToPowerOfTwoOrZero(std::clamp<size_t>(settings.grace_hash_join_initial_buckets, 1, settings.grace_hash_join_max_buckets));

    addBuckets(initial_num_buckets);

    if (buckets.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No buckets created");

    LOG_DEBUG(log, "Initialize {} bucket{}", buckets.size(), buckets.size() > 1 ? "s" : "");

    current_bucket = buckets.front().get();
    current_bucket->startJoining();
}

bool GraceHashJoin::isSupported(const std::shared_ptr<TableJoin> & table_join)
{
    bool is_asof = (table_join->strictness() == ASTTableJoin::Strictness::Asof);
    auto kind = table_join->kind();
    return !is_asof && (isInner(kind) || isLeft(kind) || isRight(kind) || isFull(kind)) && table_join->oneDisjunct();
}

GraceHashJoin::~GraceHashJoin() = default;

bool GraceHashJoin::addJoinedBlock(const Block & block, bool /*check_limits*/)
{
    if (current_bucket == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "GraceHashJoin is not initialized");

    // todo aron sparse
    // Block materialized = materializeBlock(block);
    addJoinedBlockImpl(std::move(block));
    return true;
}

BlockInputStreamPtr GraceHashJoin::createStreamWithNonJoinedRows(const Block & result_sample_block, UInt64 max_block_size_, size_t total_size, size_t index) const {
    return hash_join->createStreamWithNonJoinedRows(result_sample_block, max_block_size_, total_size, index);
}

bool GraceHashJoin::hasMemoryOverflow(size_t total_rows, size_t total_bytes) const
{
    /// One row can't be split, avoid loop
    if (total_rows < 2)
        return false;
    if (getNumBuckets() >=1024) {
        return false;
    }

    bool has_overflow;
    if (adaptive_spill_mode) {
        if (last_mem_size_triger_spill && total_bytes >= last_mem_size_triger_spill) {
            has_overflow = false;
        } else {
            double spill_triger_threshold = context->getSettingsRef().spill_triger_threshold.value;
            bool adaptive_spill_trigered = total_memory_tracker.getHardLimit() * spill_triger_threshold < total_memory_tracker.get();
            size_t bucket_index = current_bucket->idx;
            if (bucket_index) {
                has_overflow = adaptive_spill_trigered && total_bytes > max_allowed_mem_size_in_spill*2;
            } else {
                has_overflow = adaptive_spill_trigered && total_bytes > max_allowed_mem_size_in_spill;
            }
        }
    } else {
        has_overflow = !table_join->sizeLimits().softCheck(total_rows, total_bytes);
    }


    if (has_overflow)
        LOG_DEBUG(log, "Grace hash join memory overflow, size exceeded {} / {} bytes, {} / {} rows",
            ReadableSize(total_bytes), ReadableSize(table_join->sizeLimits().max_bytes),
            total_rows, table_join->sizeLimits().max_rows);

    return has_overflow;
}

bool GraceHashJoin::hasMemoryOverflow(const BlocksList & blocks) const
{
    size_t total_rows = 0;
    size_t total_bytes = 0;
    for (const auto & block : blocks)
    {
        total_rows += block.rows();
        total_bytes += block.allocatedBytes();
    }
    return hasMemoryOverflow(total_rows, total_bytes);
}

bool GraceHashJoin::hasMemoryOverflow(const InMemoryJoinPtr & hash_join_) const
{
    size_t total_rows = hash_join_->getTotalRowCount();
    size_t total_bytes = hash_join_->getTotalByteCount();

    return hasMemoryOverflow(total_rows, total_bytes);
}

GraceHashJoin::Buckets GraceHashJoin::rehashBuckets(size_t grow_multiplier)
{
    std::unique_lock lock(rehash_mutex);

    if (!isPowerOf2(buckets.size()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of buckets should be power of 2 but it's {}", buckets.size());

    const size_t to_size = buckets.size() * grow_multiplier;
    size_t current_size = buckets.size();

    if (to_size > max_num_buckets)
    {
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Too many grace hash join buckets ({} > {}), "
            "consider increasing grace_hash_join_max_buckets or max_rows_in_join/max_bytes_in_join",
            to_size,
            max_num_buckets);
    }

    LOG_DEBUG(log, "Rehashing from {} to {}", current_size, to_size);

    addBuckets(to_size - current_size);
    rehash = true;
    return buckets;
}

void GraceHashJoin::addBuckets(const size_t bucket_count)
{
    // Exception can be thrown in number of cases:
    // - during creation of temporary files for buckets
    // - in CI tests, there is a certain probability of failure in allocating memory, see memory_tracker_fault_probability
    // Therefore, new buckets are added only after all of them created successfully,
    // otherwise we can end up having unexpected number of buckets

    const size_t current_size = buckets.size();
    Buckets tmp_buckets;
    tmp_buckets.reserve(bucket_count);
    for (size_t i = 0; i < bucket_count; ++i)
    {
        try
        {
            std::shared_ptr<TemporaryFileStreams> left_files = std::make_shared<TemporaryFileStreams>();
            for (int j = 0; j < left_side_parallel; j++)
            {
                std::shared_ptr<TemporaryFileStream> left_file = tmp_data->createStreamPtrToRegularFile(left_sample_block);
                left_files->push_back(left_file);
            }
            std::shared_ptr<TemporaryFileStream> right_file = tmp_data->createStreamPtrToRegularFile(prepareRightBlock(right_sample_block));
            auto read_result_block_size = context->getSettingsRef().grace_hash_join_read_result_block_size;
            auto read_block_bytes = context->getSettingsRef().grace_hash_join_read_result_block_bytes;
            BucketPtr new_bucket = std::make_shared<FileBucket>(current_size + i, left_files, right_file, log, read_result_block_size, read_block_bytes);
            tmp_buckets.emplace_back(std::move(new_bucket));
        }
        catch (...)
        {
            LOG_ERROR(
                getLogger("GraceHashJoin"),
                "Can't create bucket {} due to error: {}",
                current_size + i,
                getCurrentExceptionMessage(false));
            throw;
        }
    }

    buckets.reserve(buckets.size() + bucket_count);
    for (auto & bucket : tmp_buckets)
        buckets.emplace_back(std::move(bucket));
}

inline size_t calculateMaxjoinedBlockBytes(size_t mem_quota, size_t used_memory, int max_threads, int K = 8) {
    //formula: mem_quota * free_memory_ratio / max_threads / K (K=8)
    if (unlikely(mem_quota <= used_memory)) {
        return 1;
    }
    return (mem_quota-used_memory) / max_threads / K;
}

inline size_t calculateMaxJoinedBlockRowsBasedOnMaxBytesAndSampleBlock(size_t max_joined_block_bytes_in_spill, const Block &block) {
    return max_joined_block_bytes_in_spill / (block.allocatedBytes()+1) * block.rows();
}

// void GraceHashJoin::checkTypesOfKeys(const Block & block) const
// {
//     chassert(hash_join);
//     return hash_join->checkTypesOfKeys(block);
// }

void GraceHashJoin::initialize(const Block & sample_block)
{
    left_sample_block = sample_block.cloneEmpty();
    output_sample_block = left_sample_block.cloneEmpty();
    ExtraBlockPtr not_processed;
    hash_join->joinBlock(output_sample_block, not_processed);
    if (not_processed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unhandled not processed block in GraceHashJoin");
    initBuckets();
}

void GraceHashJoin::inMemoryJoinBlock(Block & block, ExtraBlockPtr & not_processed) {
    if (adaptive_spill_mode) {
        if (!join_blk_rows_inited) {
            table_join->maxJoinedBlockRows() = sample_block_rows_for_bytes_estimation;
            join_blk_rows_inited = true;
        }
    }

    hash_join->joinBlock(block, not_processed);

    if (adaptive_spill_mode && block.rows()) {

        double spill_triger_threshold = context->getSettingsRef().spill_triger_threshold.value;
        bool adaptive_spill_trigered = total_memory_tracker.getHardLimit() * spill_triger_threshold < total_memory_tracker.get();
       
        if (has_low_memory_encountered.load(std::memory_order_relaxed) || adaptive_spill_trigered) {
            size_t updated_val = 
                std::min(
                    calculateMaxJoinedBlockRowsBasedOnMaxBytesAndSampleBlock(max_joined_block_bytes_in_spill, block), 
                    setting_max_joined_block_rows);
            if (updated_val) {
                table_join->maxJoinedBlockRows() = updated_val;
                // LOG_INFO(log, "update maxJoinedBlockRows: {}", updated_val);
            }
            has_low_memory_encountered.store(true, std::memory_order_relaxed);
        } else {
            table_join->maxJoinedBlockRows() = 
                std::min(
                    std::max(
                        table_join->maxJoinedBlockRows(), 
                        calculateMaxJoinedBlockRowsBasedOnMaxBytesAndSampleBlock(
                            calculateMaxjoinedBlockBytes(total_memory_tracker.getHardLimit(),total_memory_tracker.get(), num_streams), block)), 
                    setting_max_joined_block_rows);
            // LOG_INFO(log, "scale up maxJoinedBlockRows: {} RAM:{}", table_join->maxJoinedBlockRows(),  (total_memory_tracker.get()/1024.0/1024/1024) );
        }
    }
}

void GraceHashJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed)
{
    if (block.rows() == 0)
    {
        hash_join->joinBlock(block, not_processed);
        return;
    }

    // todo aron sparse
    // materializeBlockInplace(block);

    /// number of buckets doesn't change after right table is split to buckets, i.e. read-only access to buckets
    /// so, no need to copy buckets here
    size_t num_buckets = getNumBuckets();
    if (num_buckets != 1) {
        Blocks blocks = JoinCommon::scatterBlockByHash(left_key_names, block, num_buckets);

        block = std::move(blocks[current_bucket->idx]);

        flushBlocksToBuckets<JoinTableSide::Left>(blocks, buckets);
    }


    inMemoryJoinBlock(block, not_processed);
}

void GraceHashJoin::setTotals(const Block & block)
{
    if (block.rows() > 0)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Totals are not supported for GraceHashJoin, got '{}'", block.dumpStructure());
}

size_t GraceHashJoin::getTotalRowCount() const
{
    std::lock_guard lock(hash_join_mutex);
    assert(hash_join);
    return hash_join->getTotalRowCount();
}

size_t GraceHashJoin::getTotalByteCount() const
{
    std::lock_guard lock(hash_join_mutex);
    chassert(hash_join);
    return hash_join->getTotalByteCount();
}

bool GraceHashJoin::alwaysReturnsEmptySet() const
{
    if (!isInnerOrRight(table_join->kind()))
        return false;

    bool file_buckets_are_empty = [this]()
    {
        std::shared_lock lock(rehash_mutex);
        return std::all_of(buckets.begin(), buckets.end(), [](const auto & bucket) { return bucket->empty(); });
    }();

    if (!file_buckets_are_empty)
        return false;

    chassert(hash_join);
    bool hash_join_is_empty = hash_join->alwaysReturnsEmptySet();

    return hash_join_is_empty;
}

// IBlocksStreamPtr GraceHashJoin::getNonJoinedBlocks(const Block &, const Block &, UInt64) const
// {
//     /// We do no support returning non joined blocks here.
//     /// TODO: They _should_ be reported by getDelayedBlocks instead
//     return nullptr;
// }

class GraceHashJoin::DelayedBlocks : public IBlocksStream
{
public:
    explicit DelayedBlocks(size_t current_bucket_, Buckets buckets_, InMemoryJoinPtr hash_join_, const Names & left_key_names_, const Names & right_key_names_, const bool rehash_, GraceHashJoin *grace_hash_join_)
        : current_bucket(current_bucket_)
        , buckets(std::move(buckets_))
        , hash_join(std::move(hash_join_))
        , left_reader(buckets[current_bucket]->getLeftTableReader())
        , left_key_names(left_key_names_)
        , right_key_names(right_key_names_)
        , rehash(rehash_)
        , grace_hash_join(grace_hash_join_)
    {
    }

    Block nextImpl0()
    {
        ExtraBlockPtr not_processed = nullptr;

        {
            std::lock_guard lock(extra_block_mutex);
            if (!not_processed_blocks.empty())
            {
                not_processed = std::move(not_processed_blocks.front());
                not_processed_blocks.pop_front();
            }
        }

        if (not_processed)
        {
            Block block = std::move(not_processed->block);
            grace_hash_join->inMemoryJoinBlock(block, not_processed);

            if (not_processed)
            {
                std::lock_guard lock(extra_block_mutex);
                not_processed_blocks.emplace_back(std::move(not_processed));
            }
            return block;
        }

        Block block;
        size_t num_buckets = buckets.size();
        size_t current_idx = buckets[current_bucket]->idx;

        do
        {
            if (eof) 
                return {};
            block = left_reader.read();
            if (!block)
            {
                eof = true;
                return {};
            }

            if (rehash)
            {

                Blocks blocks = JoinCommon::scatterBlockByHash(left_key_names, block, num_buckets);
                block = std::move(blocks[current_idx]);

                /*
                * We need to filter out blocks that were written to the current bucket `B_{n}`
                * but then virtually moved to another bucket `B_{n+i}` on rehash.
                * Bucket `B_{n+i}` is waiting for the buckets with smaller index to be processed,
                * and rows can be moved only forward (because we increase hash modulo twice on each rehash),
                * so it is safe to add blocks.
                */
                for (size_t bucket_idx = 0; bucket_idx < num_buckets; ++bucket_idx)
                {
                    if (blocks[bucket_idx].rows() == 0)
                        continue;

                    if (bucket_idx == current_idx) // Rows that are still in our bucket
                        continue;

                    buckets[bucket_idx]->addLeftBlock(blocks[bucket_idx]);
                }
            }
        } while (block.rows() == 0);

        grace_hash_join->inMemoryJoinBlock(block, not_processed);

        if (not_processed)
        {
            std::lock_guard lock(extra_block_mutex);
            not_processed_blocks.emplace_back(std::move(not_processed));
        }

        return block;
    }

    Block nextImpl() override
    {
        Block ret;
        do {
            if (eof) {
                std::lock_guard lock(extra_block_mutex);
                if (not_processed_blocks.empty()) {
                    return {};
                }
            }
            ret = nextImpl0();
        } while(!ret);
        return ret;
    }

    size_t current_bucket;
    Buckets buckets;
    InMemoryJoinPtr hash_join;

    AccumulatedBlockReader left_reader;

    Names left_key_names;
    Names right_key_names;

    std::mutex extra_block_mutex;
    std::list<ExtraBlockPtr> not_processed_blocks TSA_GUARDED_BY(extra_block_mutex);
    std::atomic<bool> eof{false};

    bool rehash;

    GraceHashJoin *grace_hash_join = nullptr;
};

IBlocksStreamPtr GraceHashJoin::getDelayedBlocks()
{
    std::lock_guard current_bucket_lock(current_bucket_mutex);

    if (current_bucket == nullptr)
        return nullptr;

    size_t bucket_idx = current_bucket->idx;

    for (bucket_idx = bucket_idx + 1; bucket_idx < buckets.size(); ++bucket_idx)
    {
        current_bucket = buckets[bucket_idx].get();
        if (current_bucket->finished() || current_bucket->empty())
        {
            LOG_TRACE(log, "Skipping {} {} bucket {}",
                current_bucket->finished() ? "finished" : "",
                current_bucket->empty() ? "empty" : "",
                bucket_idx);
            continue;
        }

        hash_join = makeInMemoryJoin();
        auto right_reader = current_bucket->startJoining();
        size_t num_rows = 0; /// count rows that were written and rehashed
        while (Block block = right_reader.read())
        {
            num_rows += block.rows();
            addJoinedBlockImpl(std::move(block), true);
        }

        LOG_TRACE(log, "Loaded bucket {} with {}(/{}) rows",
            bucket_idx, hash_join->getTotalRowCount(), num_rows);

        return std::make_unique<DelayedBlocks>(current_bucket->idx, buckets, hash_join, left_key_names, right_key_names, rehash, this);
    }

    LOG_TRACE(log, "Finished loading all {} buckets", buckets.size());

    current_bucket = nullptr;
    return nullptr;
}

GraceHashJoin::InMemoryJoinPtr GraceHashJoin::makeInMemoryJoin()
{
    auto ret = std::make_unique<HashJoin>(table_join, right_sample_block, any_take_last_row);
   
    return ret;
}

Block GraceHashJoin::prepareRightBlock(const Block & block)
{
    return HashJoin::prepareRightBlock(block, hash_join_sample_block);
}

void GraceHashJoin::initMaxJoinedBlockBytesInSpill()
{
    double spill_triger_threshold = context->getSettingsRef().spill_triger_threshold.value; 
    max_joined_block_bytes_in_spill = calculateMaxjoinedBlockBytes(total_memory_tracker.getHardLimit(), total_memory_tracker.getHardLimit()*spill_triger_threshold, num_streams);
    LOG_INFO(log, "Init max_joined_block_bytes_in_spill:{}", max_joined_block_bytes_in_spill);
}

void GraceHashJoin::addJoinedBlockImpl(Block block, bool is_delay_read)
{
    block = prepareRightBlock(block);
    Buckets buckets_snapshot = getCurrentBuckets();
    size_t bucket_index = current_bucket->idx;
    Block current_block;
    if(buckets_snapshot.size() != 1 && (!is_delay_read || rehash))
    {
        Blocks blocks = JoinCommon::scatterBlockByHash(right_key_names, block, buckets_snapshot.size());
        flushBlocksToBuckets<JoinTableSide::Right>(blocks, buckets_snapshot, bucket_index);
        current_block = std::move(blocks[bucket_index]);
    }
    else
    {
        current_block = std::move(block);
    }

    // Add block to the in-memory join
    if (current_block.rows() > 0)
    {
        std::lock_guard lock(hash_join_mutex);

        if (!hash_join)
            hash_join = makeInMemoryJoin();

        // buckets size has been changed in other threads. Need to scatter current_block again.
        // rehash could only happen under hash_join_mutex's scope.
        auto current_buckets = getCurrentBuckets();
        if (buckets_snapshot.size() != current_buckets.size())
        {
            LOG_TRACE(log, "mismatch buckets size. previous:{}, current:{}", buckets_snapshot.size(), getCurrentBuckets().size());
            Blocks blocks = JoinCommon::scatterBlockByHash(right_key_names, current_block, current_buckets.size());
            flushBlocksToBuckets<JoinTableSide::Right>(blocks, current_buckets, bucket_index);
            current_block = std::move(blocks[bucket_index]);
            if (!current_block.rows())
                return;
        }

        // todo aron
        // auto prev_keys_num = hash_join->getTotalRowCount();
        hash_join->addJoinedBlock(current_block, /* check_limits = */ false);

        if (!hasMemoryOverflow(hash_join))
            return;

        last_mem_size_triger_spill = hash_join->getTotalByteCount();

        current_block = {};
        size_t grow_multiplier = 2;
        if (adaptive_spill_mode) {
            while(hash_join->getTotalByteCount()*2 / (grow_multiplier * current_buckets.size()) > max_allowed_mem_size_in_spill) {
                grow_multiplier *= 2;
            }
        }

        LOG_DEBUG(log, "ready to rehashing, mem_bytes:{} grow_multiplier:{} current_bkt_idx:{} current_buckets_num:{}", hash_join->getTotalByteCount(), grow_multiplier, bucket_index, current_buckets.size());
        // Must use the latest buckets snapshot in case that it has been rehashed by other threads.
        buckets_snapshot = rehashBuckets(grow_multiplier);
        auto right_blocks = hash_join->releaseJoinedBlocks(/* restructure */ false);
        hash_join = nullptr;

        {
            Blocks current_blocks;
            current_blocks.reserve(right_blocks.size());
            while(!right_blocks.empty())
            {
                const auto & right_block = right_blocks.front();
                Blocks blocks = JoinCommon::scatterBlockByHash(right_key_names, right_block, buckets_snapshot.size());
                flushBlocksToBuckets<JoinTableSide::Right>(blocks, buckets_snapshot, bucket_index);
                current_blocks.emplace_back(std::move(blocks[bucket_index]));
                right_blocks.pop_front();
            }

            if (current_blocks.size() == 1)
                current_block = std::move(current_blocks.front());
            else
                current_block = concatenateBlocks(current_blocks);
        }

        hash_join = makeInMemoryJoin();

        if (current_block.rows() > 0)
            hash_join->addJoinedBlock(current_block, /* check_limits = */ false);
    }
}

size_t GraceHashJoin::getNumBuckets() const
{
    std::shared_lock lock(rehash_mutex);
    return buckets.size();
}

GraceHashJoin::Buckets GraceHashJoin::getCurrentBuckets() const
{
    std::shared_lock lock(rehash_mutex);
    return buckets;
}

}
