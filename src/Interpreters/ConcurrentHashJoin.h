#pragma once

#include <condition_variable>
#include <memory>
#include <optional>
#include <Core/BackgroundSchedulePool.h>
#include <Functions/FunctionsLogical.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/IJoin.h>
#include <Common/Stopwatch.h>
#include <Processors/Exchange/RepartitionTransform.h>
#include <Processors/Transforms/JoiningTransform.h>

namespace DB
{

/**
 * Can run addJoinedBlock() parallelly to speedup the join process. On test, it almose linear speedup by
 * the degree of parallelism.
 *
 * The default HashJoin is not thread safe for inserting right table's rows and run it in a single thread. When
 * the right table is large, the join process is too slow.
 *
 * We create multiple HashJoin instances here. In addJoinedBlock(), one input block is split into multiple blocks
 * corresponding to the HashJoin instances by hashing every row on the join keys. And make a guarantee that every HashJoin
 * instance is written by only one thread.
 *
 * When come to the left table matching, the blocks from left table are alse split into different HashJoin instances.
 *
 */
class ConcurrentHashJoin : public IJoin
{

public:
    explicit ConcurrentHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        size_t slots_,
        size_t parallel_join_rows_batch_threshold_,
        const Block & right_sample_block_,
        bool any_take_last_row_ = false
    );
    ~ConcurrentHashJoin() override = default;

    bool addJoinedBlock(const Block & block, bool check_limits) override;
    void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed) override;

    bool addJoinedBlock(const Block & block, DispatchedColumnsCache *dispatched_columns_cache, bool check_limits);
    void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed, DispatchedColumnsCache *dispatched_columns_cache, BlocksPtr & dispatched_blocks);

    JoinType getType() const override { return JoinType::PARALLEL_HASH; }
    const TableJoin & getTableJoin() const override { return *table_join; }
    TableJoin & getTableJoin() override { return *table_join; }
    void checkTypesOfKeys(const Block & block) const;
    void setTotals(const Block & block) override;
    const Block & getTotals() const override;
    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    bool alwaysReturnsEmptySet() const override;
    bool supportParallelJoin() const override { return true; }

    BlockInputStreamPtr createStreamWithNonJoinedRows(const Block & result_sample_block, UInt64 max_block_size, size_t, size_t) const override;

    void tryBuildRuntimeFilters() const override;
    static UInt32 toPowerOfTwo(UInt32 x);

private:
    friend class ConcurrentNotJoinedBlockInputStream;
    struct InternalHashJoin
    {
        std::mutex mutex;
        std::unique_ptr<HashJoin> data;
    };

    std::shared_ptr<TableJoin> table_join;
    size_t slots;
    size_t parallel_join_rows_batch_threshold;
    Block right_sample_block;
    std::vector<std::shared_ptr<InternalHashJoin>> hash_joins;
    std::atomic<size_t> hash_join_inserter_left {0};

    std::vector<std::mutex> hash_joins_input_queue_lock;
    std::vector<std::queue<BlockPtr>> hash_joins_input_queue;

    std::mutex hash_joins_task_queue_lock;
    std::queue<size_t> hash_joins_build_task;
    std::vector<bool> hash_joins_task_in_queue;

    std::mutex totals_mutex;
    Block totals;

    IColumn::Selector selectDispatchBlock(const Strings & key_columns_names, const Block & from_block);
    void dispatchBlock(const Strings & key_columns_names, const Block & from_block, std::vector<MutableColumns> & result, Block & block_head);
    void selectDispatchBlock(
        const Strings & key_columns_names,
        const Block & from_block,
        IColumn::Selector & res_selector,
        RepartitionTransform::PartitionStartPoints & res_selector_points
    );
};

}
