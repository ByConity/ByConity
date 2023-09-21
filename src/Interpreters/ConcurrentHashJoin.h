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
    explicit ConcurrentHashJoin(std::shared_ptr<TableJoin> table_join_, size_t slots_, const Block & right_sample_block, bool any_take_last_row_ = false);
    ~ConcurrentHashJoin() override = default;

    bool addJoinedBlock(const Block & block, bool check_limits) override;
    void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed) override;

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

    BlockInputStreamPtr createStreamWithNonJoinedRows(const Block & result_sample_block, UInt64 max_block_size) const override;

    void serialize(WriteBuffer & buf) const override;
    static JoinPtr deserialize(ReadBuffer & buf, ContextPtr context);
    void tryBuildRuntimeFilters(size_t total_rows) const override;

private:
    struct InternalHashJoin
    {
        std::mutex mutex;
        std::unique_ptr<HashJoin> data;
    };

    std::shared_ptr<TableJoin> table_join;
    size_t slots;
    Block right_sample_block;
    std::vector<std::shared_ptr<InternalHashJoin>> hash_joins;

    std::mutex totals_mutex;
    Block totals;

    IColumn::Selector selectDispatchBlock(const Strings & key_columns_names, const Block & from_block);
    Blocks dispatchBlock(const Strings & key_columns_names, const Block & from_block);

};

}
