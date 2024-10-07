#include <memory>
#include <mutex>
#include <any>
#include <limits>
#include <Columns/FilterDescription.h>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/join_common.h>
#include <Interpreters/joinDispatch.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/createBlockSelector.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/parseQuery.h>
#include <QueryPlan/PlanSerDerHelper.h>
#include <Common/Exception.h>
#include <Common/WeakHash.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int UNSUPPORTED_JOIN_KEYS;
}

UInt32 ConcurrentHashJoin::toPowerOfTwo(UInt32 x)
{
    if (x <= 1)
        return 1;
    return static_cast<UInt32>(1) << (32 - std::countl_zero(x - 1));
}

ConcurrentHashJoin::ConcurrentHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    size_t slots_,
    size_t parallel_join_rows_batch_threshold_,
    const Block & right_sample_block_,
    bool any_take_last_row_
)
    : table_join(table_join_)
    , slots(toPowerOfTwo(std::min<size_t>(slots_, 256)))
    , parallel_join_rows_batch_threshold(parallel_join_rows_batch_threshold_)
    , right_sample_block(right_sample_block_)
    , hash_joins_input_queue_lock(slots)
    , hash_joins_input_queue(slots)
    , hash_joins_task_in_queue(slots, false)
{
    LOG_DEBUG(getLogger("ConcurrentHashJoin"), fmt::format("parallel_join_rows_batch_threshold:{}", parallel_join_rows_batch_threshold));
    /// Non zero `max_joined_block_rows` allows to process block partially and return not processed part.
    /// TODO: It's not handled properly in ConcurrentHashJoin case, so we set it to 0 to disable this feature.
    table_join->setMaxJoinedBlockRows(0);
    for (size_t i = 0; i < slots; ++i)
    {
        auto inner_hash_join = std::make_shared<InternalHashJoin>();
        inner_hash_join->data = std::make_unique<HashJoin>(table_join_, right_sample_block_, any_take_last_row_);
        hash_joins.emplace_back(std::move(inner_hash_join));
    }
}

void ConcurrentHashJoin::selectDispatchBlock(
    const Strings & key_columns_names,
    const Block & from_block,
    IColumn::Selector & res_selector,
    RepartitionTransform::PartitionStartPoints & res_selector_points
) {
    size_t num_rows = from_block.rows();
    size_t num_shards = hash_joins.size();

    WeakHash32 hash(num_rows);
    for (const auto & key_name : key_columns_names)
    {
        const auto & key_col = from_block.getByName(key_name).column->convertToFullColumnIfConst();
        const auto & key_col_no_lc = recursiveRemoveLowCardinality(key_col);
        key_col_no_lc->updateWeakHash32(hash);
    }

    const auto & data = hash.getData();

    VectorWithAlloc<DB::UInt64> row_to_shard(num_rows);
    VectorWithAlloc<size_t> shard_count(num_shards);
    for (size_t row_num = 0; row_num < num_rows; ++row_num)
    {
        auto v = intHash64(data[row_num]) & (num_shards - 1);
        row_to_shard[row_num] = v;
        shard_count[v] += 1;
    }
    res_selector_points.resize(num_shards+1);
    res_selector_points[0] = 0;

    VectorWithAlloc<size_t> shard_offset(num_shards);
    for (size_t shard_num = 0; shard_num < num_shards; ++shard_num)
    {
        res_selector_points[shard_num+1] = res_selector_points[shard_num] + shard_count[shard_num];
        shard_offset[shard_num] = res_selector_points[shard_num];
    }

    res_selector.resize(num_rows);
    for (size_t row_num = 0; row_num < num_rows; ++row_num)
    {
        size_t shard_num = row_to_shard[row_num];
        res_selector[shard_offset[shard_num]++] = row_num;
    }
}

void ConcurrentHashJoin::dispatchBlock(
    const Strings & key_columns_names, const Block & from_block, std::vector<MutableColumns> & result, Block & block_head)
{
    size_t num_shards = hash_joins.size();
    size_t num_cols = from_block.columns();

    if (unlikely(result.size() != num_shards))
        throw Exception(ErrorCodes::LOGICAL_ERROR, fmt::format("dispatchBlock result size({}) is diff from num_shards({})", result.size(), num_shards));

    for (auto & current_result : result)
    {
        if (current_result.empty())
        {
            current_result = block_head.cloneEmptyColumns();
        }
    }

    if (unlikely(num_cols == 0))
    {
        return;
    }

    IColumn::Selector selector;
    RepartitionTransform::PartitionStartPoints selector_points;
    selectDispatchBlock(key_columns_names, from_block, selector, selector_points);

#ifndef NDEBUG
    if (selector_points.size() != num_shards+1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, fmt::format("dispatchBlock selector_points size({}) is diff from num_shards+1({})", selector_points.size(), num_shards+1));
#endif

    for (size_t i = 0; i < num_cols; ++i)
    {
        const auto & source = from_block.getByPosition(i).column;
        for (size_t block_index = 0; block_index < num_shards; ++block_index)
        {
            size_t from = selector_points[block_index];
            size_t length = selector_points[block_index + 1] - from;
            if (length == 0)
                continue;

#ifndef NDEBUG
            if (from + length > selector.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, fmt::format("from({}) + length({}) >= selector.size()({})", from, length, selector.size()));
            if (result[block_index].size() != num_cols)
                throw Exception(ErrorCodes::LOGICAL_ERROR, fmt::format("result[{}}].size()({}) != num_cols({})", block_index, result[block_index].size(), num_cols));
#endif
            result[block_index][i]->insertRangeSelective(*source, selector, from, length);
        }
    }
}

bool ConcurrentHashJoin::addJoinedBlock(
    const Block & right_block, DispatchedColumnsCache *dispatched_columns_cache, bool check_limits)
{
    size_t num_shards = hash_joins.size();
    bool final_block = !right_block.rows();

    DispatchedColumnsCache * right_block_dispatched_cache = dispatched_columns_cache;
    if (right_block_dispatched_cache->block_head.columns() == 0)
    {
        hash_join_inserter_left.fetch_add(1);
        right_block_dispatched_cache->block_head = right_block.cloneEmpty();
        right_block_dispatched_cache->columns_cache = std::vector<MutableColumns>(num_shards);
    }

    auto & right_block_head = right_block_dispatched_cache->block_head;
    auto & columns_cache = right_block_dispatched_cache->columns_cache;

    // dispatch block to num_shards shard, save in columns_cache.
    if (right_block.rows() > 0)
    {
        dispatchBlock(table_join->keyNamesRight(), right_block, columns_cache, right_block_head);
    }

    for (size_t i = 0; i < num_shards; ++i)
    {
        if (columns_cache[i].empty() || columns_cache[i][0]->empty())
        {
            continue;
        }

        if (columns_cache[i][0]->size() < parallel_join_rows_batch_threshold && !final_block)
        {
            continue;
        }

        // Move data from columns_cache to hash_joins_input_queue, then can be handled.
        auto input_ptr = std::make_shared<Block>(right_block_head.cloneEmpty());
        input_ptr->setColumns(std::move(columns_cache[i]));
        columns_cache[i] = MutableColumns();
        {
            std::unique_lock<std::mutex> lock(hash_joins_input_queue_lock[i]);
            hash_joins_input_queue[i].emplace(std::move(input_ptr));
        }
        // There is data for shard i to be handle, add to hash_joins_task_in_queue if not in.
        {
            std::unique_lock<std::mutex> lock(hash_joins_task_queue_lock);
            if (!hash_joins_task_in_queue[i])
            {
                hash_joins_task_in_queue[i] = true;
                hash_joins_build_task.push(i);
            }
        }
    }

    // Get shard to be handle from hash_joins_task_in_queue
    // Then get data from hash_joins_task_in_queue for shard and run addJoinedBlock
    size_t tasks_left = 0;
    bool task_mainly_finished = false;
    do
    {
        size_t task_id = -1;
        {
            std::unique_lock<std::mutex> lock_queue(hash_joins_task_queue_lock);
            if (hash_joins_build_task.empty())
            {
                tasks_left = 0;
                if (!final_block)
                    break;
                else
                    goto final_check;
            }
            task_id = hash_joins_build_task.front();
            hash_joins_build_task.pop();

            hash_joins_task_in_queue[task_id] = false;
        }

        {
            // get hash_join lock for shard, and handle all data in hash_joins_task_in_queue for this shard.
            auto & hash_join = hash_joins[task_id];
            std::unique_lock<std::mutex> lock_hash(hash_join->mutex, std::try_to_lock);
            if (!lock_hash.owns_lock())
            {
                // Get lock failed, this shard should be handling by other thread,
                // we put this shard in hash_joins_task_in_queue again in case handling thread not get all data.
                // but according to test, this has some negtive affect for performance. we left the check to last task.
                /*
                std::unique_lock<std::mutex> lock_queue(hash_joins_task_queue_lock);
                if (!hash_joins_task_in_queue[task_id])
                {
                    hash_joins_task_in_queue[task_id] = true;
                    hash_joins_build_task.push(task_id);
                }
                */
                // If this time is not the last time, then we just quit.
                // If this time is last time, we loop until hash_joins_build_task is empty.
                if (!final_block)
                    break;
                else
                    goto final_check;
            }

            while (true)
            {
                BlockPtr dispatched_block_ptr;
                {
                    std::unique_lock<std::mutex> lock(hash_joins_input_queue_lock[task_id]);
                    if (hash_joins_input_queue[task_id].empty())
                        break;
                    dispatched_block_ptr = std::move(hash_joins_input_queue[task_id].front());
                    hash_joins_input_queue[task_id].pop();
                }
                if (!dispatched_block_ptr)
                    break;
                bool limit_exceeded = !hash_join->data->addJoinedBlock(*dispatched_block_ptr, check_limits);

                if (limit_exceeded)
                    return false;

                {
                    std::unique_lock<std::mutex> lock_queue(hash_joins_task_queue_lock);
                    if (!hash_joins_build_task.empty() && hash_joins_build_task.front() == task_id)
                    {
                        hash_joins_build_task.pop();
                        hash_joins_task_in_queue[task_id] = false;
                    }
                    tasks_left = hash_joins_build_task.size();
                }
            }
        }

final_check:
        if (tasks_left == 0 && final_block && !task_mainly_finished)
        {
            task_mainly_finished = true;
            /* last inserter, check tasks again */
            if (1 == hash_join_inserter_left.fetch_sub(1))
            {
                std::unique_lock<std::mutex> lock_queue(hash_joins_task_queue_lock);
                for (size_t i = 0; i < num_shards; ++i)
                {
                    hash_joins_task_in_queue[i] = true;
                    hash_joins_build_task.push(i);
                }
                tasks_left = num_shards;
            }
        }
    } while (tasks_left > 0);

    if (check_limits)
        return table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    return true;
}

bool ConcurrentHashJoin::addJoinedBlock([[maybe_unused]]const Block & right_block, [[maybe_unused]]bool check_limits)
{
    throw Exception("ConcurrentHashJoin unsupport addJoinedBlock with no dispatched_columns_cache", ErrorCodes::LOGICAL_ERROR);
}

void ConcurrentHashJoin::joinBlock(
    Block & block, std::shared_ptr<ExtraBlock> & /*not_processed*/, DispatchedColumnsCache* dispatched_columns_cache, BlocksPtr & dispatched_blocks)
{
    if (!dispatched_columns_cache) // for transformHeader, only run joinBlock onetime.
    {
        std::shared_ptr<ExtraBlock> none_extra_block;
        hash_joins[0]->data->joinBlock(block, none_extra_block);
        return;
    }

    bool final_block = block.rows() == 0;
    size_t num_shards = hash_joins.size();

    DispatchedColumnsCache * left_block_dispatched_cache = dispatched_columns_cache;
    if (left_block_dispatched_cache->block_head.columns() == 0)
    {
        if (block.columns() == 0)
            return;
        left_block_dispatched_cache->block_head = block.cloneEmpty();
        left_block_dispatched_cache->columns_cache = std::vector<MutableColumns>(num_shards);
    }

    auto & current_left_block_head = left_block_dispatched_cache->block_head;
    auto & columns_cache = left_block_dispatched_cache->columns_cache;

    dispatchBlock(table_join->keyNamesLeft(), block, columns_cache, current_left_block_head);
    block = {};

    dispatched_blocks = std::make_shared<Blocks>();

    for (size_t i = 0; i < num_shards; ++i)
    {
        if (columns_cache[i].empty() || columns_cache[i][0]->empty())
            continue;

        if (columns_cache[i][0]->size() < parallel_join_rows_batch_threshold && !final_block)
            continue;

        std::shared_ptr<ExtraBlock> none_extra_block;
        auto & hash_join = hash_joins[i];
        size_t concat_rows_threshold = parallel_join_rows_batch_threshold/4;

        Block tmp_block = current_left_block_head.cloneEmpty();
        tmp_block.setColumns(std::move(columns_cache[i]));
        hash_join->data->joinBlock(tmp_block, none_extra_block);

        if (block.rows() < concat_rows_threshold)
        {
            if (block.columns() == 0)
            {
                block = tmp_block.cloneEmpty();
                block.setColumns(tmp_block.mutateColumns());
            }
            else
            {
                auto former_columns = block.mutateColumns();
                for (size_t j = 0; j < former_columns.size(); ++j)
                {
                    const auto & tmp_column = *tmp_block.getByPosition(j).column;
                    former_columns[j]->insertRangeFrom(tmp_column, 0, tmp_block.rows());
                }
                block.setColumns(std::move(former_columns));
            }
        }
        else
        {
            if (dispatched_blocks->empty() || dispatched_blocks->back().rows() >= concat_rows_threshold)
            {
                dispatched_blocks->push_back(tmp_block.cloneEmpty());
                dispatched_blocks->back().setColumns(tmp_block.mutateColumns());
            }
            else
            {
                auto former_columns = dispatched_blocks->back().mutateColumns();
                for (size_t j = 0; j < former_columns.size(); ++j)
                {
                    const auto & tmp_column = *tmp_block.getByPosition(j).column;
                    former_columns[j]->insertRangeFrom(tmp_column, 0, tmp_block.rows());
                }
                dispatched_blocks->back().setColumns(std::move(former_columns));
            }
        }

        columns_cache[i] = MutableColumns();
        if (none_extra_block && !none_extra_block->empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "not_processed should be empty");
    }
}

void ConcurrentHashJoin::joinBlock(
    Block &, std::shared_ptr<ExtraBlock> & /*not_processed*/)
{
    throw Exception("ConcurrentHashJoin unsupport joinBlock with no dispatched_columns_cache", ErrorCodes::LOGICAL_ERROR);
}

void ConcurrentHashJoin::setTotals(const Block & block)
{
    if (block)
    {
        std::lock_guard lock(totals_mutex);
        totals = block;
    }
}

const Block & ConcurrentHashJoin::getTotals() const
{
    return totals;
}

size_t ConcurrentHashJoin::getTotalRowCount() const
{
    size_t res = 0;
    for (const auto & hash_join : hash_joins)
    {
        std::lock_guard lock(hash_join->mutex);
        res += hash_join->data->getTotalRowCount();
    }
    return res;
}

size_t ConcurrentHashJoin::getTotalByteCount() const
{
    size_t res = 0;
    for (const auto & hash_join : hash_joins)
    {
        std::lock_guard lock(hash_join->mutex);
        res += hash_join->data->getTotalByteCount();
    }
    return res;
}

bool ConcurrentHashJoin::alwaysReturnsEmptySet() const
{
    for (const auto & hash_join : hash_joins)
    {
        std::lock_guard lock(hash_join->mutex);
        if (!hash_join->data->alwaysReturnsEmptySet())
            return false;
    }
    return true;
}

/// Stream from not joined earlier rows of the right table.
class ConcurrentNotJoinedBlockInputStream : private NotJoined, public IBlockInputStream
{
public:
    ConcurrentNotJoinedBlockInputStream(std::vector<HashJoin*> && parents_, const Block & result_sample_block_, UInt64 max_block_size_)
        : NotJoined(*parents_[0]->table_join,
                    parents_[0]->savedBlockSample(),
                    parents_[0]->right_sample_block,
                    result_sample_block_)
        , parents(parents_)
        , max_block_size(max_block_size_)
    {}

    String getName() const override { return "NonJoined2"; }
    Block getHeader() const override { return result_sample_block; }

protected:
    Block readImpl() override
    {
        while (current_idx < parents.size())
        {
            if (parents[current_idx]->data->blocks.empty())
                current_idx++;
            else
                break;
        }
        if (current_idx >= parents.size())
            return Block();
        Block res;
        while (!createBlock(res));

        return res;
    }

private:
    std::vector<HashJoin*> parents;
    size_t current_idx = 0;
    UInt64 max_block_size;

    std::any position;
    std::optional<HashJoin::BlockNullmapList::const_iterator> nulls_position;

    bool createBlock(Block & res)
    {
        if (current_idx >= parents.size())
            return true;
        HashJoin* parent = parents[current_idx];
        MutableColumns columns_right = saved_block_sample.cloneEmptyColumns();

        size_t rows_added = 0;

        auto fill_callback = [&](auto, auto strictness, auto & map)
        {
            rows_added = fillColumnsFromMap<strictness>(map, columns_right);
        };

        if (!joinDispatch(parent->kind, parent->strictness, parent->data->maps, fill_callback, parent->has_inequal_condition))
            throw Exception("Logical error: unknown JOIN strictness (must be on of: ANY, ALL, ASOF)", ErrorCodes::LOGICAL_ERROR);

        fillNullsFromBlocks(columns_right, rows_added);
        if (!rows_added)
        {
            current_idx++;
            return false;
        }

        correctLowcardAndNullability(columns_right);

        res = result_sample_block.cloneEmpty();
        addLeftColumns(res, rows_added);
        addRightColumns(res, columns_right);
        copySameKeys(res);
        return true;
    }

    template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
    size_t fillColumnsFromMap(const Maps & maps, MutableColumns & columns_keys_and_right)
    {
        switch (parents[current_idx]->data->type)
        {
#define M(TYPE) \
            case HashJoin::Type::TYPE: \
                return fillColumns<STRICTNESS>(*maps.TYPE, columns_keys_and_right);
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            default:
                throw Exception("Unsupported JOIN keys. Type: " + toString(static_cast<UInt32>(parents[current_idx]->data->type)),
                                ErrorCodes::UNSUPPORTED_JOIN_KEYS);
        }

        __builtin_unreachable();
    }

    template <ASTTableJoin::Strictness STRICTNESS, typename Map>
    size_t fillColumns(const Map & map, MutableColumns & columns_keys_and_right)
    {
        using Mapped = typename Map::mapped_type;
        using Iterator = typename Map::const_iterator;

        size_t rows_added = 0;

        if (!position.has_value())
            position = std::make_any<Iterator>(map.begin());

        Iterator & it = std::any_cast<Iterator &>(position);
        auto end = map.end();

        const static size_t BATCH_SIZE = 512;
        size_t batch_pos = 0;
        UInt64 rows_with_block[BATCH_SIZE];

        const auto populate_col = [&]()
        {
            for (size_t j = 0; j < columns_keys_and_right.size(); ++j)
            {
                columns_keys_and_right[j]->reserve(columns_keys_and_right[j]->size() + batch_pos);
                for (size_t ind = 0; ind < batch_pos; ++ind)
                {
                    const RowRef * ref = reinterpret_cast<RowRef *>(rows_with_block[ind]);
                    const auto & mapped_column = ref->block->getByPosition(j).column;
                    columns_keys_and_right[j]->insertFrom(*mapped_column, ref->row_num);
                }
            }
            batch_pos = 0;
        };

        constexpr bool mapped_asof = std::is_same_v<Mapped, AsofRowRefs>;
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<Mapped, RowRef>;

        for (; it != end; ++it)
        {
            const Mapped & mapped = it->getMapped();

            size_t off = map.offsetInternal(it.getPtr());
            if (parents[current_idx]->isUsed(off))
                continue;

            const auto & right_data = parents[current_idx]->data;

            if constexpr (mapped_asof)
            {
                /// Do nothing
            }
            else if constexpr (mapped_one)
            {
                if (right_data->checkUsed(mapped.block, mapped.row_num))
                    continue;

                rows_with_block[batch_pos++] = reinterpret_cast<UInt64>(&mapped);
                ++rows_added;
            }
            else
            {
                for (auto itt = mapped.begin(); itt.ok(); ++itt)
                {
                    if (right_data->checkUsed(itt->block, itt->row_num))
                        continue;

                    rows_with_block[batch_pos++] = reinterpret_cast<UInt64>(itt.ptr());
                    ++rows_added;
                    if (batch_pos == BATCH_SIZE)
                        populate_col();

                }
            }

            if (batch_pos == BATCH_SIZE)
                populate_col();

            if (rows_added >= max_block_size)
            {
                ++it;
                break;
            }
        }

        if (batch_pos > 0)
            populate_col();

        return rows_added;
    }

    void fillNullsFromBlocks(MutableColumns & columns_keys_and_right, size_t & rows_added)
    {
        if (!nulls_position.has_value())
            nulls_position = parents[current_idx]->data->blocks_nullmaps.begin();

        auto end = parents[current_idx]->data->blocks_nullmaps.end();

        auto & it = *nulls_position;
        for (; it != end && rows_added < max_block_size; ++it)
        {
            const Block * block = it->first;
            const NullMap & nullmap = assert_cast<const ColumnUInt8 &>(*it->second).getData();

            for (size_t row = 0; row < nullmap.size(); ++row)
            {
                if (nullmap[row])
                {
                    for (size_t col = 0; col < columns_keys_and_right.size(); ++col)
                        columns_keys_and_right[col]->insertFrom(*block->getByPosition(col).column, row);
                    ++rows_added;
                }
            }
        }
        if (it == end && (rows_added == 0))
        {
            position.reset();
            nulls_position.reset();
        }

    }
};


BlockInputStreamPtr ConcurrentHashJoin::createStreamWithNonJoinedRows(const Block & result_sample_block, UInt64 max_block_size, size_t total_size, size_t index) const
{
    if (table_join->strictness() == ASTTableJoin::Strictness::Asof ||
        table_join->strictness() == ASTTableJoin::Strictness::Semi ||
        !isRightOrFull(table_join->kind()))
        return {};

    if (isRightOrFull(table_join->kind()))
    {
        std::vector<HashJoin*> parents;
        for (size_t i = 0; i < hash_joins.size(); i++)
        {
            if (i % total_size == index)
            {
                if (!hash_joins[i]->data->data->blocks.empty())
                    parents.push_back(&(*hash_joins[i]->data));
            }
        }

        if (!parents.empty())
        {
            LOG_TRACE((getLogger("ConcurrentHashJoin")), "create ConcurrentNotJoinedBlockInputStream with total_size:{} index:{} hash_joins:{}", total_size, index, parents.size());
            return std::make_shared<ConcurrentNotJoinedBlockInputStream>(std::move(parents), result_sample_block, max_block_size);
        }
        else
            return {};
    }
    return {};
}


static ALWAYS_INLINE IColumn::Selector hashToSelector(const WeakHash32 & hash, size_t num_shards)
{
    assert(num_shards > 0 && (num_shards & (num_shards - 1)) == 0);
    const auto & data = hash.getData();
    size_t num_rows = data.size();

    IColumn::Selector selector(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        /// Apply intHash64 to mix bits in data.
        /// HashTable internally uses WeakHash32, and we need to get different lower bits not to cause collisions.
        selector[i] = intHash64(data[i]) & (num_shards - 1);
    return selector;
}

IColumn::Selector ConcurrentHashJoin::selectDispatchBlock(const Strings & key_columns_names, const Block & from_block)
{
    size_t num_rows = from_block.rows();
    size_t num_shards = hash_joins.size();

    WeakHash32 hash(num_rows);
    for (const auto & key_name : key_columns_names)
    {
        const auto & key_col = from_block.getByName(key_name).column->convertToFullColumnIfConst();
        const auto & key_col_no_lc = recursiveRemoveLowCardinality(key_col);
        key_col_no_lc->updateWeakHash32(hash);
    }
    return hashToSelector(hash, num_shards);
}

void ConcurrentHashJoin::tryBuildRuntimeFilters() const
{
    size_t total_rows = 0;
    for (const auto & hash_join : hash_joins)
    {
        total_rows += hash_join->data->getTotalRowCount();
    }

    if (total_rows == 0)
    {
        // need bypass
        hash_joins.front()->data->bypassRuntimeFilters(BypassType::BYPASS_EMPTY_HT, 0);
        return ;
    }

    if (total_rows > table_join->getInBuildThreshold() && total_rows > table_join->getBloomBuildThreshold())
    {
        // need bypass
        hash_joins.front()->data->bypassRuntimeFilters(BypassType::BYPASS_LARGE_HT, total_rows);
        return ;
    }

    for (const auto & hash_join : hash_joins)
    {
        hash_join->data->tryBuildRuntimeFilters();
    }
}

}
