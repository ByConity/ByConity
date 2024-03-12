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
    std::shared_ptr<TableJoin> table_join_, size_t slots_, const Block & right_sample_block_, bool any_take_last_row_)
    : table_join(table_join_), slots(toPowerOfTwo(std::min<size_t>(slots_, 256))), right_sample_block(right_sample_block_)
{
    for (size_t i = 0; i < slots; ++i)
    {
        auto inner_hash_join = std::make_shared<InternalHashJoin>();
        inner_hash_join->data = std::make_unique<HashJoin>(table_join_, right_sample_block_, any_take_last_row_);
        hash_joins.emplace_back(std::move(inner_hash_join));
    }
}

bool ConcurrentHashJoin::addJoinedBlock(const Block & right_block, bool check_limits)
{
    Blocks dispatched_blocks = dispatchBlock(table_join->keyNamesRight(), right_block);

    size_t blocks_left = 0;
    for (const auto & block : dispatched_blocks)
    {
        if (block)
        {
            ++blocks_left;
        }
    }

    while (blocks_left > 0)
    {
        /// insert blocks into corresponding HashJoin instances
        for (size_t i = 0; i < dispatched_blocks.size(); ++i)
        {
            auto & hash_join = hash_joins[i];
            auto & dispatched_block = dispatched_blocks[i];

            if (dispatched_block)
            {
                /// if current hash_join is already processed by another thread, skip it and try later
                std::unique_lock<std::mutex> lock(hash_join->mutex, std::try_to_lock);
                if (!lock.owns_lock())
                    continue;

                bool limit_exceeded = !hash_join->data->addJoinedBlock(dispatched_block, check_limits);

                dispatched_block = {};
                blocks_left--;

                if (limit_exceeded)
                    return false;
            }
        }
    }

    if (check_limits)
        return table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    return true;
}

void ConcurrentHashJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & /*not_processed*/)
{
    Blocks dispatched_blocks = dispatchBlock(table_join->keyNamesLeft(), block);
    block = {};
    for (size_t i = 0; i < dispatched_blocks.size(); ++i)
    {
        std::shared_ptr<ExtraBlock> none_extra_block;
        auto & hash_join = hash_joins[i];
        auto & dispatched_block = dispatched_blocks[i];
        hash_join->data->joinBlock(dispatched_block, none_extra_block);
        if (none_extra_block && !none_extra_block->empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "not_processed should be empty");
    }

    block = concatenateBlocks(dispatched_blocks);
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

template <typename Mapped>
struct AdderNonJoined2
{
    static void add(const Mapped & mapped, size_t & rows_added, MutableColumns & columns_right, const std::shared_ptr<HashJoin::RightTableData> & right_data)
    {
        constexpr bool mapped_asof = std::is_same_v<Mapped, AsofRowRefs>;
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<Mapped, RowRef>;

        if constexpr (mapped_asof)
        {
            /// Do nothing
        }
        else if constexpr (mapped_one)
        {
            if (right_data->checkUsed(mapped.block, mapped.row_num))
                return;
            
            for (size_t j = 0; j < columns_right.size(); ++j)
            {
                const auto & mapped_column = mapped.block->getByPosition(j).column;
                columns_right[j]->insertFrom(*mapped_column, mapped.row_num);
            }

            ++rows_added;
        }
        else
        {
            for (auto it = mapped.begin(); it.ok(); ++it)
            {
                if (right_data->checkUsed(it->block, it->row_num))
                    continue;
                
                for (size_t j = 0; j < columns_right.size(); ++j)
                {
                    const auto & mapped_column = it->block->getByPosition(j).column;
                    columns_right[j]->insertFrom(*mapped_column, it->row_num);
                }

                ++rows_added;
            }
        }
    }
};


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

        for (; it != end; ++it)
        {
            const Mapped & mapped = it->getMapped();

            size_t off = map.offsetInternal(it.getPtr());
            if (parents[current_idx]->isUsed(off))
                continue;

            AdderNonJoined2<Mapped>::add(mapped, rows_added, columns_keys_and_right, parents[current_idx]->data);

            if (rows_added >= max_block_size)
            {
                ++it;
                break;
            }
        }

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
            LOG_TRACE((&Poco::Logger::get("ConcurrentHashJoin")), "create ConcurrentNotJoinedBlockInputStream with total_size:{} index:{} hash_joins:{}", total_size, index, parents.size());
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

Blocks ConcurrentHashJoin::dispatchBlock(const Strings & key_columns_names, const Block & from_block)
{
    size_t num_shards = hash_joins.size();
    size_t num_cols = from_block.columns();

    IColumn::Selector selector = selectDispatchBlock(key_columns_names, from_block);

    Blocks result(num_shards);
    for (size_t i = 0; i < num_shards; ++i)
        result[i] = from_block.cloneEmpty();

    for (size_t i = 0; i < num_cols; ++i)
    {
        auto dispatched_columns = from_block.getByPosition(i).column->scatter(num_shards, selector);
        assert(result.size() == dispatched_columns.size());
        for (size_t block_index = 0; block_index < num_shards; ++block_index)
        {
            result[block_index].getByPosition(i).column = std::move(dispatched_columns[block_index]);
        }
    }
    return result;
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
