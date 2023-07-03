#include <Interpreters/JoinUtils.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/TableJoin.h>

#include <IO/WriteHelpers.h>

#include <Common/HashTable/Hash.h>
#include <Common/WeakHash.h>

#include <common/FnTraits.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
}

namespace JoinCommon
{

ColumnPtr materializeColumn(const Block & block, const String & column_name)
{
    const auto & src_column = block.getByName(column_name).column;
    // todo aron sparse column
    // return recursiveRemoveLowCardinality(recursiveRemoveSparse(src_column->convertToFullColumnIfConst()));
    return recursiveRemoveLowCardinality(src_column->convertToFullColumnIfConst());
}

template <Fn<size_t(size_t)> Sharder>
static IColumn::Selector hashToSelector(const WeakHash32 & hash, Sharder sharder)
{
    const auto & hashes = hash.getData();
    size_t num_rows = hashes.size();

    IColumn::Selector selector(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        selector[i] = sharder(intHashCRC32(hashes[i]));
    return selector;
}

template <Fn<size_t(size_t)> Sharder>
static Blocks scatterBlockByHashImpl(const Strings & key_columns_names, const Block & block, size_t num_shards, Sharder sharder)
{
    size_t num_rows = block.rows();
    size_t num_cols = block.columns();

    /// Use non-standard initial value so as not to degrade hash map performance inside shard that uses the same CRC32 algorithm.
    WeakHash32 hash(num_rows);
    for (const auto & key_name : key_columns_names)
    {
        ColumnPtr key_col = materializeColumn(block, key_name);
        key_col->updateWeakHash32(hash);
    }
    auto selector = hashToSelector(hash, sharder);

    Blocks result;
    result.reserve(num_shards);
    for (size_t i = 0; i < num_shards; ++i)
    {
        result.emplace_back(block.cloneEmpty());
    }

    for (size_t i = 0; i < num_cols; ++i)
    {
        auto dispatched_columns = block.getByPosition(i).column->scatter(num_shards, selector);
        assert(result.size() == dispatched_columns.size());
        for (size_t block_index = 0; block_index < num_shards; ++block_index)
        {
            result[block_index].getByPosition(i).column = std::move(dispatched_columns[block_index]);
        }
    }
    return result;
}

static Blocks scatterBlockByHashPow2(const Strings & key_columns_names, const Block & block, size_t num_shards)
{
    size_t mask = num_shards - 1;
    return scatterBlockByHashImpl(key_columns_names, block, num_shards, [mask](size_t hash) { return hash & mask; });
}

static Blocks scatterBlockByHashGeneric(const Strings & key_columns_names, const Block & block, size_t num_shards)
{
    return scatterBlockByHashImpl(key_columns_names, block, num_shards, [num_shards](size_t hash) { return hash % num_shards; });
}

Blocks scatterBlockByHash(const Strings & key_columns_names, const Block & block, size_t num_shards)
{
    if (num_shards == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of shards must be positive");
    if (likely(isPowerOf2(num_shards)))
        return scatterBlockByHashPow2(key_columns_names, block, num_shards);
    return scatterBlockByHashGeneric(key_columns_names, block, num_shards);
}
}

}
