#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Common/Arena.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

class PartitionedBlockOutputStream : public IBlockOutputStream
{
public:
    static constexpr auto PARTITION_ID_WILDCARD = "partition_*";
    static constexpr auto PARTITION_ID_REPLACE = "*";

    PartitionedBlockOutputStream(const ContextPtr & context_, const ASTPtr & partition_by, const Block & sample_block_);

    String getName() const override { return "PartitionedBlockOutputStream"; }

    Block getHeader() const override { return sample_block; }

    void write(const Block & block) override;

    void writeSuffix() override
    {
        for (const auto & [_, stream]: partition_id_to_stream)
        {
            stream->writeSuffix();
        }
    }

    virtual BlockOutputStreamPtr createStreamForPartition(const String & partition_id) = 0;

    static void validatePartitionKey(const String & str, bool allow_slash);

    static String replaceWildcards(const String & haystack, const String & partition_id, UInt32 parallel_index);

    ContextPtr query_context; // Note: make sure init by `query_context` but `global_context`
    Block sample_block;

private:
    ExpressionActionsPtr partition_by_expr;
    String partition_by_column_name;

    // todo(jiashuo): use absl::flat_hash_map
    std::unordered_map<StringRef, BlockOutputStreamPtr> partition_id_to_stream{};
    HashMapWithSavedHash<StringRef, size_t> partition_id_to_block_index{};
    IColumn::Selector block_row_index_to_partition_index{};
    Arena partition_keys_arena;

    BlockOutputStreamPtr getStreamForPartitionKey(StringRef partition_key);
};

}
