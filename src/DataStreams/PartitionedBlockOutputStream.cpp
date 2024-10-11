#include "PartitionedBlockOutputStream.h"

#include <Functions/FunctionsConversion.h>
#include <common/types.h>
#include <common/getFQDNOrHostName.h>
#include <Common/ArenaUtils.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>

#include <boost/algorithm/string/replace.hpp>
#include <fmt/core.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
}

PartitionedBlockOutputStream::PartitionedBlockOutputStream(
    const ContextPtr & context_, const ASTPtr & partition_by, const Block & sample_block_)
    : query_context(context_), sample_block(sample_block_)
{
    std::vector<ASTPtr> arguments(1, partition_by);
    ASTPtr partition_by_string = makeASTFunction(FunctionToString::name, std::move(arguments));

    auto syntax_result = TreeRewriter(query_context).analyze(partition_by_string, sample_block.getNamesAndTypesList());
    partition_by_expr = ExpressionAnalyzer(partition_by_string, syntax_result, query_context).getActions(false);
    partition_by_column_name = partition_by_string->getColumnName();
}


BlockOutputStreamPtr PartitionedBlockOutputStream::getStreamForPartitionKey(StringRef partition_key)
{
    auto it = partition_id_to_stream.find(partition_key);
    if (it == partition_id_to_stream.end())
    {
        auto stream = createStreamForPartition(partition_key.toString());
        std::tie(it, std::ignore) = partition_id_to_stream.emplace(partition_key, stream);
    }

    return it->second;
}

void PartitionedBlockOutputStream::write(const Block & block)
{
    const auto & columns_with_type_and_name = block.getColumnsWithTypeAndName();
    const auto & columns = block.getColumns();

    Block block_with_partition_by_expr = sample_block.cloneWithoutColumns();
    block_with_partition_by_expr.setColumns(columns);
    partition_by_expr->execute(block_with_partition_by_expr);

    const auto * partition_by_result_column = block_with_partition_by_expr.getByName(partition_by_column_name).column.get();

    size_t rows_size = block.rows();
    block_row_index_to_partition_index.resize(rows_size);

    partition_id_to_block_index.clear();

    for (size_t row = 0; row < rows_size; ++row)
    {
        auto partition_key = partition_by_result_column->getDataAt(row);
        auto [it, inserted] = partition_id_to_block_index.insert(makePairNoInit(partition_key, partition_id_to_block_index.size()));
        if (inserted)
            it->value.first = copyStringInArena(partition_keys_arena, partition_key);

        block_row_index_to_partition_index[row] = it->getMapped();
    }

    size_t columns_size = columns.size();
    size_t partitions_size = partition_id_to_block_index.size();

    Blocks partition_index_to_block;
    partition_index_to_block.reserve(partitions_size);

    for (size_t column_index = 0; column_index < columns_size; ++column_index)
    {
        MutableColumns partition_index_to_column_split
            = columns[column_index]->scatter(partitions_size, block_row_index_to_partition_index);

        /// add empty block into partition_index_to_block in first loop
        if (column_index == 0)
        {
            size_t size = 0;
            while (size++ < partitions_size)
            {
                partition_index_to_block.emplace_back(Block());
            }
        }

        auto type = columns_with_type_and_name[column_index].type;
        auto name = columns_with_type_and_name[column_index].name;
        for (size_t partition_index = 0; partition_index < partitions_size; ++partition_index)
        {
            ColumnWithTypeAndName column_with_type_and_name(std::move(partition_index_to_column_split[partition_index]), type, name);
            partition_index_to_block[partition_index].insert(std::move(column_with_type_and_name));
        }
    }

    for (const auto & partition : partition_id_to_block_index)
    {
        auto key = partition.getKey();
        auto index = partition.getValue().second;
        auto stream = getStreamForPartitionKey(key);
        stream->write(std::move(partition_index_to_block[index]));
    }
}


void PartitionedBlockOutputStream::validatePartitionKey(const String & str, bool allow_slash)
{
    for (const char * i = str.data(); i != str.data() + str.size(); ++i)
    {
        if (static_cast<UInt8>(*i) < 0x20 || *i == '{' || *i == '}' || *i == '*' || *i == '?' || (!allow_slash && *i == '/'))
        {
            /// Need to convert to UInt32 because UInt8 can't be passed to format due to "mixing character types is disallowed".
            UInt32 invalid_char_byte = static_cast<UInt32>(static_cast<UInt8>(*i));
            throw DB::Exception(
                ErrorCodes::CANNOT_PARSE_TEXT,
                "Illegal character '\\x{:02x}' in partition id starting with '{}'",
                invalid_char_byte,
                std::string(str.data(), i - str.data()));
        }
    }
}


String PartitionedBlockOutputStream::replaceWildcards(const String & haystack, const String & partition_id, UInt32 parallel_index)
{
    String replace_str
        = partition_id.empty() ? fmt::format("{}_{}", parallel_index, getPodOrHostName()) : fmt::format("{}_{}_{}", partition_id, parallel_index, getPodOrHostName());
    return boost::replace_all_copy(haystack, PartitionedBlockOutputStream::PARTITION_ID_REPLACE, replace_str);
}
}
