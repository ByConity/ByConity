#pragma once
#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/Types.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/InDepthNodeVisitor.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>

namespace DB
{
#define COLUMN_BUCKET_NUMBER "_bucket_number_internal"

void prepareBucketColumn(
    Block & block, const Names bucket_columns, const Int64 split_number, const bool is_with_range, const Int64 total_shard_num, const ContextPtr & context);
void buildBucketScatterSelector(const ColumnRawPtrs & columns, PODArray<size_t> & partition_num_to_first_row, IColumn::Selector & selector, size_t max_parts);
void createColumnWithDtsPartitionHash(Block & block, const Names & bucket_columns, const Int64 & split_number, const ContextPtr & context);
ColumnPtr createColumnWithSipHash(Block & block, const Names & bucket_columns, const Int64 & divisor);
ColumnPtr createBucketNumberColumn(Block & block, const Int64 & split_number, const bool is_with_range, const Int64 total_shard_num);

struct ReplacingConstantExpressionsMatcher
{
    using Data = Block;

    static bool needChildVisit(ASTPtr &, const ASTPtr &);

    static void visit(ASTPtr & node, Block & block_with_constants);
};

struct RewriteInQueryMatcher
{
    struct Data {
        DB::ASTs ast_children_replacement;

        void replaceExpressionListChildren(const ASTFunction * fn);
    };

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);
    static void visit(ASTPtr & node, Data & data);
};
using RewriteInQueryVisitor = InDepthNodeVisitor<RewriteInQueryMatcher, true>;
}
