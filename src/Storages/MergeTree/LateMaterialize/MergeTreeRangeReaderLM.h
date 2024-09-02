#pragma once
#include <unordered_map>
#include <Core/Block.h>
#include <common/logger_useful.h>
#include <Storages/MergeTree/Index/BitmapIndexHelper.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/LateMaterialize/Stream.h>
#include <Storages/MergeTree/LateMaterialize/ReadResult.h>
#include <Storages/MergeTree/MarkRange.h>

namespace DB
{

template <typename T, bool has_buf>
class ColumnVector;
using ColumnUInt8 = ColumnVector<UInt8>;

class IMergeTreeReader;
class MergeTreeIndexGranularity;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

using ReadResult = LateMaterialize::ReadResult;
using GranuledStream = LateMaterialize::GranuledStream;
// Same as AtomicPredicate but with expression actions
struct AtomicPredicateExpr
{
    ExpressionActionsPtr predicate_actions;
    String filter_column_name;
    bool is_row_filter = false;
    MergeTreeIndexContextPtr index_context;
    bool remove_filter_column = false;
};
using AtomicPredicateExprPtr = std::shared_ptr<AtomicPredicateExpr>;

/// MergeTreeReader iterator which allows sequential reading for arbitrary number of rows between pairs of marks in the same part.
/// Stores reading state, which can be inside granule. Can skip rows in current granule and start reading from next mark.
/// Used generally for reading number of rows less than index granularity to decrease cache misses for fat blocks.
class MergeTreeRangeReaderLM
{
public:
    MergeTreeRangeReaderLM(
        IMergeTreeReader * merge_tree_reader_,
        MergeTreeRangeReaderLM * prev_reader_,
        const AtomicPredicateExpr * atomic_predicate_,
        ImmutableDeleteBitmapPtr delete_bitmap_,
        bool last_reader_in_chain_);

    MergeTreeRangeReaderLM() = default;

    bool isReadingFinished() const;

    bool isCurrentRangeFinished() const;


    ReadResult read(MarkRanges & ranges, bool need_filter = false);

    const Block & getSampleBlock() const { return sample_block; }

    void makeLastReader() { last_reader_in_chain = true; }

private:
    void readImpl(ReadResult & read_result, MarkRanges & ranges);
    void startReadingChain(ReadResult & read_result, MarkRanges & ranges);
    Columns continueReadingChain(ReadResult & read_result);
    void skip();

    int evaluatePredicateAndCombine(ReadResult & result);

    void extractBitmapIndexColumns(Columns & columns, Block & bitmap_block);

    IMergeTreeReader * merge_tree_reader = nullptr;
    const MergeTreeIndexGranularity * index_granularity = nullptr;
    MergeTreeRangeReaderLM * prev_reader = nullptr; /// If not nullptr, read from prev_reader firstly.
    const AtomicPredicateExpr * atomic_predicate;
    ImmutableDeleteBitmapPtr delete_bitmap = nullptr; /// If not nullptr, rows in delete bitmap are removed

    GranuledStream stream;

    Block sample_block;

    bool last_reader_in_chain = false;
    bool has_bitmap_index = false;
#ifndef NDEBUG
public:
    std::unordered_map<String, size_t> per_column_read_granules = {};
#endif
};

}
