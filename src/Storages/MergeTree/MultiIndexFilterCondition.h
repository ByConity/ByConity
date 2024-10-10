#pragma once

#include <Common/ChineseTokenExtractor.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/GinFilter.h>
#include <Interpreters/ITokenExtractor.h>
#include <Interpreters/PreparedSets.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndexInverted.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>

namespace DB
{

struct IndexTimeWatcher;

/// A template to generate condition executor, which allow using multiple
/// index to filter mark ranges at the same time. Currently it only
/// support inverted index
/// All immutable information are stored in MultiIndexFilterCondition, and
/// mutable information are stored in Executor, so they can avoid part RPN
/// multiple times when delay mark range filter till pipeline execution
/// NOTE: Current tuple(col0, col1) in [(v0, v1), (v2, v2)] didn't support utilize
/// multiple index by now
class MultiIndexFilterCondition
{
private:
    /// Represent matching condition for single column
    struct ColumnMatchCondition
    {
        ColumnMatchCondition(size_t key_pos_, GinFilters&& filters_):
            key_position(key_pos_), key_gin_filters(std::move(filters_)) {}

        size_t key_position;
        GinFilters key_gin_filters;
    };

    /// Wrapper around MergeTreeIndexPtr
    struct CandidateIndex
    {
        explicit CandidateIndex(const MergeTreeIndexPtr& idx_);

        std::optional<ColumnMatchCondition> tryMatchIndexOnColumn(const ASTPtr& key_col_,
            const DataTypePtr& const_type_, const Field& const_value_, bool match_to_like_) const;
        std::vector<ColumnMatchCondition> tryMatchIndexOnColumns(const ASTs& key_cols_,
            const ASTPtr& set_col_, const PreparedSets& prepared_sets_) const;
        std::optional<GinFilter> constValueToGinFilter(const Field& const_value_,
            bool match_to_like_) const;
        GinFilter stringValueToGinFilter(const StringRef& str_value_, bool match_to_like_) const;
        std::optional<size_t> columnIndexInHeader(const String& column_name_) const;

        const MergeTreeIndexPtr idx;
        const IndexDescription& idx_desc;
        const MergeTreeIndexInverted* ivt_idx;
    };

    /// Index reader and gin index store for single inverted index
    struct IndexReaderAndStore
    {
        IndexReaderAndStore(const MergeTreeIndexPtr& index_,
            const MergeTreeDataPartPtr& part_, const MergeTreeReaderSettings& reader_settings_,
            const ContextPtr& context_);

        void resetForNewRanges();
        void loadGranuleForRanges(const MarkRanges& sorted_mark_ranges_, IndexTimeWatcher& idx_timer_);

        const size_t index_granularity;

        std::unique_ptr<MergeTreeIndexReader> idx_reader;
        std::unique_ptr<PostingsCacheForStore> cache_and_store;

        MergeTreeIndexGranuleInvertedPtr cached_granule;
    };

    using IndexReaderAndStores = std::unordered_map<String,
        std::unique_ptr<IndexReaderAndStore>>;

    struct KeyTuplePositionMapping
    {
        KeyTuplePositionMapping(size_t tuple_index_, size_t key_index_) : tuple_index(tuple_index_), key_index(key_index_) { }

        size_t tuple_index;
        size_t key_index;
    };

    struct MatchedIndex
    {
        MatchedIndex(const CandidateIndex* idx_, std::vector<ColumnMatchCondition>&& conds_);

        std::unique_ptr<roaring::Roaring> match(const MarkRanges& sorted_mark_ranges_,
            IndexReaderAndStores& readers_and_stores_, IndexTimeWatcher& idx_timer_) const;

        const CandidateIndex* idx;
        /// Match condition on single/multiple columns, these conditions will
        /// combined with AND operator
        std::vector<ColumnMatchCondition> match_conditions;
    };

    struct RPNElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_EQUALS,
            FUNCTION_NOT_EQUALS,
            FUNCTION_IN,
            FUNCTION_NOT_IN,
            FUNCTION_MULTI_SEARCH,
            FUNCTION_UNKNOWN, /// Can take any value.
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        explicit RPNElement(Function func_ = FUNCTION_UNKNOWN): function(func_) {}

        Function function = FUNCTION_UNKNOWN;

        /// All the inverted indices which can be used to speedup this function.
        /// One thing to note is that single `in` with multiple columns is split
        /// into multiple MatchedIndex
        std::vector<MatchedIndex> matched_indices;
    };

    bool atomFromAST(const ASTPtr& node_, Block& block_with_constants_,
        RPNElement& out_) const;
    bool hasTokenBySeperatorAtomFromAST(const ASTs& args_, Block& block_with_constants_,
        RPNElement& out_) const;
    bool inAtomFromAST(const String& func_name_, const ASTs& args_, RPNElement& out_) const;
    bool multiSearchAnyAtomFromAST(const ASTs& args_, Block& block_with_constants_,
        RPNElement& out_) const;

    const size_t search_filter_size_limit;
    const ContextPtr context;
    const MergeTreeReaderSettings reader_settings;

    std::vector<CandidateIndex> indices;
    PreparedSets prepared_sets;

    std::vector<RPNElement> rpn;
public:
    class Executor
    {
    public:
        Executor(const MultiIndexFilterCondition& condition_,
            const MergeTreeDataPartPtr& part_);

        std::unique_ptr<roaring::Roaring> match(const MarkRanges& mark_ranges_,
            IndexTimeWatcher& idx_timer_);

    private:
        std::unique_ptr<roaring::Roaring> matchingRows(const MarkRanges& sorted_mark_ranges_,
            const MergeTreeIndexGranularity& part_granularity_,
            IndexTimeWatcher& idx_timer_);

        const MultiIndexFilterCondition& condition;
        const MergeTreeDataPartPtr part;

        IndexReaderAndStores readers_and_stores;        
    };

    MultiIndexFilterCondition(const std::vector<MergeTreeIndexPtr>& indices_,
        const ContextPtr& context_, const SelectQueryInfo& query_info_,
        const MergeTreeReaderSettings& reader_settings_);

    /// Return useful indices in condition evaluations
    std::vector<String> usefulIndices() const;

    std::unique_ptr<Executor> executor(const MergeTreeDataPartPtr& part_) const;
};

}
