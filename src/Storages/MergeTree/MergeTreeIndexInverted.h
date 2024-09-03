#pragma once

#include <cstddef>
#include <memory>
#include <Interpreters/GinFilter.h>
#include <Interpreters/ITokenExtractor.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

#include <Common/config.h>
#if USE_TSQUERY
#include "Common/TextSreachQuery.h"
#endif 

#include <Common/ChineseTokenExtractor.h>
#include <roaring.hh>
#include <common/types.h>

namespace DB
{

class MergeTreeIndexGranuleInverted final : public IMergeTreeIndexGranule
{
public:
    explicit MergeTreeIndexGranuleInverted(const String & index_name_, size_t columns_number, const GinFilterParameters & params_);

    ~MergeTreeIndexGranuleInverted() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    bool empty() const override { return !has_elems; }

    String index_name;
    GinFilterParameters params;
    GinFilters gin_filters;
    bool has_elems;
};

using MergeTreeIndexGranuleInvertedPtr = std::shared_ptr<MergeTreeIndexGranuleInverted>;

class MergeTreeIndexAggregatorInverted final : public IMergeTreeIndexAggregator
{
public:
    MergeTreeIndexAggregatorInverted(
        GinIndexStorePtr store_,
        const Names & index_columns_,
        const String & index_name_,
        const GinFilterParameters & params_,
        TokenExtractorPtr token_extractor_,
        ChineseTokenExtractorPtr nlp_extractor_);

    ~MergeTreeIndexAggregatorInverted() override = default;

    bool empty() const override { return !granule || granule->empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

    void addToGinFilter(UInt32 rowID, const char * data, size_t length, GinFilter & gin_filter);

    GinIndexStorePtr store;
    Names index_columns;
    const String index_name;
    const GinFilterParameters params;

    TokenExtractorPtr token_extractor;
    ChineseTokenExtractorPtr nlp_extractor;

    MergeTreeIndexGranuleInvertedPtr granule;
};

class MergeTreeConditionInverted final : public IMergeTreeIndexCondition, WithContext
{
public:
    MergeTreeConditionInverted(
        const SelectQueryInfo & query_info,
        ContextPtr context_,
        const Block & index_sample_block,
        const GinFilterParameters & params_,
        TokenExtractorPtr token_extractor_,
        ChineseTokenExtractorPtr nlp_extractor_);

    ~MergeTreeConditionInverted() override = default;

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule([[maybe_unused]] MergeTreeIndexGranulePtr granule) const override
    {
        /// should call mayBeTrueOnGranuleInPart instead
        assert(false);
        return false;
    }
    bool mayBeTrueOnGranuleInPart(MergeTreeIndexGranulePtr idx_granule, [[maybe_unused]] PostingsCacheForStore & cache_store, [[maybe_unused]]roaring::Roaring & filter_bitmap) const;

private:
    struct KeyTuplePositionMapping
    {
        KeyTuplePositionMapping(size_t tuple_index_, size_t key_index_) : tuple_index(tuple_index_), key_index(key_index_) { }

        size_t tuple_index;
        size_t key_index;
    };
    /// Uses RPN like KeyCondition
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
            #if USE_TSQUERY
            // FOR TEXT SEARCH
            FUNCTION_TEXT_SEARCH,
            #endif
        };

        RPNElement( /// NOLINT
            Function function_ = FUNCTION_UNKNOWN,
            size_t key_column_ = 0,
            std::unique_ptr<GinFilter> && const_gin_filter_ = nullptr)
            : function(function_), key_column(key_column_), gin_filter(std::move(const_gin_filter_))
        {
        }

        Function function = FUNCTION_UNKNOWN;
        /// For FUNCTION_EQUALS, FUNCTION_NOT_EQUALS and FUNCTION_MULTI_SEARCH
        size_t key_column;

        /// For FUNCTION_EQUALS, FUNCTION_NOT_EQUALS
        std::unique_ptr<GinFilter> gin_filter;

        /// For FUNCTION_IN, FUNCTION_NOT_IN and FUNCTION_MULTI_SEARCH
        std::vector<GinFilters> set_gin_filters;
        
        #if USE_TSQUERY
        // For FUNCTION_TEXT_SEARCH
        std::unique_ptr<TextSearchQueryExpression> text_search_filter; 
        #endif 
        
        /// For FUNCTION_IN and FUNCTION_NOT_IN
        std::vector<size_t> set_key_position;
    };

    using RPN = std::vector<RPNElement>;

    bool atomFromAST(const ASTPtr & node, Block & block_with_constants, RPNElement & out);

    bool getKey(const ASTPtr & node, size_t & key_column_num);
    bool tryPrepareSetGinFilter(const ASTs & args, RPNElement & out);


    static bool createFunctionEqualsCondition(
        RPNElement & out, 
        const Field & value, 
        const GinFilterParameters & params, 
        TokenExtractorPtr token_extractor,
        ChineseTokenExtractorPtr nlp_extractor);

    static bool createFunctionTextSearchCondition(
        RPNElement & out, 
        const Field & value, 
        const GinFilterParameters & params, 
        TokenExtractorPtr token_extractor,
        ChineseTokenExtractorPtr nlp_extractor);

    const Block & header;
    GinFilterParameters params;

    TokenExtractorPtr token_extractor;
    ChineseTokenExtractorPtr nlp_extractor;
    
    RPN rpn;

    size_t skip_size;
    /// Sets from syntax analyzer.
    PreparedSets prepared_sets;
};

class MergeTreeIndexInverted final : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexInverted(
        const IndexDescription & index_, const GinFilterParameters & params_, std::unique_ptr<ITokenExtractor> && token_extractor_)
        : IMergeTreeIndex(index_), params(params_), token_extractor(std::move(token_extractor_))
    {
    }

    explicit MergeTreeIndexInverted(
        const IndexDescription & index_, const GinFilterParameters & params_, std::unique_ptr<ChineseTokenExtractor> && nlp_extractor_)
        : IMergeTreeIndex(index_), params(params_), nlp_extractor(std::move(nlp_extractor_))
    {
    }

    ~MergeTreeIndexInverted() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregatorForPart(const GinIndexStorePtr & store) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const SelectQueryInfo & query_info, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override;

    bool isInvertedIndex() const override { return true; }

    GinFilterParameters params;
    /// Function for selecting next token.
    std::unique_ptr<ITokenExtractor> token_extractor;
    // select next token with nlp, now we only have chinese
    std::unique_ptr<ChineseTokenExtractor> nlp_extractor;
};

}
