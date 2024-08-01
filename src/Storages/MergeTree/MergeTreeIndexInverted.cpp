#include <cstddef>
#include <memory>
#include <utility>
#include <string.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/GinFilter.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/BoolMask.h>
#include <Storages/MergeTree/GinIndexStore.h>
#include <Storages/MergeTree/MergeTreeIndexInverted.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Interpreters/ITokenExtractor.h>
#include "common/logger_useful.h"
#include <Common/config.h>

#if USE_TSQUERY
#include <Common/TextSreachQuery.h>
#endif 

#include <Common/ChineseTokenExtractor.h>
#include <common/types.h>
#include <Core/Types.h>
#include <roaring.hh>
#include <Poco/Logger.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

/// Unified condition for equals, startsWith and endsWith
bool MergeTreeConditionInverted::createFunctionEqualsCondition(
    RPNElement & out, const Field & const_value, const GinFilterParameters & params, TokenExtractorPtr token_extractor, ChineseTokenExtractorPtr nlp_extractor)
{
    out.function = RPNElement::FUNCTION_EQUALS;
    out.gin_filter = std::make_unique<GinFilter>(params);
    const auto & value = const_value.get<String>();

    if (token_extractor != nullptr)
    {
        ITokenExtractor::stringToGinFilter(value.data(), value.size(), token_extractor, *out.gin_filter);
    }
    else 
    {
        ChineseTokenExtractor::stringLikeToGinFilter(value, nlp_extractor, *out.gin_filter);
    }

    return true;
}
#if USE_TSQUERY
bool MergeTreeConditionInverted::createFunctionTextSearchCondition(
    RPNElement & out, const Field & const_value, const GinFilterParameters & params, TokenExtractorPtr token_extractor, ChineseTokenExtractorPtr nlp_extractor)
{
    out.function = RPNElement::FUNCTION_TEXT_SEARCH;
    const auto & value = const_value.get<String>();

    TextSearchQuery tsquery(value);

    LOG_TRACE(&Poco::Logger::get(__func__), tsquery.toString());

    out.text_search_filter = tsquery.toTextSearchQueryExpression(params, token_extractor, nlp_extractor);

    return true;
}
#endif

MergeTreeIndexGranuleInverted::MergeTreeIndexGranuleInverted(
    const String & index_name_, size_t columns_number, const GinFilterParameters & params_)
    : index_name(index_name_), params(params_), gin_filters(columns_number, GinFilter(params_)), has_elems(false)
{
}

void MergeTreeIndexGranuleInverted::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty gin index {}.", backQuote(index_name));

    const auto & size_type = std::make_shared<DataTypeUInt32>();
    auto size_serialization = size_type->getDefaultSerialization();

    for (const auto & gin_filter : gin_filters)
    {
        size_t filter_size = gin_filter.getFilter().size();
        size_serialization->serializeBinary(filter_size, ostr);
        ostr.write(
            reinterpret_cast<const char *>(gin_filter.getFilter().data()),
            filter_size * sizeof(GinSegmentWithRowIdRangeVector::value_type));
    }
}

void MergeTreeIndexGranuleInverted::deserializeBinary(ReadBuffer & istr)
{
    Field file_rows;
    const auto & size_type = std::make_shared<DataTypeUInt32>();
    auto size_serialization = size_type->getDefaultSerialization();

    for (auto & gin_filter : gin_filters)
    {
        size_serialization->deserializeBinary(file_rows, istr);
        size_t filter_size = file_rows.get<size_t>();

        if (filter_size == 0)
            continue;

        gin_filter.getFilter().assign(filter_size, {});
        istr.readStrict(
            reinterpret_cast<char *>(gin_filter.getFilter().data()), filter_size * sizeof(GinSegmentWithRowIdRangeVector::value_type));
    }
    has_elems = true;
}

MergeTreeIndexAggregatorInverted::MergeTreeIndexAggregatorInverted(
    GinIndexStorePtr store_,
    const Names & index_columns_,
    const String & index_name_,
    const GinFilterParameters & params_,
    TokenExtractorPtr token_extractor_,
    ChineseTokenExtractorPtr nlp_extractor_)
    : store(store_)
    , index_columns(index_columns_)
    , index_name(index_name_)
    , params(params_)
    , token_extractor(token_extractor_)
    , nlp_extractor(nlp_extractor_)
    , granule(std::make_shared<MergeTreeIndexGranuleInverted>(index_name, index_columns.size(), params))
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorInverted::getGranuleAndReset()
{
    auto new_granule = std::make_shared<MergeTreeIndexGranuleInverted>(index_name, index_columns.size(), params);

    new_granule.swap(granule);
    return new_granule;
}

void MergeTreeIndexAggregatorInverted::addToGinFilter(UInt32 rowID, const char * data, size_t length, GinFilter & gin_filter)
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    if (token_extractor != nullptr)
    {
        while (cur < length && token_extractor->nextInStringPadded(data, length, &cur, &token_start, &token_len))
        {
            gin_filter.add(data + token_start, token_len, rowID, store);
        }
    }
    else 
    {   
        String value(data, length); 
        ChineseTokenExtractor::WordRangesWithIterator iterator;

        nlp_extractor->preCutString(value, iterator);

        while (ChineseTokenExtractor::nextInCutString(token_start, token_len, iterator))
        {
            gin_filter.add(data + token_start, token_len, rowID, store);
        }
    }

}

void MergeTreeIndexAggregatorInverted::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. "
            "Position: {}, Block rows: {}.",
            *pos,
            block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);
    auto row_id = store->getNextRowIDRange(rows_read);
    auto start_row_id = row_id;

    for (size_t col = 0; col < index_columns.size(); ++col)
    {
        const auto & column_with_type = block.getByName(index_columns[col]);
        const auto & column = column_with_type.column;
        size_t current_position = *pos;

        bool need_to_write = false;

        for (size_t i = 0; i < rows_read; ++i)
        {
            auto ref = column->getDataAt(current_position + i);
            addToGinFilter(row_id, ref.data, ref.size, granule->gin_filters[col]);
            store->incrementCurrentSizeBy(ref.size);
            row_id++;
            if (store->needToWrite())
                need_to_write = true;
        }

        granule->gin_filters[col].addRowRangeToGinFilter(
            store->getCurrentSegmentID(), start_row_id, static_cast<UInt32>(start_row_id + rows_read - 1));

        store->setStoreDensity(params.density);
        
        if (need_to_write)
        {
            store->writeSegment();
        }
    }

    granule->has_elems = true;
    *pos += rows_read;
}

MergeTreeConditionInverted::MergeTreeConditionInverted(
    const SelectQueryInfo & query_info,
    ContextPtr context_,
    const Block & index_sample_block,
    const GinFilterParameters & params_,
    TokenExtractorPtr token_extractor_,
    ChineseTokenExtractorPtr nlp_extractor_)
    : header(index_sample_block)
    , params(params_)
    , token_extractor(token_extractor_)
    , nlp_extractor(nlp_extractor_)
    , prepared_sets(query_info.sets)

{
    skip_size = context_->getSettingsRef().skip_inverted_index_term_size;
    rpn = std::move(RPNBuilder<RPNElement>(
                        query_info,
                        context_,
                        [this](const ASTPtr & node, ContextPtr /* context */, Block & block_with_constants, RPNElement & out) -> bool {
                            return this->atomFromAST(node, block_with_constants, out);
                        })
                        .extractRPN());
}

/// Keep in-sync with MergeTreeConditionFullText::alwaysUnknownOrTrue
bool MergeTreeConditionInverted::alwaysUnknownOrTrue() const
{
    /// Check like in KeyCondition.
    std::vector<bool> rpn_stack;

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN || element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.push_back(true);
        }
        else if (
            element.function == RPNElement::FUNCTION_EQUALS || element.function == RPNElement::FUNCTION_NOT_EQUALS
            || element.function == RPNElement::FUNCTION_IN || element.function == RPNElement::FUNCTION_NOT_IN
            || element.function == RPNElement::FUNCTION_MULTI_SEARCH || element.function == RPNElement::ALWAYS_FALSE
            #if USE_TSQUERY
            || element.function == RPNElement::FUNCTION_TEXT_SEARCH 
            #endif
        )
        {
            rpn_stack.push_back(false);
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            // do nothing
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            bool arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            bool arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 && arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            bool arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            bool arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 || arg2;
        }
        else
            throw Exception("Unexpected function type in KeyCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    return rpn_stack[0];
}

// TODO @caichangheng
// filter_bitmap without text search query may case wrong with not
// will ignore filter_bitmap select without text search;
bool MergeTreeConditionInverted::mayBeTrueOnGranuleInPart(
    MergeTreeIndexGranulePtr idx_granule, [[maybe_unused]] PostingsCacheForStore & cache_store, [[maybe_unused]] roaring::Roaring & filter_bitmap) const
{
    std::shared_ptr<MergeTreeIndexGranuleInverted> granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleInverted>(idx_granule);
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "GinFilter index condition got a granule with the wrong type.");
    
    // filter without text search do not return filter_bitmap
    roaring::Roaring empty_bitmap;

    /// Check like in KeyCondition.
    std::vector<BoolMask> rpn_stack;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }
        else if (element.function == RPNElement::FUNCTION_EQUALS || element.function == RPNElement::FUNCTION_NOT_EQUALS)
        {
            rpn_stack.emplace_back(granule->gin_filters[element.key_column].contains(*element.gin_filter, cache_store, empty_bitmap), true);

            if (element.function == RPNElement::FUNCTION_NOT_EQUALS)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_IN || element.function == RPNElement::FUNCTION_NOT_IN)
        {
            std::vector<bool> result(element.set_gin_filters.back().size(), true);

            if (skip_size < element.set_gin_filters[0].size())
            {
                rpn_stack.emplace_back(true, true);
            }
            else
            {
                for (size_t column = 0; column < element.set_key_position.size(); ++column)
                {
                    const size_t key_idx = element.set_key_position[column];

                    const auto & gin_filters = element.set_gin_filters[column];
                    for (size_t row = 0; row < gin_filters.size(); ++row)
                        result[row] = result[row] && granule->gin_filters[key_idx].contains(gin_filters[row], cache_store, empty_bitmap);
                }

                rpn_stack.emplace_back(std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
                if (element.function == RPNElement::FUNCTION_NOT_IN)
                    rpn_stack.back() = !rpn_stack.back();
            }
        }
        else if (element.function == RPNElement::FUNCTION_MULTI_SEARCH)
        {
            std::vector<bool> result(element.set_gin_filters.back().size(), true);

            const auto & gin_filters = element.set_gin_filters[0];

            if (skip_size < gin_filters.size())
            {
                rpn_stack.emplace_back(true, true);
            }
            else
            {
                for (size_t row = 0; row < gin_filters.size(); ++row)
                    result[row] = result[row] && granule->gin_filters[element.key_column].contains(gin_filters[row], cache_store, empty_bitmap);

                rpn_stack.emplace_back(std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
            }
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.emplace_back(false, true);
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.emplace_back(true, false);
        }
        #if USE_TSQUERY
        else if (element.function == RPNElement::FUNCTION_TEXT_SEARCH)
        {
            rpn_stack.emplace_back(
                TextSearchQueryExpression::calculate(
                    element.text_search_filter, 
                    granule->gin_filters[element.key_column], 
                    cache_store, 
                    filter_bitmap),
                    true);
        }
        #endif
        else
            throw Exception("Unexpected function type in GinFilterCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    if (rpn_stack.size() != 1)
        throw Exception("Unexpected stack size in GinFilterCondition::mayBeTrueOnGranule", ErrorCodes::LOGICAL_ERROR);

    return rpn_stack[0].can_be_true;
}

bool MergeTreeConditionInverted::getKey(const ASTPtr & node, size_t & key_column_num)
{
    Names index_columns = header.getNames();
    auto it = std::find(index_columns.begin(), index_columns.end(), node->getColumnName());
    if (it == index_columns.end())
        return false;

    key_column_num = static_cast<size_t>(it - index_columns.begin());
    return true;
}

bool MergeTreeConditionInverted::atomFromAST(const ASTPtr & node, Block & block_with_constants, RPNElement & out)
{
    Field const_value;
    DataTypePtr const_type;
    if (const auto * func = typeid_cast<const ASTFunction *>(node.get()))
    {
        const ASTs & args = typeid_cast<const ASTExpressionList &>(*func->arguments).children;

        if (args.size() != 2)
            return false;

        size_t key_arg_pos; /// Position of argument with key column (non-const argument)
        size_t key_column_num = -1; /// Number of a key column (inside key_column_names array)
        const auto & func_name = func->name;

        if (functionIsInOrGlobalInOperator(func_name) && tryPrepareSetGinFilter(args, out))
        {
            key_arg_pos = 0;
        }
        else if (KeyCondition::getConstant(args[1], block_with_constants, const_value, const_type) && getKey(args[0], key_column_num))
        {
            key_arg_pos = 0;
        }
        else if (KeyCondition::getConstant(args[0], block_with_constants, const_value, const_type) && getKey(args[1], key_column_num))
        {
            key_arg_pos = 1;
        }
        else
            return false;
        
        #if USE_TSQUERY
        /// For textSearch 
        if (func_name == "textSearch")
        {
            out.key_column = key_column_num;
            return createFunctionTextSearchCondition(out, const_value, params, token_extractor, nlp_extractor);
        }
        #endif

        if (const_type && const_type->getTypeId() != TypeIndex::String && const_type->getTypeId() != TypeIndex::FixedString
            && const_type->getTypeId() != TypeIndex::Array)
        {
            return false;
        }

        if (key_arg_pos == 1 && (func_name != "equals" && func_name != "notEquals"))
            return false;

        if (func_name == "notEquals")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_NOT_EQUALS;
            out.gin_filter = std::make_unique<GinFilter>(params);
            auto & value = const_value.get<String>();
            if (token_extractor != nullptr)
            {
                ITokenExtractor::stringToGinFilter(value.data(), value.size(), token_extractor, *out.gin_filter);
            }
            else 
            {
                ChineseTokenExtractor::stringToGinFilter(value, nlp_extractor, *out.gin_filter);
            }
            return true;
        }
        else if (func_name == "equals")
        {
            out.key_column = key_column_num;
            return createFunctionEqualsCondition(out, const_value, params, token_extractor, nlp_extractor);
        }
        else if (func_name == "like")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_EQUALS;
            out.gin_filter = std::make_unique<GinFilter>(params);
            auto & value = const_value.get<String>();
            if (token_extractor != nullptr)
            {
                ITokenExtractor::stringLikeToGinFilter(value.data(), value.size(), token_extractor, *out.gin_filter);
            }
            else 
            {
                ChineseTokenExtractor::stringLikeToGinFilter(value, nlp_extractor, *out.gin_filter);
            }
            return true;
        }
        else if (func_name == "notLike")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_NOT_EQUALS;
            out.gin_filter = std::make_unique<GinFilter>(params);
            auto & value = const_value.get<String>();

            if (token_extractor != nullptr)
            {
                ITokenExtractor::stringLikeToGinFilter(value.data(), value.size(), token_extractor, *out.gin_filter);
            }
            else 
            {
                ChineseTokenExtractor::stringLikeToGinFilter(value, nlp_extractor, *out.gin_filter);
            }
            return true;
        }
        else if (func_name == "hasToken" || func_name == "hasTokens")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_EQUALS;
            out.gin_filter = std::make_unique<GinFilter>(params);
            auto & value = const_value.get<String>();
            if (token_extractor != nullptr)
            {
                ITokenExtractor::stringToGinFilter(value.data(), value.size(), token_extractor, *out.gin_filter);
            }
            else
            {
                ChineseTokenExtractor::stringToGinFilter(value, nlp_extractor, *out.gin_filter);
            }
    
            LOG_TRACE(&Poco::Logger::get("inverted index"),"search string: {} with token : [ {} ] ", value, out.gin_filter->getTermsInString());
    
            return true;
             
        }
        else if (func_name == "startsWith")
        {
            out.key_column = key_column_num;
            return createFunctionEqualsCondition(out, const_value, params, token_extractor, nlp_extractor);
        }
        else if (func_name == "endsWith")
        {
            out.key_column = key_column_num;
            return createFunctionEqualsCondition(out, const_value, params, token_extractor, nlp_extractor);
        }
        else if (func_name == "multiSearchAny")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_MULTI_SEARCH;

            /// 2d vector is not needed here but is used because already exists for FUNCTION_IN
            std::vector<std::vector<GinFilter>> gin_filters;
            gin_filters.emplace_back();
            for (const auto & element : const_value.get<Array>())
            {
                if (element.getType() != Field::Types::String)
                    return false;

                gin_filters.back().emplace_back(params);
                const auto & value = element.get<String>();
                if (token_extractor != nullptr)
                {
                    ITokenExtractor::stringToGinFilter(value.data(), value.size(), token_extractor, gin_filters.back().back());
                }
                else 
                {
                    ChineseTokenExtractor::stringToGinFilter(value, nlp_extractor, gin_filters.back().back());
                }
            }
            out.set_gin_filters = std::move(gin_filters);
            return true;
        }
        else if (func_name == "notIn")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_NOT_IN;
            return true;
        }
        else if (func_name == "in")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_IN;
            return true;
        }

        return false;
    }
    else if (KeyCondition::getConstant(node, block_with_constants, const_value, const_type))
    {
        /// Check constant like in KeyCondition
        if (const_value.getType() == Field::Types::UInt64 || const_value.getType() == Field::Types::Int64
            || const_value.getType() == Field::Types::Float64)
        {
            /// Zero in all types is represented in memory the same way as in UInt64.
            out.function = const_value.get<UInt64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;

            return true;
        }
    }

    return false;
}

bool MergeTreeConditionInverted::tryPrepareSetGinFilter(const ASTs & args, RPNElement & out)
{
    const ASTPtr & left_arg = args[0];
    const ASTPtr & right_arg = args[1];

    std::vector<KeyTuplePositionMapping> key_tuple_mapping;
    DataTypes data_types;

    const auto * left_arg_tuple = typeid_cast<const ASTFunction *>(left_arg.get());
    if (left_arg_tuple && left_arg_tuple->name == "tuple")
    {
        const auto & tuple_elements = left_arg_tuple->arguments->children;
        for (size_t i = 0; i < tuple_elements.size(); ++i)
        {
            size_t key = 0;
            if (getKey(tuple_elements[i], key))
            {
                key_tuple_mapping.emplace_back(i, key);
                data_types.push_back(header.getDataTypes()[key]);
            }
        }
    }
    else
    {
        size_t key = 0;
        if (getKey(left_arg, key))
        {
            key_tuple_mapping.emplace_back(0, key);
            data_types.push_back(header.getDataTypes()[key]);
        }
    }

    if (key_tuple_mapping.empty())
        return false;

    PreparedSetKey set_key;
    if (typeid_cast<const ASTSubquery *>(right_arg.get()) || typeid_cast<const ASTIdentifier *>(right_arg.get()))
        set_key = PreparedSetKey::forSubquery(*right_arg);
    else
        set_key = PreparedSetKey::forLiteral(*right_arg, data_types);

    auto set_it = prepared_sets.find(set_key);
    if (set_it == prepared_sets.end())
        return false;

    const SetPtr & prepared_set = set_it->second;
    if (!prepared_set->hasExplicitSetElements())
        return false;

    for (const auto & data_type : prepared_set->getDataTypes())
        if (data_type->getTypeId() != TypeIndex::String && data_type->getTypeId() != TypeIndex::FixedString)
            return false;

    std::vector<std::vector<GinFilter>> gin_filters;
    std::vector<size_t> key_position;

    Columns columns = prepared_set->getSetElements();
    for (const auto & elem : key_tuple_mapping)
    {
        gin_filters.emplace_back();
        key_position.push_back(elem.key_index);

        size_t tuple_idx = elem.tuple_index;
        const auto & column = columns[tuple_idx];
        for (size_t row = 0; row < prepared_set->getTotalRowCount(); ++row)
        {
            gin_filters.back().emplace_back(params);
            auto ref = column->getDataAt(row);
            if (token_extractor != nullptr)
            {
                ITokenExtractor::stringPaddedToGinFilter(ref.data, ref.size, token_extractor, gin_filters.back().back());
            }
            else
            {
                ChineseTokenExtractor::stringPaddedToGinFilter(ref.toString(), nlp_extractor, gin_filters.back().back());
            }

        }
    }

    out.set_key_position = std::move(key_position);
    out.set_gin_filters = std::move(gin_filters);

    return true;
}


MergeTreeIndexGranulePtr MergeTreeIndexInverted::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleInverted>(index.name, index.column_names.size(), params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexInverted::createIndexAggregator() const
{
    /// should not be called: createIndexAggregatorForPart should be used
    assert(false);
    return nullptr;
}

MergeTreeIndexAggregatorPtr MergeTreeIndexInverted::createIndexAggregatorForPart(const GinIndexStorePtr & store) const
{
    return std::make_shared<MergeTreeIndexAggregatorInverted>(store, index.column_names, index.name, params, token_extractor.get(), nlp_extractor.get());
}

MergeTreeIndexConditionPtr MergeTreeIndexInverted::createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeConditionInverted>(query, context, index.sample_block, params, token_extractor.get(), nlp_extractor.get());
};

bool MergeTreeIndexInverted::mayBenefitFromIndexForIn(const ASTPtr & node) const
{
    return std::find(std::cbegin(index.column_names), std::cend(index.column_names), node->getColumnName())
        != std::cend(index.column_names);
}

// just a tmp function to fit ce branch, will pick soon
bool parseWithNewInvertedIndexArguments(const FieldVector& arguments_)
{
    return arguments_.size() == 2 && arguments_[0].getType() == Field::Types::String && arguments_[1].getType() == Field::Types::String;
}

void checkWithNewInvertedIndexArguments(const FieldVector & arguments_)
{
    String config_type = arguments_[0].get<String>();
    String config_value = arguments_[1].get<String>();

    if (config_type != StandardTokenExtractor::getName() || config_value != "{}")
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown config type {} and value should only {} now", config_type);
    }
}

MergeTreeIndexPtr ginIndexCreator(const IndexDescription & index)
{   
    // just a tmp code to fit ce branch, will pick soon
    if (parseWithNewInvertedIndexArguments(index.arguments))
    {
        GinFilterParameters params(0, 1.0);
        auto tokenizer = std::make_unique<StandardTokenExtractor>();
        return std::make_shared<MergeTreeIndexInverted>(index, params, std::move(tokenizer));
    }

    if (index.arguments.size() > 2)
    {
        // String type_name = index.arguments[0].get<String>(); use for select nlp
        String config_name = index.arguments[1].get<String>();
        Float64 density = index.arguments[2].get<Float64>();

        GinFilterParameters params(0, density);

        auto nlp_extractor = std::make_unique<ChineseTokenExtractor>(config_name);
        return std::make_shared<MergeTreeIndexInverted>(index, params, std::move(nlp_extractor));
    }
    else
    {
        size_t n = index.arguments.empty() ? 0 : index.arguments[0].get<size_t>();
        Float64 density = index.arguments.size() < 2 ? 1.0 : index.arguments[1].get<Float64>();
        GinFilterParameters params(n, density);

        /// Use SplitTokenExtractor when n is 0, otherwise use NgramTokenExtractor
        if (n > 0)
        {
            auto tokenizer = std::make_unique<NgramTokenExtractor>(n);
            return std::make_shared<MergeTreeIndexInverted>(index, params, std::move(tokenizer));
        }
        else
        {
            auto tokenizer = std::make_unique<SplitTokenExtractor>();
            return std::make_shared<MergeTreeIndexInverted>(index, params, std::move(tokenizer));
        }
    }
}

void ginIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    for (const auto & index_data_type : index.data_types)
    {
        WhichDataType data_type(index_data_type);

        if (!data_type.isString() && !data_type.isFixedString())
            throw Exception("Inverted index can be used only with `String`, `FixedString`", ErrorCodes::INCORRECT_QUERY);
    }

    if (parseWithNewInvertedIndexArguments(index.arguments))
    {
        checkWithNewInvertedIndexArguments(index.arguments);
        return;
    }

    if (index.arguments.size() > 2) /// NLP tokenizer [type_name , config_name , density]
    {
        if (index.arguments.size() > 3)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Inverted index must have less than three arguments.");

        if (index.arguments[0].getType() != Field::Types::String || index.arguments[1].getType() != Field::Types::String )  // we only have chinese tokenizer now
        {
            throw Exception("The first/second Inverted index argument with NLP tokenizer must be positive string.", ErrorCodes::INCORRECT_QUERY);
        }

        // now we only have token_chinese_default for nlp tokenizer, todo refactor here
        if (index.arguments[0].get<String>() != ChineseTokenExtractor::getName())
        {
            throw Exception(ErrorCodes::INCORRECT_QUERY, "only support type {} now ", ChineseTokenExtractor::getName());  
        }


        if(index.arguments[2].getType() != Field::Types::Float64 || index.arguments[2].get<Float64>() <= 0 || index.arguments[2].get<Float64>() > 1)
        {
            throw Exception("The third Inverted index argument must be a float between 0 and 1.", ErrorCodes::INCORRECT_QUERY);
        }
    }
    else /// ngram/token tokenizer
    {
        if (!index.arguments.empty() && index.arguments[0].getType() != Field::Types::UInt64)
            throw Exception("The first Inverted index argument must be positive integer.", ErrorCodes::INCORRECT_QUERY);

        if (index.arguments.size() == 2
            && (index.arguments[1].getType() != Field::Types::Float64 || index.arguments[1].get<Float64>() <= 0
                || index.arguments[1].get<Float64>() > 1))
            throw Exception("The second Inverted index argument must be a float between 0 and 1.", ErrorCodes::INCORRECT_QUERY);


        /// Just validate
        size_t ngrams = index.arguments.empty() ? 0 : index.arguments[0].get<size_t>();
        Float64 density = index.arguments.size() < 2 ? 1.0 : index.arguments[1].get<Float64>();
        GinFilterParameters params(ngrams, density);
    }
}


}
