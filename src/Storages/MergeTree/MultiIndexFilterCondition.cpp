#include <Storages/MergeTree/MultiIndexFilterCondition.h>
#include <Parsers/ASTSubquery.h>
#include <Interpreters/misc.h>
#include <Storages/MergeTree/FilterWithRowUtils.h>
#include <Storages/MergeTree/MergeTreeIndexInverted.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/MergeTree/GinIndexDataPartHelper.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Storages/DiskCache/DiskCacheFactory.h>

namespace DB
{

MultiIndexFilterCondition::CandidateIndex::CandidateIndex(
    const MergeTreeIndexPtr& idx_): idx(idx_), idx_desc(idx->index),
        ivt_idx(typeid_cast<const MergeTreeIndexInverted*>(idx_.get()))
{
    if (ivt_idx == nullptr)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MultiIndexFilterCondition only "
            "support inverted index by now, got index {} type {}",
            idx_desc.name, idx_desc.type);
    }
}

std::optional<MultiIndexFilterCondition::ColumnMatchCondition>
MultiIndexFilterCondition::CandidateIndex::tryMatchIndexOnColumn(
    const ASTPtr& key_col_, const DataTypePtr& const_type_, const Field& const_value_,
    bool match_to_like_) const
{
    std::optional<size_t> column_idx = columnIndexInHeader(key_col_->getColumnName());
    if (!column_idx.has_value())
    {
        return std::nullopt;
    }

    std::vector<GinFilter> gin_filters;
    if (const_type_->getTypeId() == TypeIndex::String
        || const_type_->getTypeId() == TypeIndex::FixedString)
    {
        std::optional<GinFilter> gin_filter = constValueToGinFilter(const_value_,
            match_to_like_);
        if (gin_filter.has_value())
        {
            gin_filters.emplace_back(std::move(gin_filter.value()));
        }
    }
    else if (const_type_->getTypeId() == TypeIndex::Array)
    {
        for (const auto& element : const_value_.get<Array>())
        {
            std::optional<GinFilter> gin_filter = constValueToGinFilter(element,
                match_to_like_);
            if (gin_filter.has_value())
            {
                gin_filters.emplace_back(std::move(gin_filter.value()));
            }
            else
            {
                gin_filters.clear();
                break;
            }
        }
    }

    if (gin_filters.empty())
    {
        return std::nullopt;
    }
    return ColumnMatchCondition(column_idx.value(), std::move(gin_filters));
}

std::vector<MultiIndexFilterCondition::ColumnMatchCondition>
MultiIndexFilterCondition::CandidateIndex::tryMatchIndexOnColumns(
    const ASTs& key_cols_, const ASTPtr& set_col_,
    const PreparedSets& prepared_sets_) const
{
    std::vector<KeyTuplePositionMapping> key_tuple_mapping;
    DataTypes data_types;
    for (size_t i = 0; i < key_cols_.size(); ++i)
    {
        std::optional<size_t> key_idx = columnIndexInHeader(key_cols_[i]->getColumnName());
        if (key_idx.has_value())
        {
            key_tuple_mapping.emplace_back(i, key_idx.value());
            data_types.emplace_back(idx_desc.sample_block.getByPosition(key_idx.value()).type);
        }
    }
    if (key_tuple_mapping.empty())
    {
        return {};
    }

    PreparedSetKey set_key =
        (typeid_cast<const ASTSubquery*>(set_col_.get()) || typeid_cast<const ASTIdentifier*>(set_col_.get())) ?
            PreparedSetKey::forSubquery(*set_col_)
            : PreparedSetKey::forLiteral(*set_col_, data_types);
    auto set_iter = prepared_sets_.find(set_key);
    if (set_iter == prepared_sets_.end())
    {
        return {};
    }
    const SetPtr& prepared_set = set_iter->second;
    if (!prepared_set->hasExplicitSetElements())
    {
        return {};
    }
    for (const auto& data_type : prepared_set->getDataTypes())
    {
        if (data_type->getTypeId() != TypeIndex::String && data_type->getTypeId() != TypeIndex::FixedString)
        {
            return {};
        }
    }

    std::vector<ColumnMatchCondition> matched_conditions;
    Columns columns = prepared_set->getSetElements();
    for (const auto& elem : key_tuple_mapping)
    {
        GinFilters gin_filters;

        size_t tuple_idx = elem.tuple_index;
        const auto& column = columns[tuple_idx];
        for (size_t row = 0; row < prepared_set->getTotalRowCount(); ++row)
        {
            gin_filters.emplace_back(stringValueToGinFilter(column->getDataAt(row), false));
        }

        matched_conditions.emplace_back(elem.key_index, std::move(gin_filters));
    }
    return matched_conditions;
}

std::optional<GinFilter> MultiIndexFilterCondition::CandidateIndex::constValueToGinFilter(
    const Field& const_value_, bool match_to_like_) const
{
    if (const_value_.getType() != Field::Types::String)
    {
        return std::nullopt;
    }

    const auto& value = const_value_.get<String>();
    return stringValueToGinFilter(value, match_to_like_);
}

GinFilter MultiIndexFilterCondition::CandidateIndex::stringValueToGinFilter(
    const StringRef& str_value_, bool match_to_like_) const
{
    GinFilter filter(ivt_idx->params);
    if (match_to_like_)
    {
        if (ivt_idx->token_extractor != nullptr)
            ITokenExtractor::stringLikeToGinFilter(str_value_.data, str_value_.size,
                ivt_idx->token_extractor.get(), filter);
        else
            ChineseTokenExtractor::stringLikeToGinFilter(str_value_.toString(),
                ivt_idx->nlp_extractor.get(), filter);
    }
    else
    {
        if (ivt_idx->token_extractor != nullptr)
            ITokenExtractor::stringToGinFilter(str_value_.data, str_value_.size,
                ivt_idx->token_extractor.get(), filter);
        else
            ChineseTokenExtractor::stringToGinFilter(str_value_.toString(),
                ivt_idx->nlp_extractor.get(), filter);
    }
    return filter;
}

std::optional<size_t> MultiIndexFilterCondition::CandidateIndex::columnIndexInHeader(
    const String& column_name_) const
{
    const ColumnsWithTypeAndName& columns_with_type_and_names =
        idx_desc.sample_block.getColumnsWithTypeAndName();
    auto iter = std::find_if(columns_with_type_and_names.begin(), columns_with_type_and_names.end(),
        [&column_name_](const ColumnWithTypeAndName& column_with_type_and_type) {
            return column_with_type_and_type.name == column_name_;
        }
    );
    return iter == columns_with_type_and_names.end() ? std::nullopt :
        std::optional<size_t>(iter - columns_with_type_and_names.begin());
}

MultiIndexFilterCondition::IndexReaderAndStore::IndexReaderAndStore(
    const MergeTreeIndexPtr& index_, const MergeTreeDataPartPtr& part_,
    const MergeTreeReaderSettings& reader_settings_, const ContextPtr& context_):
        index_granularity(index_->index.granularity)
{
    idx_reader = std::make_unique<MergeTreeIndexReader>(index_, part_,
        part_->getMarksCount(), MarkRanges{MarkRange(0, part_->getMarksCount())},
        reader_settings_, context_->getMarkCache().get());

    std::unique_ptr<IGinDataPartHelper> gin_part_helper = nullptr;
    if (part_->getType() == IMergeTreeDataPart::Type::CNCH)
    {
        /// Need to follow the part chain and find the right part with this index
        String index_version_file_name = index_->getFileName() + INDEX_FILE_EXTENSION;
        gin_part_helper = std::make_unique<GinDataCNCHPartHelper>(
            part_->getMvccDataPart(index_version_file_name),
            DiskCacheFactory::instance().get(DiskCacheType::MergeTree)->getMetaCache(),
            part_->disk_cache_mode);
    }
    else
    {
        gin_part_helper = std::make_unique<GinDataLocalPartHelper>(*part_);
    }
    cache_and_store = std::make_unique<PostingsCacheForStore>();
    cache_and_store->store = context_->getGINStoreReaderFactory()->get(
        index_->getFileName(), std::move(gin_part_helper));
    cache_and_store->filter_result_cache = context_->getGinIndexFilterResultCache();
}

void MultiIndexFilterCondition::IndexReaderAndStore::resetForNewRanges()
{
    cache_and_store->cache.clear();
    cached_granule = nullptr;
}

void MultiIndexFilterCondition::IndexReaderAndStore::loadGranuleForRanges(
    const MarkRanges& sorted_mark_ranges_, IndexTimeWatcher& idx_timer_)
{
    cached_granule = nullptr;

    size_t last_read_granule = std::numeric_limits<size_t>::max() - 1;
    MergeTreeIndexGranulePtr granule = nullptr;
    for (const MarkRange& mark_range : sorted_mark_ranges_)
    {
        size_t index_range_begin = mark_range.begin / index_granularity;
        size_t index_range_end = (mark_range.end + index_granularity - 1) / index_granularity;

        if ((last_read_granule + 1 != index_range_begin)
            && (last_read_granule != index_range_begin))
        {
            idx_timer_.watch(IndexTimeWatcher::Type::SEEK, [&]() {
                idx_reader->seek(index_range_begin);
            });
        }
        for (size_t mark = index_range_begin; mark < index_range_end; ++mark)
        {
            if (last_read_granule != mark || granule == nullptr)
            {
                idx_timer_.watch(IndexTimeWatcher::Type::READ, [&]() {
                    granule = idx_reader->read();
                });
            }
            last_read_granule = mark;

            MergeTreeIndexGranuleInverted* ivt_granule =
                dynamic_cast<MergeTreeIndexGranuleInverted*>(granule.get());
            if (cached_granule == nullptr)
            {
                cached_granule = std::make_shared<MergeTreeIndexGranuleInverted>(*ivt_granule);
            }
            else
            {
                cached_granule->extend(*ivt_granule);
            }
        }
    }
}

MultiIndexFilterCondition::MatchedIndex::MatchedIndex(const CandidateIndex* idx_,
    std::vector<ColumnMatchCondition>&& conds_):
        idx(idx_), match_conditions(std::move(conds_))
{
}

std::unique_ptr<roaring::Roaring> MultiIndexFilterCondition::MatchedIndex::match(
    const MarkRanges& sorted_mark_ranges_, IndexReaderAndStores& readers_and_stores_,
    IndexTimeWatcher& idx_timer_) const
{
    auto iter = readers_and_stores_.find(idx->idx_desc.name);
    if (iter == readers_and_stores_.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index reader and store for "
            "{} not found", idx->idx_desc.name);
    }

    IndexReaderAndStore& reader_and_store = *(iter->second);

    /// 1. Convert mark ranges to granule
    if (reader_and_store.cached_granule == nullptr)
    {
        reader_and_store.loadGranuleForRanges(sorted_mark_ranges_, idx_timer_);
    }

    /// 2. Match granule against this filter and get matching rows
    size_t filter_count = match_conditions.front().key_gin_filters.size();
    std::vector<std::unique_ptr<roaring::Roaring>> filters(filter_count);
    idx_timer_.watch(IndexTimeWatcher::Type::CLAC, [&]() {
        for (const ColumnMatchCondition& column_cond : match_conditions)
        {
            for (size_t row = 0; row < column_cond.key_gin_filters.size(); ++row)
            {
                auto step_filter = std::make_unique<roaring::Roaring>();
                reader_and_store.cached_granule->gin_filters[column_cond.key_position]
                    .contains(column_cond.key_gin_filters[row], *(reader_and_store.cache_and_store),
                        step_filter.get());
                if (filters[row])
                {
                    *filters[row] &= *step_filter;
                }
                else
                {
                    filters[row] = std::move(step_filter);
                }
            }
        }
        for (size_t i = 1; i < filters.size(); ++i)
        {
            *filters[0] |= *filters[i];
        }
    });
    return std::move(filters[0]);
}

bool MultiIndexFilterCondition::atomFromAST(const ASTPtr& node_,
    Block& block_with_constants_, RPNElement& out_) const
{
    Field const_value;
    DataTypePtr const_type;
    if (const auto* func = typeid_cast<const ASTFunction*>(node_.get()))
    {
        const ASTs& args = typeid_cast<const ASTExpressionList&>(*func->arguments).children;
        const auto& func_name = func->name;

        if (args.size() == 3 && func_name == "hasTokenBySeperator")
        {
            return hasTokenBySeperatorAtomFromAST(args, block_with_constants_, out_);
        }
        else if (args.size() != 2)
        {
            return false;
        }

        if (functionIsInOrGlobalInOperator(func_name))
        {
            return inAtomFromAST(func_name, args, out_);
        }
        if (func_name == "multiSearchAny")
        {
            return multiSearchAnyAtomFromAST(args, block_with_constants_, out_);
        }

        /// Get key column position
        size_t key_col_pos = -1;
        if (KeyCondition::getConstant(args[1], block_with_constants_,
            const_value, const_type))
        {
            key_col_pos = 0;
        }
        else if (KeyCondition::getConstant(args[0], block_with_constants_,
            const_value, const_type))
        {
            key_col_pos = 1;
        }
        else
        {
            return false;
        }
        if (key_col_pos == 1 && (func_name != "equals" && func_name != "notEquals"))
        {
            return false;
        }

        if (const_type && const_type->getTypeId() != TypeIndex::String
            && const_type->getTypeId() != TypeIndex::FixedString)
        {
            return false;
        }

        static std::unordered_set<String> supported_funcs = {
            "equals", "notEquals", "hasToken", "hasTokens", "startsWith",
            "endsWith", "like", "notLike"
        };
        if (!supported_funcs.contains(func_name))
        {
            return false;
        }

        bool is_like_func = (func_name == "like" || func_name == "notLike");
        for (const auto& candidate_idx : indices)
        {
            std::optional<ColumnMatchCondition> matched_condition =
                candidate_idx.tryMatchIndexOnColumn(args[key_col_pos], const_type, const_value,
                    is_like_func);
            if (matched_condition.has_value())
            {
                out_.matched_indices.emplace_back(&candidate_idx,
                    std::vector<ColumnMatchCondition>{std::move(matched_condition.value())});
            }
        }

        if (!out_.matched_indices.empty())
        {
            out_.function = (func_name == "notEquals" || func_name == "notLike") ?
                RPNElement::FUNCTION_NOT_EQUALS : RPNElement::FUNCTION_EQUALS;
            return true;
        }
        else
        {
            return false;
        }
    }
    else if (KeyCondition::getConstant(node_, block_with_constants_, const_value,
        const_type))
    {
        if (const_value.getType() == Field::Types::UInt64
            || const_value.getType() == Field::Types::Int64
            || const_value.getType() == Field::Types::Float64)
        {
            out_.function = const_value.get<UInt64>() ? RPNElement::ALWAYS_TRUE
                : RPNElement::ALWAYS_FALSE;
            return true;
        }
    }
    return false;
}

bool MultiIndexFilterCondition::hasTokenBySeperatorAtomFromAST(const ASTs& args_,
    Block& block_with_constants_, RPNElement& out_) const
{
    for (const auto& candidate_idx : indices)
    {
        if (candidate_idx.ivt_idx->token_extractor == nullptr)
        {
            continue;
        }
        const CharSeperatorTokenExtractor* sep_tokenizer =
            dynamic_cast<const CharSeperatorTokenExtractor*>(candidate_idx.ivt_idx->token_extractor.get());
        if (sep_tokenizer == nullptr)
        {
            continue;
        }

        /// Check constant field
        Field const_value;
        Field seperator_const_value;
        DataTypePtr const_type;
        DataTypePtr seperator_const_type;
        if (!KeyCondition::getConstant(args_[1], block_with_constants_, const_value, const_type)
            || !KeyCondition::getConstant(args_[2], block_with_constants_, seperator_const_value, seperator_const_type))
        {
            continue;
        }

        if (seperator_const_type->getTypeId() != TypeIndex::String
            && seperator_const_type->getTypeId() != TypeIndex::FixedString
            && const_type->getTypeId() != TypeIndex::String
            && const_type->getTypeId() != TypeIndex::FixedString)
        {
            continue;
        }

        String seperators_str = seperator_const_value.get<String>();
        std::unordered_set<char> seperator_set(seperators_str.begin(), seperators_str.end());
        if (seperator_set != sep_tokenizer->seperators())
        {
            continue;
        }

        std::optional<ColumnMatchCondition> matched_condition = candidate_idx.tryMatchIndexOnColumn(
            args_[0], const_type, const_value, false);
        if (matched_condition.has_value())
        {
            out_.matched_indices.emplace_back(&candidate_idx,
                std::vector<ColumnMatchCondition>{std::move(matched_condition.value())});
        }
    }
    if (!out_.matched_indices.empty())
    {
        out_.function = RPNElement::FUNCTION_EQUALS;
        return true;
    }
    return false;
}

bool MultiIndexFilterCondition::inAtomFromAST(const String& func_name_,
    const ASTs& args_, RPNElement& out_) const
{
    const ASTPtr& lhs = args_[0];
    const ASTPtr& rhs = args_[1];

    const auto* lhs_tuple = typeid_cast<const ASTFunction*>(lhs.get());
    ASTs argument_columns = (lhs_tuple && lhs_tuple->name == "tuple") ?
        lhs_tuple->arguments->children : ASTs({lhs});

    for (const auto& candidate_idx : indices)
    {
        std::vector<ColumnMatchCondition> matched_conditions =
            candidate_idx.tryMatchIndexOnColumns(argument_columns, rhs, prepared_sets);
        if (!matched_conditions.empty())
        {
            out_.matched_indices.emplace_back(&candidate_idx,
                std::move(matched_conditions));
        }
    }

    if (!out_.matched_indices.empty())
    {
        out_.function = (func_name_ == "in") ? RPNElement::FUNCTION_IN :
            RPNElement::FUNCTION_NOT_IN;
        return true;
    }
    return false;
}

bool MultiIndexFilterCondition::multiSearchAnyAtomFromAST(const ASTs& args_, Block& block_with_constants_,
    RPNElement& out_) const
{
    Field const_value;
    DataTypePtr const_type;
    if (!KeyCondition::getConstant(args_[1], block_with_constants_, const_value, const_type))
    {
        return false;
    }
    if (const_type && const_type->getTypeId() != TypeIndex::Array)
    {
        return false;
    }

    for (const auto& candidate_idx : indices)
    {
        std::optional<ColumnMatchCondition> matched_condition =
            candidate_idx.tryMatchIndexOnColumn(args_[0], const_type, const_value, false);
        if (matched_condition.has_value())
        {
            out_.matched_indices.emplace_back(&candidate_idx,
                std::vector<ColumnMatchCondition>{std::move(matched_condition.value())});
        }
    }
    if (!out_.matched_indices.empty())
    {
        out_.function = RPNElement::FUNCTION_MULTI_SEARCH;
        return true;
    }
    else
    {
        return false;
    }
}

MultiIndexFilterCondition::Executor::Executor(const MultiIndexFilterCondition& condition_,
    const MergeTreeDataPartPtr& part_):
        condition(condition_), part(part_)
{
    for (const auto& candidate : condition.indices)
    {
        readers_and_stores.emplace(candidate.idx_desc.name,
            std::make_unique<IndexReaderAndStore>(candidate.idx, part_,
                condition.reader_settings, condition.context));
    }
}

std::unique_ptr<roaring::Roaring> MultiIndexFilterCondition::Executor::match(
    const MarkRanges& mark_ranges_, IndexTimeWatcher& idx_timer_)
{
    /// 1. Reset cache info
    for (auto& reader_and_store : readers_and_stores)
    {
        reader_and_store.second->resetForNewRanges();
    }

    /// 2. Sort mark ranges
    MarkRanges sorted_mark_ranges = mark_ranges_;
    std::sort(sorted_mark_ranges.begin(), sorted_mark_ranges.end(),
        [](const MarkRange& lhs_, const MarkRange& rhs_) {
            return lhs_.begin < rhs_.begin || (lhs_.begin == rhs_.begin && lhs_.end < rhs_.end);
        }
    );

    return matchingRows(sorted_mark_ranges, part->index_granularity, idx_timer_);
}

std::unique_ptr<roaring::Roaring> MultiIndexFilterCondition::Executor::matchingRows(
    const MarkRanges& sorted_mark_ranges_,
    const MergeTreeIndexGranularity& part_granularity_, IndexTimeWatcher& idx_timer_)
{
    std::vector<BoolMask> rpn_stack;
    /// Only deterministic operator will generate filter, otherwise it will put a nullptr
    /// in stack. If can_be_true is true, then a nullptr means every row need to read. If
    /// can_be_true is false, then a nullptr means no row is need to read
    std::vector<std::unique_ptr<roaring::Roaring>> filter_stack;

    for (const auto& element : condition.rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
            filter_stack.emplace_back(nullptr);
        }
        else if (element.function == RPNElement::FUNCTION_EQUALS
            || element.function == RPNElement::FUNCTION_NOT_EQUALS
            || element.function == RPNElement::FUNCTION_IN
            || element.function == RPNElement::FUNCTION_NOT_IN
            || element.function == RPNElement::FUNCTION_MULTI_SEARCH)
        {
            size_t total_filter_condition_size = 0;
            for (const auto& idx : element.matched_indices)
                for (const auto& cond : idx.match_conditions)
                    total_filter_condition_size += cond.key_gin_filters.size();

            if (condition.search_filter_size_limit < total_filter_condition_size)
            {
                rpn_stack.emplace_back(true, true);
                filter_stack.emplace_back(nullptr);
            }
            else
            {
                std::unique_ptr<roaring::Roaring> operator_filter = nullptr;
                for (const auto& idx : element.matched_indices)
                {
                    std::unique_ptr<roaring::Roaring> ret = idx.match(
                        sorted_mark_ranges_, readers_and_stores, idx_timer_);
                    if (operator_filter == nullptr)
                    {
                        operator_filter = std::move(ret);
                    }
                    else
                    {
                        *operator_filter &= *ret;
                    }
                }
                rpn_stack.emplace_back(!operator_filter->isEmpty(), true);
                filter_stack.emplace_back(std::move(operator_filter));

                if (element.function == RPNElement::FUNCTION_NOT_EQUALS
                    || element.function == RPNElement::FUNCTION_NOT_IN)
                {
                    rpn_stack.back() = !rpn_stack.back();
                    flipFilterWithMarkRanges(sorted_mark_ranges_, part_granularity_,
                        *filter_stack.back());
                }
            }
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            rpn_stack.back() = !rpn_stack.back();
            if (auto& filter = filter_stack.back(); filter != nullptr)
            {
                flipFilterWithMarkRanges(sorted_mark_ranges_, part_granularity_,
                    *filter_stack.back());
            }
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;

            std::unique_ptr<roaring::Roaring> filter1 = std::move(filter_stack.back());
            filter_stack.pop_back();
            std::unique_ptr<roaring::Roaring> filter2 = std::move(filter_stack.back());
            filter_stack.pop_back();
            if (filter1 != nullptr && filter2 != nullptr)
            {
                *filter2 &= *filter1;
                filter_stack.emplace_back(std::move(filter2));
            }
            else if (filter1 == nullptr && filter2 == nullptr)
            {
                filter_stack.emplace_back(nullptr);
            }
            else
            {
                if (filter1 == nullptr)
                {
                    std::swap(arg1, arg2);
                    std::swap(filter1, filter2);
                }
                filter_stack.emplace_back(arg2.can_be_true ? std::move(filter1) : nullptr);
            }
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;

            std::unique_ptr<roaring::Roaring> filter1 = std::move(filter_stack.back());
            filter_stack.pop_back();
            std::unique_ptr<roaring::Roaring> filter2 = std::move(filter_stack.back());
            filter_stack.pop_back();
            if (filter1 != nullptr && filter2 != nullptr)
            {
                *filter2 |= *filter1;
                filter_stack.emplace_back(std::move(filter2));
            }
            else if (filter1 == nullptr && filter2 == nullptr)
            {
                filter_stack.emplace_back(nullptr);
            }
            else
            {
                if (filter1 == nullptr)
                {
                    std::swap(arg1, arg2);
                    std::swap(filter1, filter2);
                }
                filter_stack.emplace_back(arg2.can_be_true ? nullptr : std::move(filter1));
            }
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.emplace_back(false, true);
            filter_stack.emplace_back(nullptr);
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.emplace_back(true, false);
            filter_stack.emplace_back(nullptr);
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unexpected function type {}"
                "in MultiIndexFilterCondition::matchingRows", element.function);
        }
    }

    if (rpn_stack.size() != 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected rpn stack size {} "
            "should be 1", rpn_stack.size());
    }
    if (filter_stack.size() != 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected filter stack size {} "
            "should be 1", filter_stack.size());
    }

    if (auto& filter = filter_stack[0]; filter != nullptr)
    {
        /// Remove ranges outside mark ranges
        return std::move(filter);
    }
    else
    {
        std::unique_ptr<roaring::Roaring> result = std::make_unique<roaring::Roaring>();
        if (rpn_stack[0].can_be_true)
        {
            setFilterWithMarkRanges(sorted_mark_ranges_, part_granularity_, *result);
        }
        return result;
    }
}

MultiIndexFilterCondition::MultiIndexFilterCondition(
    const std::vector<MergeTreeIndexPtr>& indices_, const ContextPtr& context_,
    const SelectQueryInfo& query_info_, const MergeTreeReaderSettings& reader_settings_):
        search_filter_size_limit(context_->getSettingsRef().skip_inverted_index_term_size),
        context(context_), reader_settings(reader_settings_),
        prepared_sets(query_info_.sets)
{
    for (const auto& index : indices_)
    {
        indices.emplace_back(index);
    }

    rpn = std::move(RPNBuilder<RPNElement>(
        query_info_, context_,
        [this](const ASTPtr& node, ContextPtr, Block& block_with_constants, RPNElement& out) {
            return this->atomFromAST(node, block_with_constants, out);
        }
    ).extractRPN());
}

/// TODO: Prune unnecessary index based on alwaysUnknownOrTrue info
std::vector<String> MultiIndexFilterCondition::usefulIndices() const
{
    struct EvaluationInfo
    {
        explicit EvaluationInfo(bool always_unknown_or_true_,
            const std::unordered_set<String>& index_names_ = {}):
                always_unknown_or_true(always_unknown_or_true_),
                useful_index_names(index_names_) {}

        bool always_unknown_or_true;
        std::unordered_set<String> useful_index_names;
    };
    std::vector<EvaluationInfo> rpn_stack;

    for (const auto& element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN
            || element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.emplace_back(true);
        }
        else if (element.function == RPNElement::FUNCTION_EQUALS
            || element.function == RPNElement::FUNCTION_NOT_EQUALS
            || element.function == RPNElement::FUNCTION_IN
            || element.function == RPNElement::FUNCTION_NOT_IN
            || element.function == RPNElement::FUNCTION_MULTI_SEARCH
            || element.function == RPNElement::ALWAYS_FALSE)
        {
            std::unordered_set<String> useful_indices;
            for (const auto& idx : element.matched_indices)
            {
                useful_indices.insert(idx.idx->idx_desc.name);
            }
            rpn_stack.emplace_back(false, useful_indices);
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            EvaluationInfo info1 = std::move(rpn_stack.back());
            rpn_stack.pop_back();
            EvaluationInfo& info2 = rpn_stack.back();
            if (info1.always_unknown_or_true && info2.always_unknown_or_true)
            {
                info2.always_unknown_or_true = true;
                info2.useful_index_names.clear();
            }
            else
            {
                info2.always_unknown_or_true = false;
                info2.useful_index_names.insert(info1.useful_index_names.begin(),
                    info1.useful_index_names.end());
            }
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            EvaluationInfo info1 = std::move(rpn_stack.back());
            rpn_stack.pop_back();
            EvaluationInfo& info2 = rpn_stack.back();
            if (!info1.always_unknown_or_true && !info2.always_unknown_or_true)
            {
                info2.always_unknown_or_true = false;
                info2.useful_index_names.insert(info1.useful_index_names.begin(),
                    info1.useful_index_names.end());
            }
            else
            {
                info2.always_unknown_or_true = true;
                info2.useful_index_names.clear();
            }
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Uknown function type {} in rpn",
                element.function);
        }
    }
    if (rpn_stack.size() != 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected rpn stack size {}, should "
            "be 1", rpn_stack.size());
    }
    return std::vector<String>(rpn_stack[0].useful_index_names.begin(),
        rpn_stack[0].useful_index_names.end());
}

std::unique_ptr<MultiIndexFilterCondition::Executor> MultiIndexFilterCondition::executor(
    const MergeTreeDataPartPtr& part_) const
{
    return std::make_unique<Executor>(*this, part_);
}

}
