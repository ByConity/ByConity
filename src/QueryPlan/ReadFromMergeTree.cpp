#include <QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPipeline.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/Transforms/ReverseTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/AddingSelectorTransform.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/LimitTransform.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeReverseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeThreadSelectBlockInputProcessor.h>
#include <Storages/MergeTree/LateMaterialize/MergeTreeSelectProcessorLM.h>
#include <Storages/MergeTree/LateMaterialize/MergeTreeReverseSelectProcessorLM.h>
#include <Storages/MergeTree/LateMaterialize/MergeTreeThreadSelectProcessorLM.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/VirtualColumnUtils.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/MapHelpers.h>
#include <Functions/IFunction.h>
#include <common/logger_useful.h>
#include <Common/JSONBuilder.h>
#include <Common/escapeForFileName.h>
#include "Storages/MergeTree/MergeTreeIOSettings.h"
#include <Parsers/queryToString.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/RewriteDistributedQueryVisitor.h>
#include <Optimizer/PredicateUtils.h>
#include <Storages/MergeTree/FilterWithRowUtils.h>

namespace ProfileEvents
{
    extern const Event SelectedParts;
    extern const Event SelectedRanges;
    extern const Event SelectedMarks;
    extern const Event TotalGranulesCount;
    extern const Event TotalSkippedGranules;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INDEX_NOT_USED;
    extern const int LOGICAL_ERROR;
}

// Update read settings from merge tree settings and query settings. Query settings
// have higher priority than merge tree settings
// QuerySetting\MergeTreeSettings  changed          unchanged
//                        changed  QuerySetting     QuerySetting
//                      unchanged MergeTreeSetting  QuerySetting
#define UNIFY_MERGE_TREE_QUERY_SETTINGS(merge_tree_settings, query_settings, read_settings, setting_name) \
    do \
    { \
        (read_settings).setting_name = \
            (merge_tree_settings).setting_name.changed \
                && !(query_settings).setting_name.changed ? \
                    (merge_tree_settings).setting_name \
                    : (query_settings).setting_name; \
    } while(0)

static const char * indexTypeToString(ReadFromMergeTree::IndexType type)
{
    switch (type)
    {
        case ReadFromMergeTree::IndexType::None:
            return "None";
        case ReadFromMergeTree::IndexType::MinMax:
            return "MinMax";
        case ReadFromMergeTree::IndexType::Partition:
            return "Partition";
        case ReadFromMergeTree::IndexType::PrimaryKey:
            return "PrimaryKey";
        case ReadFromMergeTree::IndexType::Skip:
            return "Skip";
        case ReadFromMergeTree::IndexType::Bitmap:
            return "Bitmap";
    }

    __builtin_unreachable();
}

static MergeTreeReaderSettings getMergeTreeReaderSettings(const ContextPtr & context,
    const MergeTreeMetaBase & data)
{
    MergeTreeReaderSettings settings{
        .read_settings = context->getReadSettings(),
        .save_marks_in_cache = true,
        .checksum_on_read = context->getSettingsRef().checksum_on_read,
        .read_source_bitmap = !context->getSettingsRef().use_encoded_bitmap,
    };
    UNIFY_MERGE_TREE_QUERY_SETTINGS(*data.getSettings(), context->getSettingsRef(),
        settings.read_settings, filtered_ratio_to_use_skip_read);

    settings.setDiskCacheSteaing(data.getSettings()->disk_cache_stealing_mode);
    return settings;
}

static MergeTreeData::DataPartsVector getSelectedPartsVector(const ReadFromMergeTree::AnalysisResult & result)
{
    MergeTreeData::DataPartsVector selected_parts_vector;
    for (auto & part_with_ranges : result.parts_with_ranges)
        selected_parts_vector.push_back(part_with_ranges.data_part);

    return selected_parts_vector;
}

static Array extractMapColumnKeys(const MergeTreeMetaBase & data, const MergeTreeMetaBase::DataPartsVector & parts)
{
    Array res;

    std::unordered_map<String, DataTypePtr> map_types;
    std::unordered_map<String, MutableColumnPtr> map_keys;
    /// get all map columns from storage
    StorageMetadataPtr metadata = data.getInMemoryMetadataPtr();
    const auto & columns = metadata->getColumns();
    for (auto it = columns.begin(); it != columns.end(); it++)
    {
        if (it->type->isMap() && it->type->isByteMap())
            map_types[it->name] = it->type;
    }

    phmap::flat_hash_set<String> implicit_column_files;

    LoggerPtr logger = nullptr;
    for (auto & part : parts)
    {
        for (auto & [file, _] : part->getChecksums()->files)
        {
            /// Parsing map keys from file name is a little heavy, so we remove duplicate file names first.
            if (isMapImplicitKey(file))
                implicit_column_files.insert(file);
        }
    }

    for (const auto & file: implicit_column_files)
    {
        if (isMapBaseFile(file))
            continue;

        String map_name = parseMapNameFromImplicitFileName(file);
        if (!isMapImplicitDataFileNameNotBaseOfSpecialMapName(file, map_name))
            continue;
        String key_name = parseKeyNameFromImplicitFileName(file, map_name);

        if (!map_types.count(map_name))
        {
            if (unlikely(logger == nullptr))
                logger = getLogger(data.getLogName() + " (ExtractMapKeys)");
            LOG_WARNING(logger, "Can not find byte map column {} of implicit file {}", map_name, file);
            continue;
        }
        auto type = map_types[map_name];
        auto map_key_type = static_cast<const DataTypeMap *>(type.get())->getKeyType();
        if (!map_keys.count(map_name))
            map_keys[map_name] = IColumn::mutate(map_key_type->createColumn());

        map_keys[map_name]->insert(map_key_type->stringToVisitorField(key_name));
    }

    for (auto & [map_name, column] : map_keys)
    {
        auto type_it = map_types.find(map_name);
        if (type_it == map_types.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find column {}", map_name);

        auto map_key_type = static_cast<const DataTypeMap *>(type_it->second.get())->getKeyType();
        auto serialization = map_key_type->getDefaultSerialization();

        for (size_t i = 0; i < column->size(); ++i)
        {
            WriteBufferFromOwnString buffer;
            serialization->serializeText(*column, i, buffer, {});
            res.push_back(Tuple{map_name, buffer.str()});
        }
    }

    return res;
}

static bool isSamePartition(const RangesInDataPart & lhs, const RangesInDataPart & rhs)
{
    return lhs.data_part->partition.value == rhs.data_part->partition.value;
}

static bool canReadInPartitionOrder(
    const StorageInMemoryMetadata & metadata,
    const InputOrderInfo & input_order_info,
    const ASTSelectQuery & select,
    int partition_by_monotonicity_hint)
{
    if (!metadata.isPartitionKeyDefined() || !metadata.isSortingKeyDefined())
        return false;

    const auto & partition_key = metadata.getPartitionKey();
    Names minmax_columns = partition_key.expression->getRequiredColumns();
    /// only support partition by with single required column.
    if (minmax_columns.size() != 1)
        return false;

    String partition_column = minmax_columns[0];
    Names sorting_columns = metadata.getSortingKeyColumns();
    chassert(sorting_columns.size() >= input_order_info.order_key_prefix_descr.size());
    /// optimizer guarantees that order_key_prefix is a prefix of sorting columns
    sorting_columns.resize(input_order_info.order_key_prefix_descr.size());

    /// sorting columns should contain partition column
    auto partition_column_it = std::find(sorting_columns.begin(), sorting_columns.end(), partition_column);
    if (partition_column_it == sorting_columns.end())
        return false;

    /// Allow table "partition by c order by (a, b, c)" for query "where a={} and b={} order by c",
    /// where all sorting columns before partition column match single value,
    /// note that in this case, input order is (a, b, c)
    if (partition_column_it != sorting_columns.begin())
    {
        NameSet single_value_columns;
        auto collect = [&](const ASTPtr & filter)
        {
            if (!filter)
                return;

            for (const auto & conjunct : PredicateUtils::extractConjuncts(filter->clone()))
            {
                const auto * func = conjunct->as<ASTFunction>();
                if (!func || func->name != "equals")
                    continue;
                const auto * column = func->arguments->children[0]->as<ASTIdentifier>();
                const auto * literal = func->arguments->children[1]->as<ASTLiteral>();
                if (column && literal)
                    single_value_columns.insert(column->name());
            }
        };
        collect(select.where());
        collect(select.prewhere());
        auto match_single_value = [&](const String & name) { return single_value_columns.count(name); };
        if (!std::all_of(sorting_columns.begin(), partition_column_it, match_single_value))
            return false;
    }

    /// fast path for: order by sort_column partition by sort_column
    if (partition_key.column_names.front() == *partition_column_it)
        return true;

    /// for multi-level partition like "(toDate(ts), toHour(ts))",
    /// it's difficult to deduce the monotonicity of the partition function.
    /// currently we rely on table hint `partition_by_monotonicity_hint` for it.
    if (partition_key.column_names.size() > 1)
        return partition_by_monotonicity_hint > 0;

    /// for single-level partition like "partition by func(x) order by (x)",
    /// deduce the monotonicity of partition func, allow when monotonic nondecreasing
    IFunction::Monotonicity monotonicity;
    for (const auto & action : partition_key.expression->getActions())
    {
        if (action.node->type != ActionsDAG::ActionType::FUNCTION)
        {
            continue;
        }

        /// Allow only one simple monotonic functions with one argument
        if (monotonicity.is_monotonic)
        {
            monotonicity.is_monotonic = false;
            break;
        }

        if (action.node->children.size() != 1 || action.node->children.at(0)->result_name != *partition_column_it)
            break;

        const auto & func = *action.node->function_base;
        if (!func.hasInformationAboutMonotonicity())
            break;

        monotonicity = func.getMonotonicityForRange(*func.getArgumentTypes().at(0), {}, {});
    }

    return monotonicity.is_monotonic && monotonicity.is_positive;
}

ReadFromMergeTree::ReadFromMergeTree(
    MergeTreeMetaBase::DataPartsVector parts_,
    MergeTreeMetaBase::DeleteBitmapGetter delete_bitmap_getter_,
    Names real_column_names_,
    Names virt_column_names_,
    const MergeTreeMetaBase & data_,
    const SelectQueryInfo & query_info_,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr context_,
    size_t max_block_size_,
    size_t num_streams_,
    bool sample_factor_column_queried_,
    bool map_column_keys_column_queried_,
    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read_,
    LoggerPtr log_,
    MergeTreeDataSelectAnalysisResultPtr analyzed_result_ptr_)
    : ISourceStep(DataStream{
    .header = query_info_.atomic_predicates.empty()
        ? MergeTreeBaseSelectProcessor::transformHeader(
                storage_snapshot_->getSampleBlockForColumns(real_column_names_),
                    getPrewhereInfo(query_info_),
                    data_.getPartitionValueType(),
                    virt_column_names_,
                    getIndexContext(query_info_), query_info_.read_bitmap_index)
        : MergeTreeBaseSelectProcessorLM::transformHeader(
                storage_snapshot_->getSampleBlockForColumns(real_column_names_),
            query_info_,
            data_.getPartitionValueType(),
            virt_column_names_)})
    , reader_settings(getMergeTreeReaderSettings(context_, data_))
    , prepared_parts(std::move(parts_))
    , delete_bitmap_getter(std::move(delete_bitmap_getter_))
    , real_column_names(std::move(real_column_names_))
    , virt_column_names(std::move(virt_column_names_))
    , data(data_)
    , query_info(query_info_)
    , prewhere_info(getPrewhereInfo(query_info))
    , actions_settings(ExpressionActionsSettings::fromContext(context_))
    , storage_snapshot(std::move(storage_snapshot_))
    , metadata_for_reading(storage_snapshot->getMetadataForQuery())
    , context(std::move(context_))
    , max_block_size(max_block_size_)
    , min_block_size(context->getSettingsRef().min_block_size)
    , requested_num_streams(num_streams_)
    , preferred_block_size_bytes(context->getSettingsRef().preferred_block_size_bytes)
    , preferred_max_column_in_block_size_bytes(context->getSettingsRef().preferred_max_column_in_block_size_bytes)
    , size_predictor_estimate_lc_size_by_fullstate(context->getSettingsRef().size_predictor_estimate_lc_size_by_fullstate)
    , sample_factor_column_queried(sample_factor_column_queried_)
    , map_column_keys_column_queried(map_column_keys_column_queried_)
    , max_block_numbers_to_read(std::move(max_block_numbers_to_read_))
    , log(log_), analyzed_result_ptr(analyzed_result_ptr_)
{
    if (sample_factor_column_queried)
    {
        /// Only _sample_factor virtual column is added by ReadFromMergeTree
        /// Other virtual columns are added by MergeTreeBaseSelectProcessor.
        auto type = std::make_shared<DataTypeFloat64>();
        output_stream->header.insert({type->createColumn(), type, "_sample_factor"});
    }

    if (map_column_keys_column_queried)
    {
        auto type = std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}));
        output_stream->header.insert({type->createColumn(), type, "_map_column_keys"});
    }
}

Pipe ReadFromMergeTree::readFromPool(
    RangesInDataParts parts_with_range,
    Names required_columns,
    size_t max_streams,
    size_t min_marks_for_concurrent_read,
    bool use_uncompressed_cache)
{
    Pipes pipes;
    size_t sum_marks = 0;
    size_t total_rows = 0;

    for (const auto & part : parts_with_range)
    {
        sum_marks += part.getMarksCount();
        total_rows += part.getRowsCount();
    }

    const auto & settings = context->getSettingsRef();
    MergeTreeReadPool::BackoffSettings backoff_settings(settings);

    auto pool = std::make_shared<MergeTreeReadPool>(
        max_streams,
        sum_marks,
        min_marks_for_concurrent_read,
        std::move(parts_with_range),
        delete_bitmap_getter,
        data,
        storage_snapshot,
        query_info,
        true,
        required_columns,
        backoff_settings,
        settings.preferred_block_size_bytes,
        false);

    auto logger = getLogger(data.getLogName() + " (SelectExecutor)");
    LOG_DEBUG(logger, "Reading approx. {} rows with {} streams", total_rows, max_streams);
    MergeTreeStreamSettings stream_settings {
        .min_marks_for_concurrent_read = min_marks_for_concurrent_read,
        .max_block_size = max_block_size,
        .min_block_size = min_block_size,
        .preferred_block_size_bytes = settings.preferred_block_size_bytes,
        .preferred_max_column_in_block_size_bytes = settings.preferred_max_column_in_block_size_bytes,
        .size_predictor_estimate_lc_size_by_fullstate = settings.size_predictor_estimate_lc_size_by_fullstate,
        .use_uncompressed_cache = use_uncompressed_cache,
        .actions_settings = actions_settings,
        .reader_settings = reader_settings
    };
    if (!query_info.atomic_predicates.empty())
    {
        LOG_DEBUG(logger, "readFromPool: There are {} stages that will be in LM read path, max_streams: {}", query_info.atomic_predicates.size(), max_streams);
        if (reader_settings.read_settings.remote_read_log)
        {
            for (size_t i = 0; i < query_info.atomic_predicates.size(); ++i)
            {
                const auto & p = query_info.atomic_predicates[i];
                LOG_TRACE(logger, "atomic predicate[{}]: {}", i, (p ? p->dump() : "null"));
            }
        }
        for (size_t i = 0; i < max_streams; ++i)
        {
            auto source = std::make_shared<MergeTreeThreadSelectProcessorLM>(
                i, pool, data, storage_snapshot,
                query_info, stream_settings, virt_column_names);

            if (i == 0)
            {
                /// Set the approximate number of rows for the first source only
                source->addTotalRowsApprox(total_rows);
            }

            pipes.emplace_back(std::move(source));
        }
    }
    else
    {
        for (size_t i = 0; i < max_streams; ++i)
        {
            auto source = std::make_shared<MergeTreeThreadSelectBlockInputProcessor>(
                i, pool, data, storage_snapshot, query_info, stream_settings, virt_column_names);

            if (i == 0)
            {
                /// Set the approximate number of rows for the first source only
                source->addTotalRowsApprox(total_rows);
            }

            pipes.emplace_back(std::move(source));
        }
    }

    return Pipe::unitePipes(std::move(pipes));
}

template<typename TSource>
ProcessorPtr ReadFromMergeTree::createSource(
    const RangesInDataPart & part,
    const Names & required_columns,
    const MergeTreeStreamSettings & stream_settings,
    const MarkRangesFilterCallback & range_filter_callback)
{
    return std::make_shared<TSource>(
        data, storage_snapshot, part, delete_bitmap_getter, required_columns, query_info, true, stream_settings, virt_column_names, range_filter_callback);
}

Pipe ReadFromMergeTree::readInOrder(
    RangesInDataParts parts_with_range, Names required_columns,
    ReadType read_type, bool use_uncompressed_cache,
    const std::shared_ptr<SkipIndexFilterInfo>& delayed_index)
{
    Pipes pipes;
    MergeTreeStreamSettings stream_settings{
        .max_block_size = max_block_size,
        .min_block_size = min_block_size,
        .preferred_block_size_bytes = preferred_block_size_bytes,
        .preferred_max_column_in_block_size_bytes = preferred_max_column_in_block_size_bytes,
        .size_predictor_estimate_lc_size_by_fullstate = size_predictor_estimate_lc_size_by_fullstate,
        .use_uncompressed_cache = use_uncompressed_cache,
        .actions_settings = actions_settings,
        .reader_settings = reader_settings
    };

    MarkRangesFilterCallback filter_callback;
    if (delayed_index != nullptr && (!delayed_index->indices.empty() || delayed_index->multi_idx != nullptr))
    {
        filter_callback = [reader_settings = this->reader_settings, ctx = this->context, delayed_index](const MergeTreeDataPartPtr& part_, const MarkRanges& mark_ranges_, roaring::Roaring* row_filter_) {
            size_t total_granules = 0;
            size_t dropped_granules = 0;
            auto ret = MergeTreeDataSelectExecutor::filterMarkRangesForPartByInvertedIndex(
                part_, mark_ranges_, delayed_index, ctx, reader_settings, row_filter_,
                total_granules, dropped_granules);

            ProfileEvents::increment(ProfileEvents::TotalGranulesCount, total_granules);
            ProfileEvents::increment(ProfileEvents::TotalSkippedGranules, dropped_granules);
            LOG_DEBUG(part_->storage.getLogger(), "Delayed index has dropped {}/{} granules for part {}",
                dropped_granules, total_granules, part_->name);

            return ret;
        };
    }

    if (!query_info.atomic_predicates.empty())
    {
        for (const auto & part : parts_with_range)
        {
            auto source = read_type == ReadType::InReverseOrder
                        ? createSource<MergeTreeReverseSelectProcessorLM>(part, required_columns, stream_settings, filter_callback)
                        : createSource<MergeTreeSelectProcessorLM>(part, required_columns, stream_settings, filter_callback);

            pipes.emplace_back(std::move(source));
        }
    }
    else
    {
        for (const auto & part : parts_with_range)
        {
            auto source = read_type == ReadType::InReverseOrder
                        ? createSource<MergeTreeReverseSelectProcessor>(part, required_columns, stream_settings, filter_callback)
                        : createSource<MergeTreeSelectProcessor>(part, required_columns, stream_settings, filter_callback);

            pipes.emplace_back(std::move(source));
        }
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    if (read_type == ReadType::InReverseOrder)
    {
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ReverseTransform>(header);
        });
    }

    return pipe;
}

Pipe ReadFromMergeTree::read(
    RangesInDataParts parts_with_range,
    Names required_columns,
    ReadType read_type,
    size_t max_streams,
    size_t min_marks_for_concurrent_read,
    bool use_uncompressed_cache,
    const std::shared_ptr<SkipIndexFilterInfo>& delayed_index)
{
    if (read_type == ReadType::Default && max_streams > 1)
    {
        if (unlikely(delayed_index != nullptr))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Some skip index is delayed to pipeline execution stage");
        }
        return readFromPool(parts_with_range, required_columns, max_streams, min_marks_for_concurrent_read, use_uncompressed_cache);
    }

    auto pipe = readInOrder(parts_with_range, required_columns, read_type, use_uncompressed_cache, delayed_index);

    /// Use ConcatProcessor to concat sources together.
    /// It is needed to read in parts order (and so in PK order) if single thread is used.
    if (read_type == ReadType::Default && pipe.numOutputPorts() > 1)
        pipe.addTransform(std::make_shared<ConcatProcessor>(pipe.getHeader(), pipe.numOutputPorts()));

    return pipe;
}

namespace
{

struct PartRangesReadInfo
{
    std::vector<size_t> sum_marks_in_parts;

    size_t sum_marks = 0;
    size_t total_rows = 0;
    size_t adaptive_parts = 0;
    size_t index_granularity_bytes = 0;
    size_t max_marks_to_use_cache = 0;
    size_t min_marks_for_concurrent_read = 0;

    bool use_uncompressed_cache = false;

    PartRangesReadInfo(
        const RangesInDataParts & parts,
        const Settings & settings,
        const MergeTreeSettings & data_settings)
    {
        /// Count marks for each part.
        sum_marks_in_parts.resize(parts.size());
        for (size_t i = 0; i < parts.size(); ++i)
        {
            total_rows += parts[i].getRowsCount();
            sum_marks_in_parts[i] = parts[i].getMarksCount();
            sum_marks += sum_marks_in_parts[i];

            if (parts[i].data_part->index_granularity_info.is_adaptive)
                ++adaptive_parts;
        }

        if (adaptive_parts > parts.size() / 2)
            index_granularity_bytes = data_settings.index_granularity_bytes;

        max_marks_to_use_cache = MergeTreeDataSelectExecutor::roundRowsOrBytesToMarks(
            settings.merge_tree_max_rows_to_use_cache,
            settings.merge_tree_max_bytes_to_use_cache,
            data_settings.index_granularity,
            index_granularity_bytes);

        min_marks_for_concurrent_read = MergeTreeDataSelectExecutor::minMarksForConcurrentRead(
            settings.merge_tree_min_rows_for_concurrent_read,
            settings.merge_tree_min_bytes_for_concurrent_read,
            data_settings.index_granularity,
            index_granularity_bytes,
            sum_marks);

        use_uncompressed_cache = settings.use_uncompressed_cache;
        if (sum_marks > max_marks_to_use_cache)
            use_uncompressed_cache = false;
    }
};

}

Pipe ReadFromMergeTree::spreadMarkRangesAmongStreams(
    RangesInDataParts && parts_with_ranges,
    const Names & column_names)
{
    const auto & settings = context->getSettingsRef();
    const auto data_settings = data.getSettings();

    PartRangesReadInfo info(parts_with_ranges, settings, *data_settings);

    if (0 == info.sum_marks)
        return {};

    size_t num_streams = requested_num_streams;
    if (num_streams > 1)
    {
        /// Reduce the number of num_streams if the data is small.
        if (info.sum_marks < num_streams * info.min_marks_for_concurrent_read && parts_with_ranges.size() < num_streams)
        {
            num_streams = std::max((info.sum_marks + info.min_marks_for_concurrent_read - 1) / info.min_marks_for_concurrent_read, parts_with_ranges.size());
            LOG_TRACE(getLogger("ReadFromMergeTree"),
                "Shrink the number of streams from {} to {} since data is small.", requested_num_streams, num_streams);
        }
    }

    return read(
        std::move(parts_with_ranges),
        column_names,
        ReadType::Default,
        num_streams,
        info.min_marks_for_concurrent_read,
        info.use_uncompressed_cache,
        nullptr);
}

static ActionsDAGPtr createProjection(const Block & header)
{
    auto projection = std::make_shared<ActionsDAG>(header.getNamesAndTypesList());
    projection->removeUnusedActions(header.getNames());
    projection->projectInput();
    return projection;
}

namespace
{
template <bool ascend>
struct PartitionValueComparator
{
    bool operator()(const RangesInDataPart & lhs, const RangesInDataPart & rhs) const
    {
        const auto & l = lhs.data_part->partition.value;
        const auto & r = rhs.data_part->partition.value;
        if constexpr (ascend)
            return l < r;
        else
            return l > r;
    }
};
} // anonymouse namespace

Pipe ReadFromMergeTree::spreadMarkRangesAmongStreamsWithPartitionOrder(
    RangesInDataParts && parts_with_ranges,
    const Names & column_names,
    const ActionsDAGPtr & sorting_key_prefix_expr,
    ActionsDAGPtr & out_projection,
    const InputOrderInfoPtr & input_order_info,
    const std::shared_ptr<SkipIndexFilterInfo>& delayed_index)
{
    chassert(!parts_with_ranges.empty());

    /// sort parts by partition value
    if (input_order_info->direction > 0)
        std::sort(parts_with_ranges.begin(), parts_with_ranges.end(), PartitionValueComparator<true>{});
    else
        std::sort(parts_with_ranges.begin(), parts_with_ranges.end(), PartitionValueComparator<false>{});

    Pipes pipes;
    auto prev = parts_with_ranges.begin();
    auto end = parts_with_ranges.end();

    while (prev != end)
    {
        auto curr = std::next(prev);
        while (curr != end && isSamePartition(*prev, *curr))
            ++curr;

        auto pipe = spreadMarkRangesAmongStreamsWithOrder(
            {std::make_move_iterator(prev), std::make_move_iterator(curr)},
            column_names,
            sorting_key_prefix_expr,
            out_projection,
            input_order_info,
            // for the result pipe to output ordered tuples for this partition
            1 /*num_streams*/, true /*need_preliminary_merge*/,
            delayed_index);

        pipes.emplace_back(std::move(pipe));
        prev = curr;
    }

    auto res = Pipe::unitePipes(std::move(pipes));
    if (res.numOutputPorts() > 1)
        res.addTransform(std::make_shared<ConcatProcessor>(res.getHeader(), res.numOutputPorts()));
    return res;
}

Pipe ReadFromMergeTree::spreadMarkRangesAmongStreamsWithOrder(
    RangesInDataParts && parts_with_ranges,
    const Names & column_names,
    const ActionsDAGPtr & sorting_key_prefix_expr,
    ActionsDAGPtr & out_projection,
    const InputOrderInfoPtr & input_order_info,
    size_t num_streams,
    bool need_preliminary_merge,
    const std::shared_ptr<SkipIndexFilterInfo>& delayed_index)
{
    const auto & settings = context->getSettingsRef();
    const auto data_settings = data.getSettings();

    PartRangesReadInfo info(parts_with_ranges, settings, *data_settings);

    Pipes res;

    if (info.sum_marks == 0)
        return {};

    /// Let's split ranges to avoid reading much data.
    auto split_ranges = [rows_granularity = data_settings->index_granularity, max_block_size = max_block_size]
        (const auto & ranges, int direction)
    {
        MarkRanges new_ranges;
        const size_t max_marks_in_range = (max_block_size + rows_granularity - 1) / rows_granularity;
        size_t marks_in_range = 1;

        if (direction == 1)
        {
            /// Split first few ranges to avoid reading much data.
            bool split = false;
            for (auto range : ranges)
            {
                while (!split && range.begin + marks_in_range < range.end)
                {
                    new_ranges.emplace_back(range.begin, range.begin + marks_in_range);
                    range.begin += marks_in_range;
                    marks_in_range *= 2;

                    if (marks_in_range > max_marks_in_range)
                        split = true;
                }
                new_ranges.emplace_back(range.begin, range.end);
            }
        }
        else
        {
            /// Split all ranges to avoid reading much data, because we have to
            ///  store whole range in memory to reverse it.
            for (auto it = ranges.rbegin(); it != ranges.rend(); ++it)
            {
                auto range = *it;
                while (range.begin + marks_in_range < range.end)
                {
                    new_ranges.emplace_front(range.end - marks_in_range, range.end);
                    range.end -= marks_in_range;
                    marks_in_range = std::min(marks_in_range * 2, max_marks_in_range);
                }
                new_ranges.emplace_front(range.begin, range.end);
            }
        }

        return new_ranges;
    };

    const size_t min_marks_per_stream = (info.sum_marks - 1) / num_streams + 1;

    Pipes pipes;

    for (size_t i = 0; i < num_streams && !parts_with_ranges.empty(); ++i)
    {
        size_t need_marks = min_marks_per_stream;
        RangesInDataParts new_parts;

        /// Loop over parts.
        /// We will iteratively take part or some subrange of a part from the back
        ///  and assign a stream to read from it.
        while (need_marks > 0 && !parts_with_ranges.empty())
        {
            RangesInDataPart part = parts_with_ranges.back();
            parts_with_ranges.pop_back();

            size_t & marks_in_part = info.sum_marks_in_parts.back();

            /// We will not take too few rows from a part.
            if (marks_in_part >= info.min_marks_for_concurrent_read &&
                need_marks < info.min_marks_for_concurrent_read)
                need_marks = info.min_marks_for_concurrent_read;

            /// Do not leave too few rows in the part.
            if (marks_in_part > need_marks &&
                marks_in_part - need_marks < info.min_marks_for_concurrent_read)
                need_marks = marks_in_part;

            MarkRanges ranges_to_get_from_part;

            /// We take the whole part if it is small enough.
            if (marks_in_part <= need_marks)
            {
                ranges_to_get_from_part = part.ranges;

                need_marks -= marks_in_part;
                info.sum_marks_in_parts.pop_back();
            }
            else
            {
                /// Loop through ranges in part. Take enough ranges to cover "need_marks".
                while (need_marks > 0)
                {
                    if (part.ranges.empty())
                        throw Exception("Unexpected end of ranges while spreading marks among streams",
                                        ErrorCodes::LOGICAL_ERROR);

                    MarkRange & range = part.ranges.front();

                    const size_t marks_in_range = range.end - range.begin;
                    const size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

                    ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
                    range.begin += marks_to_get_from_range;
                    marks_in_part -= marks_to_get_from_range;
                    need_marks -= marks_to_get_from_range;
                    if (range.begin == range.end)
                        part.ranges.pop_front();
                }
                parts_with_ranges.emplace_back(part);
            }
            ranges_to_get_from_part = split_ranges(ranges_to_get_from_part, input_order_info->direction);
            new_parts.emplace_back(part.data_part, part.part_index_in_query, std::move(ranges_to_get_from_part));
        }

        auto read_type = input_order_info->direction == 1
                       ? ReadFromMergeTree::ReadType::InOrder
                       : ReadFromMergeTree::ReadType::InReverseOrder;

        pipes.emplace_back(read(std::move(new_parts), column_names, read_type,
                           num_streams, info.min_marks_for_concurrent_read,
                           info.use_uncompressed_cache, delayed_index));
    }

    if (need_preliminary_merge && !pipes.empty())
    {
        SortDescription sort_description;
        for (size_t j = 0; j < input_order_info->order_key_prefix_descr.size(); ++j)
            sort_description.emplace_back(storage_snapshot->metadata->getSortingKey().column_names[j],
                                          input_order_info->direction, 1);

        auto sorting_key_expr = std::make_shared<ExpressionActions>(sorting_key_prefix_expr);

        for (auto & pipe : pipes)
        {
            pipe.addSimpleTransform([sorting_key_expr](const Block & header)
            {
                return std::make_shared<ExpressionTransform>(header, sorting_key_expr);
            });

            if (pipe.numOutputPorts() > 1)
            {
                auto transform = std::make_shared<MergingSortedTransform>(
                        pipe.getHeader(),
                        pipe.numOutputPorts(),
                        sort_description,
                        max_block_size);

                pipe.addTransform(std::move(transform));
            }
        }

        if (!out_projection)
        {
            /// Drop temporary columns, added by 'sorting_key_prefix_expr'
            out_projection = createProjection(pipes.front().getHeader());
        }
    }

    return Pipe::unitePipes(std::move(pipes));
}

static void addMergingFinal(
    Pipe & pipe,
    size_t num_output_streams,
    const SortDescription & sort_description,
    MergeTreeMetaBase::MergingParams merging_params,
    Names partition_key_columns,
    size_t max_block_size)
{
    const auto & header = pipe.getHeader();
    size_t num_outputs = pipe.numOutputPorts();

    auto get_merging_processor = [&]() -> MergingTransformPtr
    {
        switch (merging_params.mode)
        {
            case MergeTreeMetaBase::MergingParams::Ordinary:
            {
                return std::make_shared<MergingSortedTransform>(header, num_outputs,
                           sort_description, max_block_size);
            }

            case MergeTreeMetaBase::MergingParams::Collapsing:
                return std::make_shared<CollapsingSortedTransform>(header, num_outputs,
                           sort_description, merging_params.sign_column, true, max_block_size);

            case MergeTreeMetaBase::MergingParams::Summing:
                return std::make_shared<SummingSortedTransform>(header, num_outputs,
                           sort_description, merging_params.columns_to_sum, partition_key_columns, max_block_size);

            case MergeTreeMetaBase::MergingParams::Aggregating:
                return std::make_shared<AggregatingSortedTransform>(header, num_outputs,
                           sort_description, max_block_size);

            case MergeTreeMetaBase::MergingParams::Replacing:
                return std::make_shared<ReplacingSortedTransform>(header, num_outputs,
                           sort_description, merging_params.version_column, max_block_size);

            case MergeTreeMetaBase::MergingParams::VersionedCollapsing:
                return std::make_shared<VersionedCollapsingTransform>(header, num_outputs,
                           sort_description, merging_params.sign_column, max_block_size);

            case MergeTreeMetaBase::MergingParams::Graphite:
                throw Exception("GraphiteMergeTree doesn't support FINAL", ErrorCodes::LOGICAL_ERROR);
        }

        __builtin_unreachable();
    };

    if (num_output_streams <= 1 || sort_description.empty())
    {
        pipe.addTransform(get_merging_processor());
        return;
    }

    ColumnNumbers key_columns;
    key_columns.reserve(sort_description.size());

    for (const auto & desc : sort_description)
    {
        if (!desc.column_name.empty())
            key_columns.push_back(header.getPositionByName(desc.column_name));
        else
            key_columns.emplace_back(desc.column_number);
    }

    pipe.addSimpleTransform([&](const Block & stream_header)
    {
        return std::make_shared<AddingSelectorTransform>(stream_header, num_output_streams, key_columns);
    });

    pipe.transform([&](OutputPortRawPtrs ports)
    {
        Processors transforms;
        std::vector<OutputPorts::iterator> output_ports;
        transforms.reserve(ports.size() + num_output_streams);
        output_ports.reserve(ports.size());

        for (auto & port : ports)
        {
            auto copier = std::make_shared<CopyTransform>(header, num_output_streams);
            connect(*port, copier->getInputPort());
            output_ports.emplace_back(copier->getOutputs().begin());
            transforms.emplace_back(std::move(copier));
        }

        for (size_t i = 0; i < num_output_streams; ++i)
        {
            auto merge = get_merging_processor();
            merge->setSelectorPosition(i);
            auto input = merge->getInputs().begin();

            /// Connect i-th merge with i-th input port of every copier.
            for (size_t j = 0; j < ports.size(); ++j)
            {
                connect(*output_ports[j], *input);
                ++output_ports[j];
                ++input;
            }

            transforms.emplace_back(std::move(merge));
        }

        return transforms;
    });
}


Pipe ReadFromMergeTree::spreadMarkRangesAmongStreamsFinal(
    RangesInDataParts && parts_with_ranges,
    const Names & column_names,
    ActionsDAGPtr & out_projection)
{
    const auto & settings = context->getSettingsRef();
    const auto data_settings = data.getSettings();

    PartRangesReadInfo info(parts_with_ranges, settings, *data_settings);

    size_t num_streams = requested_num_streams;
    if (num_streams > settings.max_final_threads)
        num_streams = settings.max_final_threads;

    /// If setting do_not_merge_across_partitions_select_final is true than we won't merge parts from different partitions.
    /// We have all parts in parts vector, where parts with same partition are nearby.
    /// So we will store iterators pointed to the beginning of each partition range (and parts.end()),
    /// then we will create a pipe for each partition that will run selecting processor and merging processor
    /// for the parts with this partition. In the end we will unite all the pipes.
    std::vector<RangesInDataParts::iterator> parts_to_merge_ranges;
    auto it = parts_with_ranges.begin();
    parts_to_merge_ranges.push_back(it);

    if (settings.do_not_merge_across_partitions_select_final)
    {
        while (it != parts_with_ranges.end())
        {
            it = std::find_if(
                it, parts_with_ranges.end(), [&it](auto & part) { return it->data_part->info.partition_id != part.data_part->info.partition_id; });
            parts_to_merge_ranges.push_back(it);
        }
        /// We divide threads for each partition equally. But we will create at least the number of partitions threads.
        /// (So, the total number of threads could be more than initial num_streams.
        num_streams /= (parts_to_merge_ranges.size() - 1);
    }
    else
    {
        /// If do_not_merge_across_partitions_select_final is false we just merge all the parts.
        parts_to_merge_ranges.push_back(parts_with_ranges.end());
    }

    Pipes partition_pipes;

    /// If do_not_merge_across_partitions_select_final is true and num_streams > 1
    /// we will store lonely parts with level > 0 to use parallel select on them.
    std::vector<RangesInDataPart> lonely_parts;
    size_t sum_marks_in_lonely_parts = 0;

    for (size_t range_index = 0; range_index < parts_to_merge_ranges.size() - 1; ++range_index)
    {
        Pipe pipe;

        {
            RangesInDataParts new_parts;

            /// If do_not_merge_across_partitions_select_final is true and there is only one part in partition
            /// with level > 0 then we won't postprocess this part and if num_streams > 1 we
            /// can use parallel select on such parts. We save such parts in one vector and then use
            /// MergeTreeReadPool and MergeTreeThreadSelectBlockInputProcessor for parallel select.
            if (num_streams > 1 && settings.do_not_merge_across_partitions_select_final &&
                std::distance(parts_to_merge_ranges[range_index], parts_to_merge_ranges[range_index + 1]) == 1 &&
                parts_to_merge_ranges[range_index]->data_part->info.level > 0)
            {
                sum_marks_in_lonely_parts += parts_to_merge_ranges[range_index]->getMarksCount();
                lonely_parts.push_back(std::move(*parts_to_merge_ranges[range_index]));
                continue;
            }
            else
            {
                for (auto part_it = parts_to_merge_ranges[range_index]; part_it != parts_to_merge_ranges[range_index + 1]; ++part_it)
                {
                    new_parts.emplace_back(part_it->data_part, part_it->part_index_in_query, part_it->ranges);
                }
            }

            if (new_parts.empty())
                continue;

            pipe = read(std::move(new_parts), column_names, ReadFromMergeTree::ReadType::InOrder,
                num_streams, 0, info.use_uncompressed_cache, nullptr);

            /// Drop temporary columns, added by 'sorting_key_expr'
            if (!out_projection)
                out_projection = createProjection(pipe.getHeader());
        }

        auto sorting_expr = std::make_shared<ExpressionActions>(
            metadata_for_reading->getSortingKey().expression->getActionsDAG().clone());

        pipe.addSimpleTransform([sorting_expr](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, sorting_expr);
        });

        /// If do_not_merge_across_partitions_select_final is true and there is only one part in partition
        /// with level > 0 then we won't postprocess this part
        if (settings.do_not_merge_across_partitions_select_final &&
            std::distance(parts_to_merge_ranges[range_index], parts_to_merge_ranges[range_index + 1]) == 1 &&
            parts_to_merge_ranges[range_index]->data_part->info.level > 0)
        {
            partition_pipes.emplace_back(std::move(pipe));
            continue;
        }

        Names sort_columns = metadata_for_reading->getSortingKeyColumns();
        SortDescription sort_description;
        size_t sort_columns_size = sort_columns.size();
        sort_description.reserve(sort_columns_size);

        Names partition_key_columns = metadata_for_reading->getPartitionKey().column_names;

        const auto & header = pipe.getHeader();
        for (size_t i = 0; i < sort_columns_size; ++i)
            sort_description.emplace_back(header.getPositionByName(sort_columns[i]), 1, 1);

        addMergingFinal(
            pipe,
            std::min<size_t>(num_streams, settings.max_final_threads),
            sort_description, data.merging_params, partition_key_columns, max_block_size);

        partition_pipes.emplace_back(std::move(pipe));
    }

    if (!lonely_parts.empty())
    {
        RangesInDataParts new_parts;

        size_t num_streams_for_lonely_parts = num_streams * lonely_parts.size();


        const size_t min_marks_for_concurrent_read = MergeTreeDataSelectExecutor::minMarksForConcurrentRead(
            settings.merge_tree_min_rows_for_concurrent_read,
            settings.merge_tree_min_bytes_for_concurrent_read,
            data_settings->index_granularity,
            info.index_granularity_bytes,
            sum_marks_in_lonely_parts);

        /// Reduce the number of num_streams_for_lonely_parts if the data is small.
        if (sum_marks_in_lonely_parts < num_streams_for_lonely_parts * min_marks_for_concurrent_read && lonely_parts.size() < num_streams_for_lonely_parts)
            num_streams_for_lonely_parts = std::max((sum_marks_in_lonely_parts + min_marks_for_concurrent_read - 1) / min_marks_for_concurrent_read, lonely_parts.size());

        auto pipe = read(std::move(lonely_parts), column_names, ReadFromMergeTree::ReadType::Default,
                num_streams_for_lonely_parts, min_marks_for_concurrent_read, info.use_uncompressed_cache,
                nullptr);

        /// Drop temporary columns, added by 'sorting_key_expr'
        if (!out_projection)
            out_projection = createProjection(pipe.getHeader());

        auto sorting_expr = std::make_shared<ExpressionActions>(
            metadata_for_reading->getSortingKey().expression->getActionsDAG().clone());

        pipe.addSimpleTransform([sorting_expr](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, sorting_expr);
        });

        partition_pipes.emplace_back(std::move(pipe));
    }

    return Pipe::unitePipes(std::move(partition_pipes));
}

MergeTreeDataSelectAnalysisResultPtr ReadFromMergeTree::selectRangesToRead(MergeTreeData::DataPartsVector parts) const
{
    return selectRangesToRead(
        std::move(parts),
        storage_snapshot->metadata,
        storage_snapshot->getMetadataForQuery(),
        query_info,
        context,
        requested_num_streams,
        max_block_numbers_to_read,
        data,
        real_column_names,
        sample_factor_column_queried,
        log);
}

MergeTreeDataSelectAnalysisResultPtr ReadFromMergeTree::selectRangesToRead(
    MergeTreeData::DataPartsVector parts,
    const StorageMetadataPtr & metadata_snapshot_base,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    ContextPtr context,
    unsigned num_streams,
    std::shared_ptr<PartitionIdToMaxBlock> max_block_numbers_to_read,
    const MergeTreeMetaBase & data,
    const Names & real_column_names,
    bool sample_factor_column_queried,
    LoggerPtr log)
{
    AnalysisResult result;
    const auto & settings = context->getSettingsRef();

    size_t total_parts = parts.size();

    auto part_values = MergeTreeDataSelectExecutor::filterPartsByVirtualColumns(data, parts, query_info, context);
    if (part_values && part_values->empty())
        return std::make_shared<MergeTreeDataSelectAnalysisResult>(MergeTreeDataSelectAnalysisResult{.result = std::move(result)});

    result.column_names_to_read = real_column_names;

    /// If there are only virtual columns in the query, you must request at least one non-virtual one.
    if (result.column_names_to_read.empty())
    {
        NamesAndTypesList available_real_columns = metadata_snapshot->getColumns().getAllPhysical();
        result.column_names_to_read.push_back(ExpressionActions::getSmallestColumn(available_real_columns));
    }

    // storage_snapshot->check(result.column_names_to_read);

    // Build and check if primary key is used when necessary
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    Names primary_key_columns = primary_key.column_names;
    KeyCondition key_condition(query_info, context, primary_key_columns, primary_key.expression);

    if (settings.force_primary_key && key_condition.alwaysUnknownOrTrue())
    {
        return std::make_shared<MergeTreeDataSelectAnalysisResult>(MergeTreeDataSelectAnalysisResult{
            .result = std::make_exception_ptr(Exception(
                ErrorCodes::INDEX_NOT_USED,
                "Primary key ({}) is not used and setting 'force_primary_key' is set",
                fmt::join(primary_key_columns, ", ")))});
    }
    LOG_DEBUG(log, "Key condition: {}", key_condition.toString());

    const auto & select = query_info.query->as<ASTSelectQuery &>();

    size_t total_marks_pk = 0;
    size_t parts_before_pk = 0;
    try
    {
        if (query_info.partition_filter)
        {
            /// If partition filter exist reconstruct query info
            SelectQueryOptions options;
            auto mutable_context = Context::createCopy(context);
            ASTPtr copy_select = query_info.query->clone();
            auto & copy_select_query = copy_select->as<ASTSelectQuery &>();
            std::vector<String> setting_names{"enable_partition_filter_push_down", "optimize_move_to_prewhere"};
            for (auto & setting_name : setting_names)
            {
                SettingChange setting;
                setting.name = setting_name;
                setting.value = Field(false);
                if (copy_select_query.settings())
                {
                    auto * set_ast = copy_select_query.settings()->as<ASTSetQuery>();
                    auto it = std::find_if(set_ast->changes.begin(), set_ast->changes.end(), [&](const SettingChange & change) {
                        return change.name == setting_name;
                    });
                    if (it != set_ast->changes.end())
                        it->value = Field(false);
                    else
                        set_ast->changes.emplace_back(setting);
                }
                else
                {
                    ASTSetQuery set_ast;
                    set_ast.is_standalone = false;
                    set_ast.changes.emplace_back(setting);
                    copy_select_query.setExpression(ASTSelectQuery::Expression::SETTINGS, std::make_shared<ASTSetQuery>(set_ast));
                }
            }
            std::vector<ASTPtr> combine_conjuncts;
            combine_conjuncts.emplace_back(query_info.partition_filter->clone());
            if (copy_select_query.getPrewhere())
                combine_conjuncts.emplace_back(copy_select_query.getPrewhere());
            if (copy_select_query.getWhere())
                combine_conjuncts.emplace_back(copy_select_query.getWhere());
            copy_select_query.setExpression(ASTSelectQuery::Expression::WHERE,
               PredicateUtils::combineConjuncts<true, ASTPtr>(combine_conjuncts));
            copy_select_query.setExpression(ASTSelectQuery::Expression::PREWHERE, nullptr);
            /**
            * rewrite distributed query to local query since we should get all final result column from subquery, otherwise, some projection will be ignore,
            */
            auto query_data = RewriteDistributedQueryMatcher::collectTableInfos(copy_select, mutable_context);
            if (!query_data.table_rewrite_info.empty())
                RewriteDistributedQueryVisitor(query_data).visit(copy_select);
            auto interpreter = std::make_shared<InterpreterSelectQuery>(copy_select, mutable_context, options);
            interpreter->execute(true);
            LOG_TRACE(getLogger("ReadFromMergeTree::selectRangesToRead"), "Construct partition filter query {}", queryToString(copy_select));

            MergeTreeDataSelectExecutor::filterPartsByPartition(
                parts,
                part_values,
                metadata_snapshot_base,
                data,
                interpreter->getQueryInfo(),
                context,
                max_block_numbers_to_read.get(),
                log,
                result.index_stats);
        }
        else
        {
            MergeTreeDataSelectExecutor::filterPartsByPartition(
                parts,
                part_values,
                metadata_snapshot_base,
                data,
                query_info,
                context,
                max_block_numbers_to_read.get(),
                log,
                result.index_stats);
        }

        result.sampling = MergeTreeDataSelectExecutor::getSampling(
            select,
            metadata_snapshot->getColumns().getAllPhysical(),
            parts,
            key_condition,
            data,
            metadata_snapshot,
            context,
            sample_factor_column_queried,
            log);

        if (result.sampling.read_nothing)
            return std::make_shared<MergeTreeDataSelectAnalysisResult>(MergeTreeDataSelectAnalysisResult{.result = std::move(result)});

        auto reader_settings = getMergeTreeReaderSettings(context, data);

        for (const auto & part : parts)
            total_marks_pk += part->index_granularity.getMarksCountWithoutFinal();
        parts_before_pk = parts.size();

        Stopwatch stopwatch;

        result.parts_with_ranges = MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes(
            std::move(parts),
            metadata_snapshot,
            query_info,
            context,
            key_condition,
            reader_settings,
            log,
            num_streams,
            result.index_stats,
            *(result.delayed_indices),
            settings.enable_skip_index,
            data,
            result.sampling.use_sampling,
            result.sampling.relative_sample_size);

        if (settings.query_dry_run_mode == QueryDryRunMode::SKIP_READ_PARTS)
            result.parts_with_ranges.clear();

        result.parts_with_ranges = MergeTreeDataSelectExecutor::filterPartsByIntermediateResultCache(
            data.getStorageID(),
            query_info,
            context,
            log,
            result.parts_with_ranges,
            result.part_cache_holder);

        auto cost_micro_seconds = stopwatch.elapsedMicroseconds();
        LOG_DEBUG(log, "Filtering parts by primiry key and skip indexes used: {} micro seconds", cost_micro_seconds);
    }
    catch (...)
    {
        return std::make_shared<MergeTreeDataSelectAnalysisResult>(MergeTreeDataSelectAnalysisResult{.result = std::current_exception()});
    }

    for (const auto & index_stat : result.index_stats)
        LOG_DEBUG(log, "selectRangesToRead index stat : type-{}, name-{}, description-{}, condition-{}, used_keys-{}, num_parts_after-{}, num_granules_after-{}",
                        indexTypeToString(index_stat.type),
                        index_stat.name,
                        index_stat.description,
                        index_stat.condition,
                        fmt::join(index_stat.used_keys, ","),
                        std::to_string(index_stat.num_parts_after),
                        std::to_string(index_stat.num_granules_after));

    size_t sum_marks_pk = total_marks_pk;
    for (const auto & stat : result.index_stats)
        if (stat.type == IndexType::PrimaryKey)
            sum_marks_pk = stat.num_granules_after;

    size_t sum_marks = 0;
    size_t sum_ranges = 0;
    size_t sum_rows = 0;
    NameSet partition_ids;

    for (const auto & part : result.parts_with_ranges)
    {
        sum_ranges += part.ranges.size();
        sum_marks += part.getMarksCount();
        sum_rows += part.getRowsCount();
        partition_ids.insert(part.data_part->info.partition_id);
    }

    result.total_parts = total_parts;
    result.parts_before_pk = parts_before_pk;
    result.selected_parts = result.parts_with_ranges.size();
    result.selected_ranges = sum_ranges;
    result.selected_marks = sum_marks;
    result.selected_marks_pk = sum_marks_pk;
    result.total_marks_pk = total_marks_pk;
    result.selected_rows = sum_rows;
    result.selected_partitions = partition_ids.size();

    const auto & input_order_info = query_info.input_order_info
        ? query_info.input_order_info
        : (query_info.projection ? query_info.projection->input_order_info : nullptr);

    if ((settings.optimize_read_in_order || settings.optimize_aggregation_in_order) && input_order_info)
        result.read_type = (input_order_info->direction > 0) ? ReadType::InOrder
                                                             : ReadType::InReverseOrder;

    return std::make_shared<MergeTreeDataSelectAnalysisResult>(MergeTreeDataSelectAnalysisResult{.result = std::move(result)});
}

ReadFromMergeTree::AnalysisResult ReadFromMergeTree::getAnalysisResult() const
{
    auto result_ptr = analyzed_result_ptr ? analyzed_result_ptr : selectRangesToRead(prepared_parts);
    if (std::holds_alternative<std::exception_ptr>(result_ptr->result))
        std::rethrow_exception(std::get<std::exception_ptr>(result_ptr->result));

    return std::get<ReadFromMergeTree::AnalysisResult>(result_ptr->result);
}

void ReadFromMergeTree::initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    auto result = getAnalysisResult();
    LOG_DEBUG(
        log,
        "Selected {}/{} parts by partition key, {} parts by primary key, {}/{} marks by primary key, {} marks to read from {} ranges",
        result.parts_before_pk,
        result.total_parts,
        result.selected_parts,
        result.selected_marks_pk,
        result.total_marks_pk,
        result.selected_marks,
        result.selected_ranges);

    if (context->getSettingsRef().report_segment_profiles)
        fillRuntimeAttributeDescriptions(result);

    ProfileEvents::increment(ProfileEvents::SelectedParts, result.selected_parts);
    ProfileEvents::increment(ProfileEvents::SelectedRanges, result.selected_ranges);
    ProfileEvents::increment(ProfileEvents::SelectedMarks, result.selected_marks);

    auto query_id_holder = MergeTreeDataSelectExecutor::checkLimits(data, result.parts_with_ranges, context);

    if (result.part_cache_holder)
        pipeline.addCacheHolder(std::move(result.part_cache_holder));

    if (result.parts_with_ranges.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    // extract selected_parts_vector before result.parts_with_ranges be moved
    auto selected_parts_vector = getSelectedPartsVector(result);

    /// Projection, that needed to drop columns, which have appeared by execution
    /// of some extra expressions, and to allow execute the same expressions later.
    /// NOTE: It may lead to double computation of expressions.
    ActionsDAGPtr result_projection;

    Names column_names_to_read = std::move(result.column_names_to_read);
    const auto & select = query_info.query->as<ASTSelectQuery &>();
    if (!select.final() && result.sampling.use_sampling && !context->getSettingsRef().enable_sample_by_range
        && !context->getSettingsRef().enable_deterministic_sample_by_range)
    {
        /// Add columns needed for `sample_by_ast` to `column_names_to_read`.
        /// Skip this if final was used, because such columns were already added from PK.
        std::vector<String> add_columns = result.sampling.filter_expression->getRequiredColumns().getNames();
        column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());
        std::sort(column_names_to_read.begin(), column_names_to_read.end());
        column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()),
                                   column_names_to_read.end());
    }

    const auto & input_order_info = query_info.input_order_info
        ? query_info.input_order_info
        : (query_info.projection ? query_info.projection->input_order_info : nullptr);

    Pipe pipe;

    const auto & settings = context->getSettingsRef();
    bool can_read_in_partition_order = false;

    if (select.final())
    {
        /// Add columns needed to calculate the sorting expression and the sign.
        std::vector<String> add_columns = metadata_for_reading->getColumnsRequiredForSortingKey();
        column_names_to_read.insert(column_names_to_read.end(), add_columns.begin(), add_columns.end());

        if (!data.merging_params.sign_column.empty())
            column_names_to_read.push_back(data.merging_params.sign_column);
        if (!data.merging_params.version_column.empty())
            column_names_to_read.push_back(data.merging_params.version_column);

        std::sort(column_names_to_read.begin(), column_names_to_read.end());
        column_names_to_read.erase(std::unique(column_names_to_read.begin(), column_names_to_read.end()), column_names_to_read.end());

        pipe = spreadMarkRangesAmongStreamsFinal(
            std::move(result.parts_with_ranges),
            column_names_to_read,
            result_projection);
    }
    else if ((settings.optimize_read_in_order || settings.optimize_aggregation_in_order) && input_order_info)
    {
        size_t prefix_size = input_order_info->order_key_prefix_descr.size();
        auto order_key_prefix_ast = metadata_for_reading->getSortingKey().expression_list_ast->clone();
        order_key_prefix_ast->children.resize(prefix_size);

        auto syntax_result = TreeRewriter(context).analyze(order_key_prefix_ast, metadata_for_reading->getColumns().getAllPhysical());
        auto sorting_key_prefix_expr = ExpressionAnalyzer(order_key_prefix_ast, syntax_result, context).getActionsDAG(false);

        can_read_in_partition_order = (settings.optimize_read_in_partition_order || settings.force_read_in_partition_order)
            && canReadInPartitionOrder(
                *metadata_for_reading, *input_order_info, query_info.query->as<ASTSelectQuery &>(),
                data.getSettings()->partition_by_monotonicity_hint);

        if (can_read_in_partition_order && result.selected_partitions > 1)
        {
            pipe = spreadMarkRangesAmongStreamsWithPartitionOrder(
                std::move(result.parts_with_ranges),
                column_names_to_read,
                sorting_key_prefix_expr,
                result_projection,
                input_order_info,
                result.delayed_indices);
        }
        else
        {
            bool need_preliminary_merge = (result.parts_with_ranges.size() > settings.read_in_order_two_level_merge_threshold);
            pipe = spreadMarkRangesAmongStreamsWithOrder(
                std::move(result.parts_with_ranges),
                column_names_to_read,
                sorting_key_prefix_expr,
                result_projection,
                input_order_info,
                requested_num_streams,
                need_preliminary_merge,
                result.delayed_indices);
        }
    }
    else
    {
        pipe = spreadMarkRangesAmongStreams(
            std::move(result.parts_with_ranges),
            column_names_to_read);
    }

    if (settings.force_read_in_partition_order && !can_read_in_partition_order)
        throw Exception(ErrorCodes::INDEX_NOT_USED, "Cannot read in partition order but 'force_read_in_partition_order' is set");

    if (pipe.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputStream().header)));
        return;
    }

    if (result.sampling.use_sampling && !settings.enable_sample_by_range && !settings.enable_deterministic_sample_by_range)
    {
        auto sampling_actions = std::make_shared<ExpressionActions>(result.sampling.filter_expression);
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<FilterTransform>(
                header,
                sampling_actions,
                result.sampling.filter_function->getColumnName(),
                false);
        });
    }

    Block cur_header = pipe.getHeader();

    auto append_actions = [&result_projection](ActionsDAGPtr actions)
    {
        if (!result_projection)
            result_projection = std::move(actions);
        else
            result_projection = ActionsDAG::merge(std::move(*result_projection), std::move(*actions));
    };

    /// By the way, if a distributed query or query to a Merge table is made, then the `_sample_factor` column can have different values.
    if (sample_factor_column_queried)
    {
        ColumnWithTypeAndName column;
        column.name = "_sample_factor";
        column.type = std::make_shared<DataTypeFloat64>();
        column.column = column.type->createColumnConst(0, Field(result.sampling.used_sample_factor));

        auto adding_column = ActionsDAG::makeAddingColumnActions(std::move(column));
        append_actions(std::move(adding_column));
    }

    if (map_column_keys_column_queried)
    {
        if (settings.early_limit_for_map_virtual_columns > 0)
        {
            pipe.addSimpleTransform([&](const Block & header) {
                return std::make_shared<LimitTransform>(header, settings.early_limit_for_map_virtual_columns, 0);
            });
        }

        ColumnWithTypeAndName column;
        column.name = "_map_column_keys";
        column.type = std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}));

        column.column = column.type->createColumnConst(0, Field(extractMapColumnKeys(data, selected_parts_vector)));

        auto adding_column = ActionsDAG::makeAddingColumnActions(std::move(column));
        append_actions(std::move(adding_column));
    }

    if (result_projection)
        cur_header = result_projection->updateHeader(cur_header);

    /// Extra columns may be returned (for example, if sampling is used).
    /// Convert pipe to step header structure.
    if (!isCompatibleHeader(cur_header, getOutputStream().header))
    {
        auto converting = ActionsDAG::makeConvertingActions(
            cur_header.getColumnsWithTypeAndName(),
            getOutputStream().header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

        append_actions(std::move(converting));
    }

    if (result_projection)
    {
        auto projection_actions = std::make_shared<ExpressionActions>(result_projection);
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, projection_actions);
        });
    }

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    // Attach QueryIdHolder if needed
    if (query_id_holder)
        pipe.addQueryIdHolder(std::move(query_id_holder));

    pipeline.init(std::move(pipe));
}

static const char * readTypeToString(ReadFromMergeTree::ReadType type)
{
    switch (type)
    {
        case ReadFromMergeTree::ReadType::Default:
            return "Default";
        case ReadFromMergeTree::ReadType::InOrder:
            return "InOrder";
        case ReadFromMergeTree::ReadType::InReverseOrder:
            return "InReverseOrder";
    }

    __builtin_unreachable();
}

void ReadFromMergeTree::describeActions(FormatSettings & format_settings) const
{
    auto result = getAnalysisResult();
    std::string prefix(format_settings.offset, format_settings.indent_char);
    format_settings.out << prefix << "ReadType: " << readTypeToString(result.read_type) << '\n';

    if (!result.index_stats.empty())
    {
        format_settings.out << prefix << "Parts: " << result.index_stats.back().num_parts_after << '\n';
        format_settings.out << prefix << "Granules: " << result.index_stats.back().num_granules_after << '\n';
    }
}

void ReadFromMergeTree::describeActions(JSONBuilder::JSONMap & map) const
{
    auto result = getAnalysisResult();
    map.add("Read Type", readTypeToString(result.read_type));
    if (!result.index_stats.empty())
    {
        map.add("Parts", result.index_stats.back().num_parts_after);
        map.add("Granules", result.index_stats.back().num_granules_after);
    }
}

void ReadFromMergeTree::describeIndexes(FormatSettings & format_settings) const
{
    auto result = getAnalysisResult();
    auto index_stats = std::move(result.index_stats);

    std::string prefix(format_settings.offset, format_settings.indent_char);
    if (!index_stats.empty())
    {
        /// Do not print anything if no indexes is applied.
        if (index_stats.size() == 1 && index_stats.front().type == IndexType::None)
            return;

        std::string indent(format_settings.indent, format_settings.indent_char);
        format_settings.out << prefix << "Indexes:\n";

        for (size_t i = 0; i < index_stats.size(); ++i)
        {
            const auto & stat = index_stats[i];
            if (stat.type == IndexType::None)
                continue;

            format_settings.out << prefix << indent << indexTypeToString(stat.type) << '\n';

            if (!stat.name.empty())
                format_settings.out << prefix << indent << indent << "Name: " << stat.name << '\n';

            if (!stat.description.empty())
                format_settings.out << prefix << indent << indent << "Description: " << stat.description << '\n';

            if (!stat.used_keys.empty())
            {
                format_settings.out << prefix << indent << indent << "Keys: " << stat.name << '\n';
                for (const auto & used_key : stat.used_keys)
                    format_settings.out << prefix << indent << indent << indent << used_key << '\n';
            }

            if (!stat.condition.empty())
                format_settings.out << prefix << indent << indent << "Condition: " << stat.condition << '\n';

            format_settings.out << prefix << indent << indent << "Parts: " << stat.num_parts_after;
            if (i)
                format_settings.out << '/' << index_stats[i - 1].num_parts_after;
            format_settings.out << '\n';

            format_settings.out << prefix << indent << indent << "Granules: " << stat.num_granules_after;
            if (i)
                format_settings.out << '/' << index_stats[i - 1].num_granules_after;
            format_settings.out << '\n';
        }
    }
}

void ReadFromMergeTree::describeIndexes(JSONBuilder::JSONMap & map) const
{
    auto result = getAnalysisResult();
    auto index_stats = std::move(result.index_stats);

    if (!index_stats.empty())
    {
        /// Do not print anything if no indexes is applied.
        if (index_stats.size() == 1 && index_stats.front().type == IndexType::None)
            return;

        auto indexes_array = std::make_unique<JSONBuilder::JSONArray>();

        for (size_t i = 0; i < index_stats.size(); ++i)
        {
            const auto & stat = index_stats[i];
            if (stat.type == IndexType::None)
                continue;

            auto index_map = std::make_unique<JSONBuilder::JSONMap>();

            index_map->add("Type", indexTypeToString(stat.type));

            if (!stat.name.empty())
                index_map->add("Name", stat.name);

            if (!stat.description.empty())
                index_map->add("Description", stat.description);

            if (!stat.used_keys.empty())
            {
                auto keys_array = std::make_unique<JSONBuilder::JSONArray>();

                for (const auto & used_key : stat.used_keys)
                    keys_array->add(used_key);

                index_map->add("Keys", std::move(keys_array));
            }

            if (!stat.condition.empty())
                index_map->add("Condition", stat.condition);

            if (i)
                index_map->add("Initial Parts", index_stats[i - 1].num_parts_after);
            index_map->add("Selected Parts", stat.num_parts_after);

            if (i)
                index_map->add("Initial Granules", index_stats[i - 1].num_granules_after);
            index_map->add("Selected Granules", stat.num_granules_after);

            indexes_array->add(std::move(index_map));
        }

        map.add("Indexes", std::move(indexes_array));
    }
}
std::shared_ptr<IQueryPlanStep> ReadFromMergeTree::copy(ContextPtr) const
{
    throw Exception("ReadFromMergeTree can not copy", ErrorCodes::NOT_IMPLEMENTED);
}

void ReadFromMergeTree::fillRuntimeAttributeDescriptions(const ReadFromMergeTree::AnalysisResult & result)
{
    auto index_stats = result.index_stats;
    if (!result.index_stats.empty())
    {
        RuntimeAttributeDescription index_desc;
        for (size_t i = 0; i < index_stats.size(); ++i)
        {
            const auto & stat = index_stats[i];
            if (stat.type == IndexType::None)
                continue;
            std::stringstream out;
            out << "Type: " << indexTypeToString(stat.type) << ";";
            if (!stat.name.empty())
                out << " Name: " << stat.name << ";";
            if (!stat.description.empty())
                out << " Description: " << stat.description << ";";
            if (!stat.used_keys.empty())
            {
                String keys = fmt::format("{}", fmt::join(stat.used_keys, ","));
                out << " Keys: " << keys << ";";
            }
            if (!stat.condition.empty())
                out << " Condition: " << stat.condition << ";";
            out << " Parts: " << stat.num_parts_after;
            if (i)
                out << '/' << index_stats[i - 1].num_parts_after;
            out << ";";
            out << " Granules: " << stat.num_granules_after;
            if (i)
                out << '/' << index_stats[i - 1].num_granules_after;
            out << ";";
            index_desc.name_and_detail.emplace_back(indexTypeToString(stat.type), out.str());
        }
        index_desc.description = "Indexes";
        attribute_descriptions.emplace(index_desc.description, std::move(index_desc));
    }

    RuntimeAttributeDescription parts_desc;
    String selected_parts_info = fmt::format(
        "Selected {}/{} parts by partition key, {} parts by primary key, {}/{} marks by primary key, {} marks to read from {} ranges",
        result.parts_before_pk,
        result.total_parts,
        result.selected_parts,
        result.selected_marks_pk,
        result.total_marks_pk,
        result.selected_marks,
        result.selected_ranges);
    parts_desc.description = selected_parts_info;
    attribute_descriptions.emplace("SelectParts", std::move(parts_desc));
}

bool MergeTreeDataSelectAnalysisResult::error() const
{
    return std::holds_alternative<std::exception_ptr>(result);
}

size_t MergeTreeDataSelectAnalysisResult::marks() const
{
    if (std::holds_alternative<std::exception_ptr>(result))
        std::rethrow_exception(std::get<std::exception_ptr>(result));

    const auto & index_stats = std::get<ReadFromMergeTree::AnalysisResult>(result).index_stats;
    if (index_stats.empty())
        return 0;
    return index_stats.back().num_granules_after;
}

}
