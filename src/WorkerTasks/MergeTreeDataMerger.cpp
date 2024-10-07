/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <WorkerTasks/MergeTreeDataMerger.h>

#include <Common/ProfileEvents.h>
#include <Common/filesystemHelpers.h>
#include <DataTypes/ObjectUtils.h>
#include <DataStreams/ColumnGathererStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/MapHelpers.h>
#include <Processors/Executors/PipelineExecutingBlockInputStream.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/GraphiteRollupSortedTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <WorkerTasks/CnchMergePrefetcher.h>
#include <WorkerTasks/ManipulationTaskParams.h>
#include <Common/ProfileEvents.h>
#include <Common/filesystemHelpers.h>
#include <Storages/CnchTablePartitionMetrics.h>

namespace ProfileEvents
{
    extern const Event CloudMergeStarted;
    extern const Event CloudMergeEnded;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ABORTED;
    extern const int DIRECTORY_ALREADY_EXISTS;
}

namespace
{
    constexpr auto DISK_USAGE_COEFFICIENT_TO_RESERVE = 1.1;

    // should we consider projections that managed by the current part？
    size_t estimateNeededDiskSpace(const MergeTreeDataPartsVector & source_parts)
    {
        size_t res = 0;
        for (const auto & part : source_parts)
            res += part->getBytesOnDisk();
        return static_cast<size_t>(res * DISK_USAGE_COEFFICIENT_TO_RESERVE);
    }

    // calculated the total_rows_count of projections that managed by the current part
    size_t estimateTotalRowsCount(const MergeTreeDataPartsVector & source_parts)
    {
        size_t res = 0;
        for (const auto & part : source_parts)
//            res += part->rows_count;
            res += part->index_granularity.getTotalRows();
        return res;
    }

    String toDebugString(const MergeTreeDataPartsVector & parts)
    {
        WriteBufferFromOwnString out;
        for (const auto & part : parts)
        {
            out << part->name;
            out << ' ';
        }
        return std::move(out.str());
    }

    SortDescription getSortDescription(const Names & sort_columns, const Block & header)
    {
        SortDescription sort_description;
        size_t sort_columns_size = sort_columns.size();
        sort_description.reserve(sort_columns_size);

        for (size_t i = 0; i < sort_columns_size; ++i)
            sort_description.emplace_back(header.getPositionByName(sort_columns[i]), 1, 1);
        return sort_description;
    }
}

MergeTreeDataMerger::MergeTreeDataMerger(
    MergeTreeMetaBase & data_,
    const ManipulationTaskParams & params_,
    ContextPtr context_,
    ManipulationListElement * manipulation_entry_,
    CheckCancelCallback check_cancel_,
    bool build_rowid_mappings_)
    : data(data_)
    , data_settings(data.getSettings())
    , params(params_)
    , context(context_)
    , task_manipulation_entry(manipulation_entry_)
    , check_cancel(std::move(check_cancel_))
    , build_rowid_mappings(build_rowid_mappings_)
    , rowid_mappings(params.source_data_parts.size())
    , log(getLogger(data.getLogName() + " (Merger)"))
{
    if (build_rowid_mappings && data.merging_params.mode != MergeTreeMetaBase::MergingParams::Ordinary)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Rowid mapping is only supported for Ordinal mode, got {}", data.merging_params.getModeName());
    space_reservation = data.reserveSpace(estimateNeededDiskSpace(params.source_data_parts), IStorage::StorageLocation::AUXILITY);
}

MergeTreeDataMerger::~MergeTreeDataMerger()
{
    if (prefetcher)
        prefetcher.reset();
}

void MergeTreeDataMerger::prepareColumnNamesAndTypes(
    const StorageSnapshotPtr & storage_snapshot,
    const MergeTreeMetaBase::MergingParams & merging_params,
    Names & all_column_names,
    Names & gathering_column_names,
    Names & merging_column_names,
    NamesAndTypesList & storage_columns,
    NamesAndTypesList & gathering_columns,
    NamesAndTypesList & merging_columns)
{
    auto metadata_snapshot = storage_snapshot->metadata;
    all_column_names = metadata_snapshot->getColumns().getNamesOfPhysical();
    storage_columns = metadata_snapshot->getColumns().getAllPhysical();

    extendObjectColumns(storage_columns, storage_snapshot->object_columns, false);

    Names sort_key_columns_vec = metadata_snapshot->getSortingKey().expression->getRequiredColumns();
    std::set<String> key_columns(sort_key_columns_vec.cbegin(), sort_key_columns_vec.cend());
    for (const auto & index : metadata_snapshot->getSecondaryIndices())
    {
        Names index_columns_vec = index.expression->getRequiredColumns();
        std::copy(index_columns_vec.cbegin(), index_columns_vec.cend(), std::inserter(key_columns, key_columns.end()));
    }

    for (const auto & projection : metadata_snapshot->getProjections())
    {
        Names projection_columns_vec = projection.required_columns;
        std::copy(projection_columns_vec.cbegin(), projection_columns_vec.cend(), std::inserter(key_columns, key_columns.end()));
    }

    /// Force unique key columns and extra column for Unique mode,
    /// otherwise MergedBlockOutputStream won't have the required columns to generate unique key index file.
    if (metadata_snapshot->hasUniqueKey())
    {
        auto unique_key_expr = metadata_snapshot->getUniqueKey().expression;
        if (!unique_key_expr)
            throw Exception("Missing unique key expression for Unique mode", ErrorCodes::LOGICAL_ERROR);

        Names index_columns_vec = unique_key_expr->getRequiredColumns();
        std::copy(index_columns_vec.cbegin(), index_columns_vec.cend(), std::inserter(key_columns, key_columns.end()));

        /// also need version column when building unique key index file
        if (merging_params.hasExplicitVersionColumn())
            key_columns.insert(merging_params.version_column);
    }

    /// Force sign column for Collapsing mode
    if (merging_params.mode == MergeTreeMetaBase::MergingParams::Collapsing)
        key_columns.emplace(merging_params.sign_column);

    /// Force version column for Replacing mode
    if (merging_params.mode == MergeTreeMetaBase::MergingParams::Replacing)
        key_columns.emplace(merging_params.version_column);

    /// Force sign column for VersionedCollapsing mode. Version is already in primary key.
    if (merging_params.mode == MergeTreeMetaBase::MergingParams::VersionedCollapsing)
        key_columns.emplace(merging_params.sign_column);

    /// Force to merge at least one column in case of empty key
    if (key_columns.empty())
        key_columns.emplace(storage_columns.front().name);

    /// TODO: also force "summing" and "aggregating" columns to make Horizontal merge only for such columns

    for (const auto & column : storage_columns)
    {
        if (key_columns.count(column.name))
        {
            merging_columns.emplace_back(column);
            merging_column_names.emplace_back(column.name);
        }
        else
        {
            gathering_columns.emplace_back(column);
            gathering_column_names.emplace_back(column.name);
        }
    }
}

static auto isMergePart(const MergeTreeDataPartPtr & part) -> bool
{
    return !part->get_deleted() && part->get_info().hint_mutation == 0 && part->get_info().level != 0;
}

MergeTreeMutableDataPartPtr MergeTreeDataMerger::prepareNewParts(
    const MergeTreeDataPartsVector & source_data_parts, const IMergeTreeDataPart * parent_part, const NamesAndTypesList & storage_columns)
{
    String TMP_PREFIX = "tmp_merge_";
    const auto & new_part_name = parent_part ? source_data_parts.front()->name : params.new_part_names.front();
    String new_part_tmp_path = parent_part ? (new_part_name + ".proj") : (TMP_PREFIX + toString(UInt64(context->getCurrentCnchStartTime())) + '-' + new_part_name);

    DiskPtr disk = space_reservation->getDisk();
    /// Check directory
    if (!parent_part)
    {
        String new_part_tmp_rel_path = data.getRelativeDataPath(IStorage::StorageLocation::AUXILITY) + "/" + new_part_tmp_path;
        if (disk->exists(new_part_tmp_rel_path))
            throw Exception("Directory " + fullPath(disk, new_part_tmp_rel_path) + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);
        disk->createDirectories(new_part_tmp_rel_path); /// TODO: could we remove it ?
    }

    /// Create new data part object
    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + new_part_name, disk, 0);
    auto part_info = parent_part ? MergeTreePartInfo{"all", 0, 0, 0} : MergeTreePartInfo::fromPartName(new_part_name, data.format_version);

    MergeTreeMutableDataPartPtr new_data_part = std::make_shared<MergeTreeDataPartWide>(
        data, new_part_name, part_info, single_disk_volume, new_part_tmp_path, parent_part, IStorage::StorageLocation::AUXILITY);

    /// Common fields
    /// TODO uuid
    new_data_part->setColumns(storage_columns);
    new_data_part->partition.assign(source_data_parts.front()->partition);
    new_data_part->is_temp = parent_part == nullptr;

    /// TODO: support TTL ?

    /// CNCH fields
    // Should projection parts use the same columns_commit_time with the parent_part
    new_data_part->columns_commit_time = params.columns_commit_time;
    /// use max_mutation_commit_time
    /// because we will remove outdated columns, indices, bitmap indices, skip indices if need when merge new parts.
    TxnTimestamp mutation_commit_time = 0;
    for (const auto & part : source_data_parts)
        mutation_commit_time = std::max(mutation_commit_time, part->mutation_commit_time);
    new_data_part->mutation_commit_time = mutation_commit_time;

    TxnTimestamp last_modification_time = 0;
    for (const MergeTreeDataPartPtr & part : source_data_parts)
    {
        if (isMergePart(part)) {
            last_modification_time = std::max(last_modification_time, part->last_modification_time);
        } else {
            last_modification_time
                = std::max(last_modification_time, part->last_modification_time ? part->last_modification_time : part->commit_time);
        }
    }
    new_data_part->last_modification_time = last_modification_time;

    if (!parent_part && params.is_bucket_table)
        new_data_part->bucket_number = source_data_parts.front()->bucket_number;

    LOG_DEBUG(log, "Merging {} parts: {} into {}", source_data_parts.size(), toDebugString(source_data_parts), new_data_part->relative_path);

    return new_data_part;
}

MergeAlgorithm MergeTreeDataMerger::chooseMergeAlgorithm(
    const MergeTreeDataPartsVector & source_data_parts,
    const NamesAndTypesList & gathering_columns,
    const MergeTreeMetaBase::MergingParams & merging_params,
    size_t sum_input_rows_upper_bound)
{
    if (data_settings->enable_vertical_merge_algorithm == 0)
    {
        return MergeAlgorithm::Horizontal;
    }

    bool is_supported_storage = merging_params.mode == MergeTreeMetaBase::MergingParams::Ordinary
        || merging_params.mode == MergeTreeMetaBase::MergingParams::Collapsing
        || merging_params.mode == MergeTreeMetaBase::MergingParams::Replacing
        || merging_params.mode == MergeTreeMetaBase::MergingParams::VersionedCollapsing;

    // return MergeAlgorithm::Vertical if there is a map key since we cannot know how many keys in this column.
    // If there are too many keys, it may exhaust all file handles.
    if (is_supported_storage)
    {
        for (const auto & column : gathering_columns)
        {
            if (column.type->isByteMap() || column.type->lowCardinality() || isBitmap64(column.type))
            {
                return MergeAlgorithm::Vertical;
            }
        }
    }

    bool enough_ordinary_cols = gathering_columns.size() >= data_settings->vertical_merge_algorithm_min_columns_to_activate;

    bool enough_total_rows = sum_input_rows_upper_bound >= data_settings->vertical_merge_algorithm_min_rows_to_activate;

    bool no_parts_overflow = source_data_parts.size() <= RowSourcePart::MAX_PARTS;

    auto merge_alg = (is_supported_storage && enough_total_rows && enough_ordinary_cols && no_parts_overflow)
                        ? MergeAlgorithm::Vertical : MergeAlgorithm::Horizontal;

    LOG_DEBUG(log, "Selected MergeAlgorithm: {}", toString(merge_alg));

    return merge_alg;
}

MergeTreeMutableDataPartPtr MergeTreeDataMerger::mergePartsToTemporaryPartImpl(
    const MergeTreeDataPartsVector & source_data_parts,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeMetaBase::MergingParams & merging_params,
    ManipulationListElement * manipulation_entry,
    const IMergeTreeDataPart * parent_part)
{
    /// prepare gathering and merging columns and names
    Names all_column_names;
    Names gathering_column_names;
    Names merging_column_names;
    NamesAndTypesList storage_columns;
    NamesAndTypesList gathering_columns;
    NamesAndTypesList merging_columns;

    auto storage_snapshot = data.getStorageSnapshot(metadata_snapshot, context);

    prepareColumnNamesAndTypes(
        storage_snapshot,
        merging_params,
        all_column_names,
        gathering_column_names,
        merging_column_names,
        storage_columns,
        gathering_columns,
        merging_columns);

    /// prepare new parts
    MergeTreeMutableDataPartPtr new_data_part = prepareNewParts(source_data_parts, parent_part, storage_columns);

    // choose merge algorithm
    // in ce version: projection and normal use the same sum_input_rows_upper_bound in merge_entry
    // shall we calculate the total rows_count for projection by summing their's rows_count ?
    size_t sum_input_rows_upper_bound = parent_part ? estimateTotalRowsCount(source_data_parts) : manipulation_entry->total_rows_count;
    MergeAlgorithm merge_alg = chooseMergeAlgorithm(source_data_parts, gathering_columns, merging_params, sum_input_rows_upper_bound);

    // choose compression codec
    // shall we specially calculate the compression_codec for projection using the corresponding proj's params?
    CompressionCodecPtr compression_codec = context->chooseCompressionCodec(
        manipulation_entry->total_size_bytes_compressed,
        static_cast<double>(manipulation_entry->total_size_bytes_compressed) / data.getTotalActiveSizeInBytes());

    /// prepare for vertical merge
    DiskPtr tmp_disk;
    std::unique_ptr<Poco::TemporaryFile> rows_sources_file;
    std::unique_ptr<WriteBuffer> rows_sources_uncompressed_write_buf;
    std::unique_ptr<WriteBuffer> rows_sources_write_buf;
    std::optional<ColumnSizeEstimator> column_sizes;

    if (merge_alg == MergeAlgorithm::Vertical)
    {
        ColumnSizeEstimator::ColumnToSize merged_column_to_size;
        /// calc map { column -> size }
        for (const auto & part : source_data_parts)
            part->accumulateColumnSizes(merged_column_to_size);

        tmp_disk = context->getTemporaryVolume()->getDisk();
        rows_sources_file = createTemporaryFile(tmp_disk->getPath());
        rows_sources_uncompressed_write_buf = std::make_unique<WriteBufferFromFile>(rows_sources_file->path());
        rows_sources_write_buf = std::make_unique<CompressedWriteBuffer>(*rows_sources_uncompressed_write_buf);

        /// Collect implicit columns of byte map
        NamesAndTypesList old_gathering_columns;
        std::swap(old_gathering_columns, gathering_columns);
        for (auto & column : old_gathering_columns)
        {
            if (column.type->isByteMap())
            {
                /// Implicit column names in merged_column_to_size is unescaped, so column.name need keep unescaped either.
                auto [curr, end] = getFileRangeFromOrderedFilesByPrefix(getMapKeyPrefix(column.name), merged_column_to_size);
                for (; curr != end; ++curr)
                {
                    /// Treat map implicit columns as ordinary columns, need add to ColumnSizeEstimator to get the right total size.
                    gathering_column_names.emplace_back(curr->first);
                    gathering_columns.emplace_back(
                        curr->first, dynamic_cast<const DataTypeMap *>(column.type.get())->getValueTypeForImplicitColumn());
                }
            }
            else
            {
                gathering_columns.push_back(column);
            }
        }
        gathering_columns.sort(); /// It gains better performance if gathering by sorted columns
        column_sizes = ColumnSizeEstimator(merged_column_to_size, merging_column_names, gathering_column_names);
    }
    else
    {
        merging_columns = storage_columns;
        merging_column_names = all_column_names;
        gathering_columns.clear();
        gathering_column_names.clear();
    }

    MergeStageProgress horizontal_stage_progress(column_sizes ? column_sizes->keyColumnsWeight() : 1.0);

    // create source input
    Pipes pipes;
    UInt64 watch_prev_elapsed = 0;
    // since we're reading from pre-fetched files, data will be mostly in page cache
    const bool read_with_direct_io = false;

    // create merge prefetcher if necessary
    if (context->getSettingsRef().cnch_enable_merge_prefetch)
    {
        if (std::any_of(gathering_columns.cbegin(), gathering_columns.cend(), [](auto & c) { return isBitEngineDataType(c.type); }))
        {
            LOG_DEBUG(log, "Prefetcher is disabled as there is some BitEngine column in gathering_columns");
        }
        else if (data.canUseAdaptiveGranularity())
        {
            LOG_DEBUG(log, "Prefetcher is disabled as adaptive_granularity is enabled for storage.");
        }
        else
        {
            prefetcher = std::make_unique<CnchMergePrefetcher>(*context, data, params.task_id);
            for (const auto & part : params.source_data_parts)
                prefetcher->submitDataPart(part, merging_columns, gathering_columns);
        }
    }

    for (const auto & part : source_data_parts)
    {
        CnchMergePrefetcher::PartFutureFiles* future_files = prefetcher ? prefetcher->tryGetFutureFiles(part->name) : nullptr;

        auto input = std::make_unique<MergeTreeSequentialSource>(
            data,
            storage_snapshot,
            part,
            merging_column_names,
            read_with_direct_io,
            /*take_column_types_from_storage*/ true,
            /*quiet*/ false,
            future_files);
        input->setProgressCallback(ManipulationProgressCallback(manipulation_entry, watch_prev_elapsed, horizontal_stage_progress));

        Pipe pipe(std::move(input));

        if (metadata_snapshot->hasSortingKey())
        {
            pipe.addSimpleTransform([metadata_snapshot](const Block & header) {
                return std::make_shared<ExpressionTransform>(header, metadata_snapshot->getSortingKey().expression);
            });
        }

        pipes.emplace_back(std::move(pipe));
    }

    // create merged stream and output stream
    auto header = pipes.front().getHeader();
    auto sort_description = getSortDescription(metadata_snapshot->getSortingKeyColumns(), header);

    /// The order of the streams is important: when the key is matched, the elements go in the order of the source stream number.
    /// In the merged part, the lines with the same key must be in the ascending order of the identifier of original part,
    ///  that is going in insertion order.
    ProcessorPtr merged_transform;

    /// If merge is vertical we cannot calculate it
    bool blocks_are_granules_size = (merge_alg == MergeAlgorithm::Vertical && !isCompactPart(new_data_part));

    MergingSortedAlgorithm::PartIdMappingCallback row_mapping_cb;
    if (!parent_part && build_rowid_mappings)
    {
        row_mapping_cb = [&](size_t part_index, size_t nrows) {
            for (size_t i = 0; i < nrows; ++i)
            {
                rowid_mappings[part_index].push_back(output_rowid++);
            }
        };
    }

    UInt64 merge_block_size = data_settings->merge_max_block_size;

    switch (merging_params.mode)
    {
        case MergeTreeMetaBase::MergingParams::Ordinary:
            merged_transform = std::make_unique<MergingSortedTransform>(
                header,
                pipes.size(),
                sort_description,
                merge_block_size,
                0, /// limit
                rows_sources_write_buf.get(),
                true, /// quiet
                blocks_are_granules_size,
                true, /// have_all_inputs
                row_mapping_cb);
            break;

        case MergeTreeMetaBase::MergingParams::Collapsing:
            merged_transform = std::make_unique<CollapsingSortedTransform>(
                header,
                pipes.size(),
                sort_description,
                merging_params.sign_column,
                false,
                merge_block_size,
                rows_sources_write_buf.get(),
                blocks_are_granules_size);
            break;

        case MergeTreeMetaBase::MergingParams::Summing:
            merged_transform = std::make_unique<SummingSortedTransform>(
                header,
                pipes.size(),
                sort_description,
                merging_params.columns_to_sum,
                metadata_snapshot->getPartitionKey().column_names,
                merge_block_size);
            break;

        case MergeTreeMetaBase::MergingParams::Aggregating:
            merged_transform = std::make_unique<AggregatingSortedTransform>(header, pipes.size(), sort_description, merge_block_size);
            break;

        case MergeTreeMetaBase::MergingParams::Replacing:
            merged_transform = std::make_unique<ReplacingSortedTransform>(
                header,
                pipes.size(),
                sort_description,
                merging_params.version_column,
                merge_block_size,
                rows_sources_write_buf.get(),
                blocks_are_granules_size);
            break;

        case MergeTreeMetaBase::MergingParams::Graphite:
            merged_transform = std::make_unique<GraphiteRollupSortedTransform>(
                header, pipes.size(), sort_description, merge_block_size, merging_params.graphite_params, time(nullptr));
            break;

        case MergeTreeMetaBase::MergingParams::VersionedCollapsing:
            merged_transform = std::make_unique<VersionedCollapsingTransform>(
                header,
                pipes.size(),
                sort_description,
                merging_params.sign_column,
                merge_block_size,
                rows_sources_write_buf.get(),
                blocks_are_granules_size);
            break;
    }

    QueryPipeline pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes)));
    pipeline.addTransform(std::move(merged_transform));
    pipeline.setMaxThreads(1);
    BlockInputStreamPtr merged_stream = std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline));

    // TODO:
    // if (deduplicate)
    //     merged_stream = std::make_shared<DistinctSortedBlockInputStream>(merged_stream, sort_description, SizeLimits(), 0 /*limit_hint*/, deduplicate_by_columns);

    // if (need_remove_expired_values)
    // {
    //     LOG_DEBUG(log, "Outdated rows found in source parts, TTLs processing enabled for merge");
    //     merged_stream = std::make_shared<TTLBlockInputStream>(merged_stream, data, metadata_snapshot, new_data_part, time_of_merge, force_ttl);
    // }

    if (metadata_snapshot->hasSecondaryIndices())
    {
        const auto & indices = metadata_snapshot->getSecondaryIndices();
        merged_stream = std::make_shared<ExpressionBlockInputStream>(
            merged_stream, indices.getSingleExpressionForIndices(metadata_snapshot->getColumns(), data.getContext()));
        merged_stream = std::make_shared<MaterializingBlockInputStream>(merged_stream);
    }

    const auto & index_factory = MergeTreeIndexFactory::instance();
    auto to = std::make_unique<MergedBlockOutputStream>(
        new_data_part,
        metadata_snapshot,
        merging_columns,
        index_factory.getMany(metadata_snapshot->getSecondaryIndices()),
        compression_codec,
        blocks_are_granules_size,
        context->getSettingsRef().optimize_map_column_serialization);

    // copy data from source parts to new part
    merged_stream->readPrefix();
    to->writePrefix();

    size_t rows_written = 0;
    const size_t initial_reservation = space_reservation ? space_reservation->getSize() : 0;

    Block block;
    while (!check_cancel() && (block = merged_stream->read()))
    {
        rows_written += block.rows();

        to->write(block);

        manipulation_entry->rows_written = merged_stream->getProfileInfo().rows;
        manipulation_entry->bytes_written_uncompressed = merged_stream->getProfileInfo().bytes;

        /// Reservation updates is not performed yet, during the merge it may lead to higher free space requirements
        if (space_reservation && sum_input_rows_upper_bound)
        {
            /// The same progress from manipulation_entry could be used for both algorithms (it should be more accurate)
            /// But now we are using inaccurate row-based estimation in Horizontal case for backward compatibility
            Float64 progress = (merge_alg == MergeAlgorithm::Horizontal)
                ? std::min(1., 1. * rows_written / sum_input_rows_upper_bound)
                : std::min(1., manipulation_entry->progress.load(std::memory_order_relaxed));

            space_reservation->update(static_cast<size_t>((1. - progress) * initial_reservation));
        }
    }

    merged_stream->readSuffix();
    merged_stream.reset();

    if (check_cancel())
        throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

    if (!parent_part && build_rowid_mappings && output_rowid != rows_written)
        throw Exception(
            "Written " + toString(rows_written) + " rows, but output rowid is " + toString(output_rowid), ErrorCodes::LOGICAL_ERROR);


    // gather columns
    MergeTreeDataPartChecksums additional_column_checksums;
    if (merge_alg == MergeAlgorithm::Vertical)
    {
        /// Set horizontal stage progress
        manipulation_entry->columns_written.store(merging_column_names.size(), std::memory_order_relaxed);
        manipulation_entry->progress.store(column_sizes->keyColumnsWeight(), std::memory_order_relaxed);

        /// Prepare to gather
        rows_sources_write_buf->next();
        rows_sources_uncompressed_write_buf->next();
        /// Ensure data has written to disk.
        rows_sources_uncompressed_write_buf->finalize();

        size_t sum_input_rows_exact = manipulation_entry->rows_read;
        size_t rows_sources_count = rows_sources_write_buf->count();
        /// In special case, when there is only one source part, and no rows were skipped, we may have
        /// skipped writing rows_sources file. Otherwise rows_sources_count must be equal to the total
        /// number of input rows.
        if ((rows_sources_count > 0 || source_data_parts.size() > 1) && sum_input_rows_exact != rows_sources_count)
            throw Exception(
                "Number of rows in source parts (" + toString(sum_input_rows_exact)
                    + ") differs from number of bytes written to rows_sources file (" + toString(rows_sources_count) + "). It is a bug.",
                ErrorCodes::LOGICAL_ERROR);

        auto rows_sources_read_buf
            = std::make_unique<CompressedReadBufferFromFile>(tmp_disk->readFile(fileName(rows_sources_file->path())));
        auto written_offset_columns = std::make_unique<IMergedBlockOutputStream::WrittenOffsetColumns>();

        auto writer_settings = std::make_unique<MergeTreeWriterSettings>(
            new_data_part->storage.getContext()->getSettings(),
            new_data_part->storage.getSettings(),
            /*can_use_adaptive_granularity = */ new_data_part->index_granularity_info.is_adaptive,
            /* rewrite_primary_key = */ false,
            /*blocks_are_granules_size = */ false,
            context->getSettingsRef().optimize_map_column_serialization,
            /* enable_disk_based_key_index_ = */ false);

        auto gather_column = [&](const String & column_name, BitEngineReadType bitengine_read_type = BitEngineReadType::ONLY_SOURCE) {
            Float64 progress_before = manipulation_entry->progress.load(std::memory_order_relaxed);
            Float64 column_weight = column_sizes->columnWeight(column_name);
            MergeStageProgress column_progress(progress_before, column_weight);
            LOG_TRACE(log, "Gather column {} weight {} in progress {}", column_name, column_weight, progress_before);

            /// Prepare input streams
            BlockInputStreams column_part_streams(source_data_parts.size());

            auto rt_ctx = std::make_shared<MergeTreeSequentialSource::RuntimeContext>();

            for (size_t part_num = 0; part_num < source_data_parts.size(); ++part_num)
            {
                CnchMergePrefetcher::PartFutureFiles * future_files = prefetcher ? prefetcher->tryGetFutureFiles(source_data_parts[part_num]->name) : nullptr;

                auto column_part_source = std::make_shared<MergeTreeSequentialSource>(
                    data,
                    storage_snapshot,
                    source_data_parts[part_num],
                    Names{column_name},
                    read_with_direct_io,
                    /*take_column_types_from_storage*/ bitengine_read_type
                        == BitEngineReadType::ONLY_SOURCE, /// default is true, in bitengine may set false,
                    /*quiet*/ false,
                    future_files,
                    bitengine_read_type,
                    /*block_preferred_size_bytes_*/ data_settings->merge_max_block_size_bytes,
                    rt_ctx);

                column_part_source->setProgressCallback(
                    ManipulationProgressCallback(manipulation_entry, watch_prev_elapsed, column_progress));

                QueryPipeline column_part_pipeline;
                column_part_pipeline.init(Pipe(std::move(column_part_source)));
                column_part_pipeline.setMaxThreads(1);

                column_part_streams[part_num] = std::make_shared<PipelineExecutingBlockInputStream>(std::move(column_part_pipeline));
            }

            rows_sources_read_buf->seek(0, 0);

            ColumnGathererStream column_gathered_stream(
                column_name,
                column_part_streams,
                *rows_sources_read_buf,
                /*block_preferred_size_rows_ =*/ data_settings->merge_max_block_size,
                /*block_preferred_size_bytes_ =*/ data_settings->merge_max_block_size_bytes,
                context->getSettingsRef().enable_low_cardinality_merge_new_algo,
                context->getSettingsRef().low_cardinality_distinct_threshold);

            /// Prepare output stream
            MergedColumnOnlyOutputStream column_to(
                new_data_part,
                metadata_snapshot,
                *writer_settings,
                column_gathered_stream.getHeader(),
                compression_codec,
                std::vector<MergeTreeIndexPtr>{},
                written_offset_columns.get(),
                to->getIndexGranularity(),
                true /// is_merge
            );
            column_to.writePrefix();

            /// Do gathering
            size_t column_elems_written = 0;

            while (!check_cancel() && (block = column_gathered_stream.read()))
            {
                /// TODO: support lc

                column_elems_written += block.rows();
                column_to.write(block);

                if (space_reservation)
                {
                    Float64 progress = std::min(1., manipulation_entry->progress.load(std::memory_order_relaxed));

                    space_reservation->update(static_cast<size_t>((1. - progress) * initial_reservation));
                }
            }

            if (check_cancel())
                throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

            if (rows_written != column_elems_written)
            {
                throw Exception(
                    "Written " + toString(column_elems_written) + " elements of column " + column_name + ", but " + toString(rows_written)
                        + " rows of PK columns",
                    ErrorCodes::LOGICAL_ERROR);
            }

            column_gathered_stream.readSuffix();
            auto changed_checksums = column_to.writeSuffixAndGetChecksums(new_data_part, additional_column_checksums);
            additional_column_checksums.add(std::move(changed_checksums));

            /// Update profiles and progress
            manipulation_entry->columns_written.fetch_add(1, std::memory_order_relaxed);
            manipulation_entry->bytes_written_uncompressed.fetch_add(
                column_gathered_stream.getProfileInfo().bytes, std::memory_order_relaxed);
            manipulation_entry->progress.store(progress_before + column_weight, std::memory_order_relaxed);
        };

        for (auto & [column_name, column_type] : gathering_columns)
        {
            if (column_type->isByteMap())
                continue;

            gather_column(column_name);
        }

        LOG_DEBUG(log, "Gathered {} columns in vertical merge, read {} rows for each column, part name is {}", gathering_columns.size(), rows_written, new_data_part->name);
    }

    for (const auto & part : source_data_parts)
        new_data_part->minmax_idx.merge(part->minmax_idx);

       /// Print overall profiling info. NOTE: it may duplicates previous messages
    {
        double elapsed_seconds = manipulation_entry->watch.elapsedSeconds();
        LOG_DEBUG(
            log,
            "Merge sorted {} rows, containing {} columns ({} merged, {} gathered) in {} sec., {} rows/sec., {}/sec.",
            manipulation_entry->rows_read,
            all_column_names.size(),
            merging_column_names.size(),
            gathering_columns.size(),
            elapsed_seconds,
            manipulation_entry->rows_read / elapsed_seconds,
            ReadableSize(manipulation_entry->bytes_read_uncompressed / elapsed_seconds));
    }

    for (const auto & projection : metadata_snapshot->getProjections())
    {
        MergeTreeData::DataPartsVector projection_parts;
        for (const auto & part : source_data_parts)
        {
            auto it = part->getProjectionParts().find(projection.name);
            if (it != part->getProjectionParts().end())
                projection_parts.push_back(it->second);
        }
        if (projection_parts.size() < source_data_parts.size())
        {
            LOG_DEBUG(log, "Projection {} is not merged because some parts don't have it", projection.name);
            continue;
        }

        LOG_DEBUG(
            log,
            "Selected {} projection_parts from {} to {}",
            projection_parts.size(),
            projection_parts.front()->name,
            projection_parts.back()->name);

        MergeTreeMetaBase::MergingParams projection_merging_params;
        projection_merging_params.mode = MergeTreeMetaBase::MergingParams::Ordinary;
        if (projection.type == ProjectionDescription::Type::Aggregate)
            projection_merging_params.mode = MergeTreeMetaBase::MergingParams::Aggregating;

        // we use a new merge_entry for projection merge
        // Here, projection_manipulation_entry use the parent_parent's params to initilize itself, which is not accurate.
        // THe most accurate way is to construct a new params by the projection_parts.
        ManipulationListElement projection_manipulation_entry(params, false, context);

        auto merged_projection_part = mergePartsToTemporaryPartImpl(
            std::move(projection_parts), projection.metadata, projection_merging_params, &projection_manipulation_entry, new_data_part.get());
        new_data_part->addProjectionPart(projection.name, std::move(merged_projection_part));
    }

    // finalize the writing
    if (merge_alg != MergeAlgorithm::Vertical)
        to->writeSuffixAndFinalizePart(new_data_part, false, nullptr, &additional_column_checksums);
    else
        to->writeSuffixAndFinalizePart(new_data_part, false, &storage_columns, &additional_column_checksums);

    return new_data_part;
}

MergeTreeMutableDataPartPtr MergeTreeDataMerger::mergePartsToTemporaryPart()
{
    return mergePartsToTemporaryPartImpl(params.source_data_parts, data.getInMemoryMetadataPtr(), data.merging_params, task_manipulation_entry, nullptr);
}

}
