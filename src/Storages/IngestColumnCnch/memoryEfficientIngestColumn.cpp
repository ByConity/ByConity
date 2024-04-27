#include <Storages/IngestColumnCnch/memoryEfficientIngestColumn.h>
#include <Storages/IngestColumnCnch/IngestColumnBlockInputStream.h>
#include <Storages/IngestColumnCnch/IngestColumnHelper.h>
#include <Storages/IngestColumnCnch/DefaultBlockInputStream.h>
#include <Storages/StorageCloudMergeTree.h>
#include <CloudServices/CnchPartsHelper.h>
//#include <MergeTreeCommon/MergeMutateCommon.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <CloudServices/CnchDataWriter.h>
#include <Common/HashTable/HashMap.h>
#include <DataTypes/MapHelpers.h>
#include <string>
// for read part
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Processors/Executors/PipelineExecutingBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
    extern const int LOGICAL_ERROR;
}

using IngestColumn::KeyInfo;
using IngestColumn::PartMap;

MemoryEfficientIngestColumn::MemoryEfficientIngestColumn(IngestColumnBlockInputStream & stream_)
    : stream{stream_},
      context{stream_.context},
      settings{stream_.context->getSettingsRef()},
      target_cloud_merge_tree{*stream_.target_cloud_merge_tree},
      source_cloud_merge_tree{*stream_.source_cloud_merge_tree},
      target_storage_snapshot{stream_.target_storage_snapshot},
      source_storage_snapshot{stream_.source_storage_snapshot},
      visible_target_parts{stream_.getCurrentVisibleTargetParts()},
      visible_source_parts{stream_.getCurrentVisibleSourceParts()},
      number_of_threads_for_read_source_parts{std::min(settings.parallel_ingest_threads.value, stream_.visible_source_parts.size())},
      log{stream_.log},
      source_part_map{PartMap::buildPartMap(visible_source_parts)},
      target_part_map{PartMap::buildPartMap(visible_target_parts)}
{
    KeyInfo::checkNumberOfParts(visible_target_parts);
    KeyInfo::checkNumberOfParts(visible_source_parts);
}

void MemoryEfficientIngestColumn::execute()
{
    size_t source_rows_count = countRows(visible_source_parts);
    if (source_rows_count == 0)
        throw Exception("the number of source rows count is 0", ErrorCodes::LOGICAL_ERROR);

    size_t max_row = settings.memory_efficient_ingest_partition_max_key_count_in_memory;
    if (max_row == 0)
        throw Exception("setting memory_efficient_ingest_partition_max_key_count_in_memory is 0", ErrorCodes::LOGICAL_ERROR);

    size_t number_of_buckets = (source_rows_count / max_row) + 1;
    LOG_DEBUG(log, "add new part for ingest using {} buckets, for the source row count is {} and setting max_ingest_rows_size is {}", number_of_buckets, source_rows_count, settings.memory_efficient_ingest_partition_max_key_count_in_memory.toString());

    for (size_t i = 0; i < number_of_buckets; ++i)
    {
        addNewPartsForBucket(i, number_of_buckets);
    }

    updateTargetParts();
}

void MemoryEfficientIngestColumn::addNewPartsForBucket(size_t bucket_num,
    size_t number_of_buckets)
{
    std::vector<Arena> keys_pools(number_of_threads_for_read_source_parts);
    /// key-> flag that encrypt the info indicate whether key is exist in target data set, and which source_part it belong
    HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> source_key_map =
        buildHashTableForSourceData(keys_pools, bucket_num, number_of_buckets);

    probeHashMapWithTargetData(source_key_map, bucket_num, number_of_buckets);
    insertNewData(source_key_map, bucket_num, number_of_buckets);
}

void MemoryEfficientIngestColumn::probeHashMapWithTargetData(
    HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> & source_key_map,
    size_t bucket_num,
    size_t number_of_buckets)
{
    MergeTreeDataPartsVector target_parts = visible_target_parts;
    size_t num_threads = std::min(settings.parallel_ingest_threads.value, target_parts.size());
    Exceptions exceptions(num_threads);
    std::mutex read_parts_mutex;
    std::mutex source_key_map_and_index_mutex;
    int count = 0;
    std::atomic<bool> has_read_exception = false;

    auto probe_hash_table_func = [&]()
    {
        int idx = 0;
        {
            auto count_lock = std::lock_guard<std::mutex>(read_parts_mutex);
            idx = count;
            ++count;
        }

        LOG_TRACE(log, "readTargetParts thread idx: {}", idx);
        Arena temporary_keys_pool;

        while (1)
        {
            MergeTreeDataPartPtr part = nullptr;

            {
                auto read_lock = std::lock_guard<std::mutex>(read_parts_mutex);
                if (target_parts.empty() || has_read_exception)
                    return;

                part = target_parts.back();
                target_parts.pop_back();
            }

            if (!part)
                return;

            try
            {
                UInt32 part_id = target_part_map.getPartID(part);
                bool read_with_direct_io = settings.min_bytes_to_use_direct_io != 0 &&
                               part->bytes_on_disk >= settings.min_bytes_to_use_direct_io;

                auto source_input = std::make_unique<MergeTreeSequentialSource>(
                    target_cloud_merge_tree,
                    target_storage_snapshot,
                    part, stream.ordered_key_names, read_with_direct_io, true);

                QueryPipeline source_pipeline;
                source_pipeline.init(Pipe(std::move(source_input)));
                source_pipeline.setMaxThreads(1);
                BlockInputStreamPtr in = std::make_shared<PipelineExecutingBlockInputStream>(std::move(source_pipeline));

                in = std::make_shared<SquashingBlockInputStream>(in, settings.min_insert_block_size_rows, settings.min_insert_block_size_bytes);
                in->readPrefix();

                while (Block block = in->read())
                {
                    /// TODO if performance is slow, look at this
                    auto probe_lock = std::lock_guard<std::mutex>(source_key_map_and_index_mutex);
                    probeHashTableFromBlock(
                        part_id,
                        block,
                        stream.ordered_key_names,
                        source_key_map,
                        bucket_num,
                        temporary_keys_pool,
                        number_of_buckets,
                        target_to_source_part_index
                        );
                }

                in->readSuffix();
            }
            catch(...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                has_read_exception = true;
                exceptions[idx] = std::current_exception();
                return;
            }
        }
    };

    std::unique_ptr<ThreadPool> thread_pool = std::make_unique<ThreadPool>(num_threads);
    for (size_t i = 0; i < num_threads; i++)
        thread_pool->scheduleOrThrowOnError(probe_hash_table_func);

    thread_pool->wait();
    if (has_read_exception)
        rethrowFirstException(exceptions);
}

HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> MemoryEfficientIngestColumn::buildHashTableForSourceData(
    std::vector<Arena> & keys_pools,
    size_t bucket_num,
    size_t number_of_buckets)
{
    const size_t num_threads = number_of_threads_for_read_source_parts;
    MergeTreeDataPartsVector source_part = visible_source_parts;
    Exceptions exceptions(num_threads);
    std::vector<HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash>> thread_hashmaps(num_threads);
    std::mutex read_parts_mutex;
    int count = 0;
    std::atomic<bool> has_read_exception = false;

    auto read_part_func = [&]()
    {
        int thread_idx = 0;
        {
            auto count_lock = std::lock_guard<std::mutex>(read_parts_mutex);
            thread_idx = count;
            ++count;
        }

        LOG_TRACE(log, "buildHashTableFromSource thread thread_idx: {}", thread_idx);

        while (1)
        {
            MergeTreeDataPartPtr part = nullptr;

            {
                auto read_lock = std::lock_guard<std::mutex>(read_parts_mutex);
                if (source_part.empty() || has_read_exception)
                    return;

                part = source_part.back();
                source_part.pop_back();
            }

            if (!part)
                return;

            try
            {
                const UInt32 part_id = source_part_map.getPartID(part);
                const bool read_with_direct_io = settings.min_bytes_to_use_direct_io != 0 &&
                               part->bytes_on_disk >= settings.min_bytes_to_use_direct_io;
                auto source_input = std::make_unique<MergeTreeSequentialSource>(
                    source_cloud_merge_tree,
                    source_storage_snapshot,
                    part, stream.ordered_key_names, read_with_direct_io, true);
                QueryPipeline source_pipeline;
                source_pipeline.init(Pipe(std::move(source_input)));
                source_pipeline.setMaxThreads(1);
                BlockInputStreamPtr pipeline_input_stream = std::make_shared<PipelineExecutingBlockInputStream>(std::move(source_pipeline));

                pipeline_input_stream = std::make_shared<SquashingBlockInputStream>(pipeline_input_stream, settings.min_insert_block_size_rows, settings.min_insert_block_size_bytes);

                pipeline_input_stream->readPrefix();

                while (Block block = pipeline_input_stream->read())
                {
                    buildHashTableFromBlock(
                        part_id,
                        block,
                        stream.ordered_key_names,
                        thread_hashmaps[thread_idx],
                        bucket_num,
                        keys_pools[thread_idx],
                        number_of_buckets);
                }

                pipeline_input_stream->readSuffix();
            }
            catch(...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                has_read_exception = true;
                exceptions[thread_idx] = std::current_exception();
                return;
            }
        }
    };

    std::unique_ptr<ThreadPool> thread_pool = std::make_unique<ThreadPool>(num_threads);
    for (size_t i = 0; i < num_threads; i++)
        thread_pool->scheduleOrThrowOnError(read_part_func);

    thread_pool->wait();
    if (has_read_exception)
        rethrowFirstException(exceptions);

    return combineHashmaps(std::move(thread_hashmaps));
}

void MemoryEfficientIngestColumn::insertNewData(
    const HashMapWithSavedHash<StringRef, IngestColumn::KeyInfo, StringRefHash> & source_key_map,
    size_t bucket_num,
    size_t number_of_buckets)
{
    const size_t num_threads = number_of_threads_for_read_source_parts;
    MergeTreeDataPartsVector source_part = visible_source_parts;
    Exceptions exceptions(number_of_threads_for_read_source_parts);
    std::mutex read_parts_mutex;
    std::mutex new_part_output_mutex;
    int count = 0;
    std::atomic<bool> has_read_exception = false;
    const Names source_read_columns =
        IngestColumn::getColumnsFromSourceTableForInsertNewPart(stream.ordered_key_names,
            stream.ingest_column_names, source_storage_snapshot->metadata);

    BlockOutputStreamPtr new_part_output = target_cloud_merge_tree.write(ASTPtr(), target_storage_snapshot->metadata, context);
    new_part_output = std::make_shared<SquashingBlockOutputStream>(
        new_part_output, target_storage_snapshot->metadata->getSampleBlock(), settings.min_insert_block_size_rows, settings.min_insert_block_size_bytes);

    Block ingested_header;
    for (const auto & name : source_read_columns)
    {
        auto column_name = name;
        /// No need to add implicit map column
        if (isMapImplicitKey(name))
            column_name = parseMapNameFromImplicitColName(name);
        auto column = target_storage_snapshot->metadata->getColumns().getColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column_name);
        ingested_header.insertUnique(ColumnWithTypeAndName(column.type, column.name));
    }

    new_part_output = std::make_shared<AddingDefaultBlockOutputStream>(
        new_part_output, ingested_header, target_storage_snapshot->metadata->getColumns(), context);

    new_part_output->writePrefix();

    auto read_part_func = [&]()
    {
        int idx = 0;
        {
            auto count_lock = std::lock_guard<std::mutex>(read_parts_mutex);
            idx = count;
            ++count;
        }

        LOG_TRACE(log, "insertNewData thread idx: {}", idx);

        while (1)
        {
            MergeTreeDataPartPtr part = nullptr;

            {
                auto read_lock = std::lock_guard<std::mutex>(read_parts_mutex);
                if (source_part.empty() || has_read_exception)
                    return;

                part = source_part.back();
                source_part.pop_back();
            }

            if (!part)
                return;

            try
            {
                bool read_with_direct_io = settings.min_bytes_to_use_direct_io != 0 &&
                               part->bytes_on_disk >= settings.min_bytes_to_use_direct_io;

                auto source_input = std::make_unique<MergeTreeSequentialSource>(
                    source_cloud_merge_tree,
                    source_storage_snapshot,
                    part, source_read_columns, read_with_direct_io, true);
                QueryPipeline source_pipeline;
                source_pipeline.init(Pipe(std::move(source_input)));
                source_pipeline.setMaxThreads(1);
                BlockInputStreamPtr in = std::make_shared<PipelineExecutingBlockInputStream>(std::move(source_pipeline));

                in = std::make_shared<SquashingBlockInputStream>(in, settings.min_insert_block_size_rows, settings.min_insert_block_size_bytes);

                in->readPrefix();

                while (Block block = in->read())
                {
                    writeBlock(
                        block,
                        stream.ordered_key_names,
                        bucket_num,
                        source_key_map,
                        new_part_output_mutex,
                        number_of_buckets,
                        *new_part_output,
                        target_storage_snapshot->metadata,
                        log
                    );
                }

                in->readSuffix();
            }
            catch(...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                has_read_exception = true;
                exceptions[idx] = std::current_exception();
                return;
            }
        }
    };

    std::unique_ptr<ThreadPool> thread_pool =
        std::make_unique<ThreadPool>(num_threads);
    for (size_t i = 0; i < num_threads; i++)
        thread_pool->scheduleOrThrowOnError(read_part_func);

    thread_pool->wait();
    if (has_read_exception)
        rethrowFirstException(exceptions);

    new_part_output->writeSuffix();
}


void MemoryEfficientIngestColumn::updateTargetParts()
{
    MergeTreeDataPartsVector target_parts = visible_target_parts;
    size_t num_threads = std::min(settings.parallel_ingest_threads.value, target_parts.size());
    Exceptions exceptions(num_threads);
    std::mutex target_parts_mutex;
    IMutableMergeTreeDataPartsVector new_partial_parts;
    new_partial_parts.reserve(target_parts.size());
    std::mutex new_partitial_parts_mutex;
    Names all_columns = stream.ordered_key_names;
    std::copy(stream.ingest_column_names.begin(), stream.ingest_column_names.end(), std::back_inserter(all_columns));
    int count = 0;
    std::atomic<bool> has_exception = false;

    auto alter_part_func = [&]()
    {
        int idx = 0;
        {
            auto count_lock = std::lock_guard<std::mutex>(target_parts_mutex);
            idx = count;
            ++count;
        }

        LOG_TRACE(log, "updateTargetPart thread idx: {}", idx);

        while (1)
        {
            MergeTreeDataPartPtr part = nullptr;

            {
                auto read_lock = std::lock_guard<std::mutex>(target_parts_mutex);
                if (target_parts.empty() || has_exception)
                    return;

                part = target_parts.back();
                target_parts.pop_back();
            }

            if (!part)
                return;

            try
            {
                MergeTreeMutableDataPartPtr new_partial_part;
                const UInt32 target_part_id = target_part_map.getPartID(part);
                auto it = target_to_source_part_index.find(target_part_id);
                if (it == target_to_source_part_index.end())
                {
                    if (part->storage.getSettings()->ingest_default_column_value_if_not_provided)
                        new_partial_part = updateTargetPartWithoutSourcePart(part);
                    else
                        return;
                }
                else
                    new_partial_part = updateTargetPart(part, it->second, all_columns);

                {
                    auto new_partitial_parts_lock = std::lock_guard<std::mutex>(new_partitial_parts_mutex);
                    new_partial_parts.push_back(std::move(new_partial_part));
                }
            }
            catch(...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                has_exception = true;
                exceptions[idx] = std::current_exception();
                return;
            }
        }
    };

    std::unique_ptr<ThreadPool> thread_pool = std::make_unique<ThreadPool>(num_threads);
    for (size_t i = 0; i < num_threads; i++)
        thread_pool->scheduleOrThrowOnError(alter_part_func);

    thread_pool->wait();
    if (has_exception)
        rethrowFirstException(exceptions);

    CnchDataWriter cnch_writer(target_cloud_merge_tree, context, ManipulationType::Insert, context->getCurrentQueryId());
    cnch_writer.dumpAndCommitCnchParts(new_partial_parts);
}

MergeTreeMutableDataPartPtr MemoryEfficientIngestColumn::updateTargetPartWithoutSourcePart(MergeTreeDataPartPtr target_part)
{
    auto estimated_space_for_result = static_cast<size_t>(target_part->getBytesOnDisk());
    ReservationPtr reserved_space = target_cloud_merge_tree.reserveSpace(estimated_space_for_result,
        IStorage::StorageLocation::AUXILITY);

    auto new_partial_part = createEmptyTempPart(target_cloud_merge_tree, target_part, stream.ingest_column_names, reserved_space, context);

    BlockInputStreamPtr res_block_in =
        std::make_shared<DefaultBlockInputStream>(
            target_storage_snapshot->getSampleBlockForColumns(stream.ingest_column_names),
            target_part->rows_count, settings.min_insert_block_size_rows);

    updateTempPartWithData(new_partial_part, target_part, res_block_in, target_storage_snapshot->metadata);
    return new_partial_part;
}

MergeTreeMutableDataPartPtr MemoryEfficientIngestColumn::updateTargetPart(
    MergeTreeDataPartPtr target_part,
    const std::set<UInt32> & source_part_ids,
    const Names & all_columns)
{
    IngestColumn::TargetPartData target_part_data =
        readTargetPartForUpdate(target_part, all_columns);

    std::for_each(source_part_ids.begin(), source_part_ids.end(),
        [& ] (UInt32 source_part_id)
        {
            MergeTreeDataPartPtr source_part = source_part_map.getPart(source_part_id);
            updateTargetDataWithSourcePart(
                std::move(source_part),
                all_columns, target_part_data
            );
        }
    );

    //create new part
    auto estimated_space_for_result = static_cast<size_t>(target_part->getBytesOnDisk());
    ReservationPtr reserved_space = target_cloud_merge_tree.reserveSpace(estimated_space_for_result,
        IStorage::StorageLocation::AUXILITY);

    BlocksList res_block_list = makeBlockListFromUpdateData(target_part_data);
    target_part_data.reset(); // clear target_part_data to release memory
    auto new_partial_part = createEmptyTempPart(target_cloud_merge_tree, target_part, stream.ingest_column_names, reserved_space, context);

    BlockInputStreamPtr res_block_in = std::make_shared<BlocksListBlockInputStream>(std::move(res_block_list));

    updateTempPartWithData(new_partial_part, target_part, res_block_in, target_storage_snapshot->metadata);
    return new_partial_part;
}

IngestColumn::TargetPartData MemoryEfficientIngestColumn::readTargetPartForUpdate(
    MergeTreeDataPartPtr target_part,
    const Names & all_columns)
{
    const bool ingest_default_column_value_if_not_provided =
        target_part->storage.getSettings()->ingest_default_column_value_if_not_provided;
    bool read_with_direct_io = settings.min_bytes_to_use_direct_io != 0 &&
                               target_part->bytes_on_disk >= settings.min_bytes_to_use_direct_io;
    const Strings & ingest_column_names = stream.ingest_column_names;
    std::vector<UInt8> ingest_column_compression_statuses(ingest_column_names.size(), 0);
    for (const NameAndTypePair & p : target_part->getColumns())
    {
        auto it = std::find(ingest_column_names.begin(), ingest_column_names.end(), p.name);
        if (it != ingest_column_names.end())
            ingest_column_compression_statuses[std::distance(ingest_column_names.begin(), it)] = p.type->isCompression();
    }

    auto source_input = std::make_unique<MergeTreeSequentialSource>(
        target_cloud_merge_tree,
        target_storage_snapshot,
        target_part, all_columns, read_with_direct_io, true);
    QueryPipeline source_pipeline;
    source_pipeline.init(Pipe(std::move(source_input)));
    source_pipeline.setMaxThreads(1);
    BlockInputStreamPtr target_part_in = std::make_shared<PipelineExecutingBlockInputStream>(std::move(source_pipeline));

    target_part_in = std::make_shared<SquashingBlockInputStream>(target_part_in, settings.min_insert_block_size_rows, settings.min_insert_block_size_bytes);

    return IngestColumn::readTargetDataForUpdate(*target_part_in, stream.ordered_key_names, stream.ingest_column_names, ingest_column_compression_statuses, ingest_default_column_value_if_not_provided);
}

void MemoryEfficientIngestColumn::updateTargetDataWithSourcePart(
    MergeTreeDataPartPtr source_part,
    const Names & all_columns,
    IngestColumn::TargetPartData & target_part_data)
{
    bool read_with_direct_io = settings.min_bytes_to_use_direct_io != 0 &&
                               source_part->bytes_on_disk >= settings.min_bytes_to_use_direct_io;

    auto source_input = std::make_unique<MergeTreeSequentialSource>(
        source_cloud_merge_tree,
        source_storage_snapshot,
        source_part, all_columns, read_with_direct_io, true);
    QueryPipeline source_pipeline;
    source_pipeline.init(Pipe(std::move(source_input)));
    source_pipeline.setMaxThreads(1);
    BlockInputStreamPtr source_part_in = std::make_shared<PipelineExecutingBlockInputStream>(std::move(source_pipeline));

    source_part_in = std::make_shared<SquashingBlockInputStream>(source_part_in, settings.min_insert_block_size_rows, settings.min_insert_block_size_bytes);

    updateTargetDataWithSourceData(
        *source_part_in, target_part_data);
}

} /// end namespace DB
