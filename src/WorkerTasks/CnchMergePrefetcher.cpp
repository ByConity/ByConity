#include <WorkerTasks/CnchMergePrefetcher.h>

#include <Common/escapeForFileName.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/MapHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>

namespace DB
{

void CnchMergePrefetcher::PartFutureFiles::tryPrefetchStage(int stage)
{
    for (auto* future_segment : prefetch_stages[stage])
        schedulePrefetchTask(*future_segment);
    prefetch_stages[stage].clear(); /// each segment could only be prefetched once
}

void CnchMergePrefetcher::PartFutureFiles::schedulePrefetchTask(FutureSegment & future_segment)
{
    future_segment.done = std::async(std::launch::async, [&future_segment] {
        auto & cancel_flag = future_segment.prefetcher->cancel;
        const auto *part = future_segment.part;
        auto local_path = future_segment.reservation->getDisk()->getPath() \
            + future_segment.data_relative_path;
        auto offset = future_segment.offset;
        auto size = future_segment.size;
        auto stage = future_segment.stage;

        String remote_rel_path = part->getFullRelativePath() + "data";
        String remote_abs_path = part->getFullPath() + "data";
        try
        {
            Stopwatch stopwatch;
            LOG_TRACE(getLogger("CnchMergePrefetcher"), "Stage {} copying to {}", stage, local_path);

            std::unique_ptr<ReadBufferFromFileBase> in = part->volume->getDisk()->readFile(
                remote_rel_path, future_segment.prefetcher->read_settings);

            in->seek(offset);
            WriteBufferFromFile out(local_path);
            copyData(*in, out, size, cancel_flag, future_segment.reservation.get());

            if (!cancel_flag.load(std::memory_order_relaxed))
            {
                LOG_TRACE(getLogger("CnchMergePrefetcher"), "Stage {} "
                    "copied to {}, elapsed {} ms.", stage, local_path,
                    stopwatch.elapsedMilliseconds());
            }
        }
        catch (Exception & e)
        {
            tryLogCurrentException("CnchMergePrefetcher", "Fail to copy " + remote_abs_path + " to " + local_path);
            e.addMessage("(while coping " + remote_abs_path + " to " + local_path + ")");
            throw;
        }
        catch (...)
        {
            tryLogCurrentException("CnchMergePrefetcher", "Fail to copy " + remote_abs_path + " to " + local_path);
            throw;
        }
    });
}

CnchMergePrefetcher::FutureSegment * CnchMergePrefetcher::PartFutureFiles::tryGetFutureSegment(const String & stream_name)
{
    if (stream_to_mutation_index.empty()) /// optimize for small parts
    {
        if (mutation_segments.empty())
            throw Exception("CnchMergePrefetcher: mutation_segments should not be empty", ErrorCodes::LOGICAL_ERROR);

        if (mutation_segments.begin()->second.empty())
            throw Exception("CnchMergePrefetcher: the first vector of mutation_segments should not be empty", ErrorCodes::LOGICAL_ERROR);

        return &mutation_segments.begin()->second.front();
    }

    auto it = stream_to_mutation_index.find(stream_name);
    if (it == stream_to_mutation_index.end())
        return nullptr;

    auto & [mutation, index] = it->second;
    /// we can ensure that mutation_segments[mutation] has be resized to enough size
    return &mutation_segments[mutation][index];
}

CnchMergePrefetcher::FutureSegment * CnchMergePrefetcher::PartFutureFiles::getFutureSegmentAndPrefetch(const String & stream_name)
{
    auto* future_segment = tryGetFutureSegment(stream_name);
    if (!future_segment)
        throw Exception("Future segment for " + stream_name + " not found", ErrorCodes::LOGICAL_ERROR);

    for (int i = future_segment->stage; i < std::min(future_segment->stage + 2, static_cast<int>(prefetch_stages.size())); ++i)
        tryPrefetchStage(i);

    if (!future_segment->valid())
        throw Exception("Future segment is not valid: " + future_segment->reservation->getDisk()->getPath() \
            + future_segment->data_relative_path, ErrorCodes::LOGICAL_ERROR);

    return future_segment;
}

void CnchMergePrefetcher::PartFutureFiles::releaseSegment(const String & stream_name)
{
    if (auto* future_segment = tryGetFutureSegment(stream_name))
    {
        future_segment->future_access -= 1;
        if (future_segment->future_access == 0)
        {
            LOG_TRACE(getLogger("CnchMergePrefetcher"), "Removing {}",
                future_segment->reservation->getDisk()->getPath() + future_segment->data_relative_path);
            future_segment->reservation->getDisk()->removeRecursive(future_segment->data_relative_path);
        }
        else if (future_segment->future_access < 0)
        {
            LOG_WARNING(getLogger("CnchMergePrefetcher"), "FutureSegment access count < 0, this is a bug");
        }
    }
}

namespace
{
    IMergeTreeDataPartPtr findDataPartWithVersion(const String & root_part, IMergeTreeDataPartPtr part, Int64 mutation)
    {
        while (mutation != part->info.mutation)
        {
            if (!part->isPartial())
                throw Exception("Version " + toString(mutation) + " not found in delta chain of " + root_part, ErrorCodes::LOGICAL_ERROR);
            part = part->getPreviousPart();
        }
        return part;
    }
}

void CnchMergePrefetcher::submitDataPart(
    const IMergeTreeDataPartPtr & data_part, const NamesAndTypesList & merging_columns, const NamesAndTypesList & gathering_columns)
{
    if (merging_columns.empty())
        throw Exception("Expect non-empty merging_columns", ErrorCodes::LOGICAL_ERROR);

    auto log = getLogger("CnchMergePrefetcher");

    auto* future_files
        = part_to_future_files.try_emplace(data_part->name, std::make_unique<PartFutureFiles>(*this, data_part->name)).first->second.get();
    auto & mutation_segments = future_files->mutation_segments;
    auto & prefetch_stages = future_files->prefetch_stages;

    size_t bytes_on_disk = data_part->volume->getDisk()->getFileSize(
        data_part->getFullRelativePath() + "data"); /// XXX: should use part->bytes_on_disk in the future
    /// optimize for small parts
    if (!data_part->isPartial() && bytes_on_disk <= segment_size * 2)
    {
        auto & segments = mutation_segments[data_part->info.mutation];
        segments.emplace_back();
        segments.back().prefetcher = this;
        segments.back().part = data_part.get();
        segments.back().offset = 0;
        segments.back().size = bytes_on_disk;
        ReservationPtr reservation = storage.reserveSpace(bytes_on_disk,
            IStorage::StorageLocation::AUXILITY);
        segments.back().data_relative_path = storage.getRelativeDataPath(IStorage::StorageLocation::AUXILITY)
            + temp_dir_rel_path + data_part->name + "_data_0_0_" + toString(segments.back().size);
        segments.back().reservation = std::move(reservation);
        segments.back().stage = 0;
        prefetch_stages.emplace_back();
        prefetch_stages.back().push_back(&segments.back());

        LOG_DEBUG(log, "Will prefetch small part {}", data_part->name);
        future_files->tryPrefetchStage(0);
        return;
    }

    /// Sort all streams
    auto & checksums_files = data_part->getChecksums()->files;
    // std::vector<decltype(checksums_files)::value_type *> sorted_checksums;
    std::vector<MergeTreeDataPartChecksums::FileChecksums::value_type *> sorted_checksums;
    sorted_checksums.reserve(checksums_files.size());
    for (auto & file_checksum : checksums_files)
    {
        if (!file_checksum.second.is_deleted)
            sorted_checksums.emplace_back(&file_checksum);
    }
    /// Order by mutation desc, offset asc
    std::sort(sorted_checksums.begin(), sorted_checksums.end(), [](auto & lhs, auto & rhs) {
        if (lhs->second.mutation > rhs->second.mutation)
            return true;
        else if (lhs->second.mutation < rhs->second.mutation)
            return false;
        return lhs->second.file_offset < rhs->second.file_offset;
    });

    /// Split all mvcc parts
    IMergeTreeDataPartPtr curr_part = data_part;
    for (size_t l = 0, r = 0; l < sorted_checksums.size(); l = r)
    {
        /// Find corresponding part
        auto curr_mutation = sorted_checksums[l]->second.mutation;
        auto new_part = findDataPartWithVersion(data_part->name, data_part, curr_mutation);
        if (new_part != curr_part)
        {
            curr_part = new_part;
            bytes_on_disk = curr_part->volume->getDisk()->getFileSize(curr_part->getFullRelativePath() + "data");
        }

        /// Determine checksums with the same mutation
        for (r = l + 1; r < sorted_checksums.size() && curr_mutation == sorted_checksums[r]->second.mutation; ++r)
            ;

        /// Split current part
        std::vector<size_t> split_offsets{0};
        for (size_t i = l; i < r; ++i)
        {
            auto & [file, checksum] = *sorted_checksums[i];
            auto offset = checksum.file_offset;

            if (offset - split_offsets.back() >= segment_size && bytes_on_disk - offset >= segment_size / 3)
                split_offsets.push_back(offset);

            auto & stream_to_mutation_index = future_files->stream_to_mutation_index;
            stream_to_mutation_index.try_emplace(file, curr_mutation, split_offsets.size() - 1);
        }
        split_offsets.push_back(bytes_on_disk);

        /// Prepare future segments accord the splitting offset
        auto & future_segments = mutation_segments[curr_mutation];
        future_segments.resize(split_offsets.size() - 1);
        for (size_t i = 0; i < future_segments.size(); ++i)
        {
            auto & future_segment = future_segments[i];
            future_segment.prefetcher = this;
            future_segment.part = curr_part.get();
            future_segment.offset = split_offsets[i];
            future_segment.size = split_offsets[i + 1] - split_offsets[i];
            ReservationPtr reservation = storage.reserveSpace(future_segment.size,
                IStorage::StorageLocation::AUXILITY);
            future_segment.data_relative_path = storage.getRelativeDataPath(IStorage::StorageLocation::AUXILITY)
                + temp_dir_rel_path + curr_part->name + "_data_" + toString(i) + "_" + toString(future_segment.offset)
                + "_" + toString(future_segment.size);
            future_segment.reservation = std::move(reservation);
        }

        LOG_TRACE(log, "Split part {} to {} segments to prefetch", curr_part->name,
            future_segments.size());
    }

    String & fixed_injected_column = future_files->fixed_injected_column;

    auto calc_stage = [&](const String & column_name, const DataTypePtr & column_type) {
        data_part->getSerializationForColumn({column_name, column_type})->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path) {
            String stream_name = ISerialization::getFileNameForStream(column_name, substream_path);
            for (const auto & extension : {".bin", ".mrk"})
            {
                if (auto* future_segment = future_files->tryGetFutureSegment(stream_name + extension))
                {
                    if (fixed_injected_column.empty())
                    {
                        fixed_injected_column = column_name;
                        future_segment->future_access = 99999999;
                    }
                    else if (column_name == fixed_injected_column || columns_may_inject.count(column_name))
                        future_segment->future_access = 99999999; /// Do not remove this future segment which injected column belongs to
                    else
                        future_segment->future_access += 1;

                    if (-1 == future_segment->stage)
                    {
                        future_segment->stage = prefetch_stages.size() - 1;
                        prefetch_stages.back().push_back(future_segment);
                    }
                }
            }
        });
    };

    auto callback_for_data_types = [&](const String & column_name, const DataTypePtr & column_type) {
        if (column_type->isByteMap())
        {
            auto implicit_value_type = typeid_cast<const DataTypeMap &>(*column_type.get()).getValueTypeForImplicitColumn();
            auto serialization = implicit_value_type->getDefaultSerialization();

            auto [curr, end] = getMapColumnRangeFromOrderedFiles(column_name, checksums_files);
            for (; curr != end; ++curr)
            {
                if (curr->second.is_deleted || !isMapImplicitDataFileNameNotBaseOfSpecialMapName(curr->first, column_name))
                    continue;
                const String & implicit_key_name = parseImplicitColumnFromImplicitFileName(curr->first, column_name);
                calc_stage(implicit_key_name, implicit_value_type);
            }
        }
        else
        {
            calc_stage(column_name, column_type);
        }
    };

    /// Horizontal stage
    prefetch_stages.emplace_back();
    for (const auto & [column_name, column_type] : merging_columns)
        callback_for_data_types(column_name, column_type);

    /// Vertical stage for normal column
    for (const auto & [column_name, column_type] : gathering_columns)
    {
        if (!prefetch_stages.back().empty())
            prefetch_stages.emplace_back();
        callback_for_data_types(column_name, column_type);
    }

    if (prefetch_stages.back().empty())
        prefetch_stages.pop_back();
    LOG_DEBUG(log, "Will prefetch {} in {} stages", data_part->name, prefetch_stages.size());
    future_files->tryPrefetchStage(0);
}

CnchMergePrefetcher::CnchMergePrefetcher(const Context & context_, const MergeTreeMetaBase & storage_, const String & temp_dir_)
    : storage(storage_)
    , segment_size(context_.getSettingsRef().cnch_merge_prefetch_segment_size)
    , temp_dir_rel_path(temp_dir_.empty() ? temp_dir_ : (temp_dir_.back() == '/' ? temp_dir_ : temp_dir_ + '/'))
    , read_settings(context_.getReadSettings())
{
    StoragePolicyPtr policy = storage.getStoragePolicy(IStorage::StorageLocation::AUXILITY);
    for (const DiskPtr& disk : policy->getDisks())
    {
        String temp_dir_path = storage.getRelativeDataPath(IStorage::StorageLocation::AUXILITY) + temp_dir_rel_path;
        disk->createDirectories(temp_dir_path);
    }

    for (auto & [column_name, column_default] : storage.getInMemoryMetadataPtr()->getColumns().getDefaults())
        column_default.expression->collectIdentifierNames(columns_may_inject);
}

CnchMergePrefetcher::~CnchMergePrefetcher()
{
    cancel.store(1, std::memory_order_relaxed);

    for (auto & [part, future_files] : part_to_future_files)
        for (auto & [mutation, future_segments] : future_files->mutation_segments)
            for (auto & future_segment : future_segments)
            {
                if (!future_segment.done.valid())
                    continue;
                try
                {
                    future_segment.done.get();
                    if (future_segment.future_access > 0) /// Should warning here ?
                        future_segment.reservation->getDisk()->removeRecursive(future_segment.data_relative_path);
                }
                catch (...)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }

    try
    {
        StoragePolicyPtr policy = storage.getStoragePolicy(IStorage::StorageLocation::AUXILITY);
        for (const DiskPtr & disk : policy->getDisks())
        {
            String temp_dir_path = storage.getRelativeDataPath(IStorage::StorageLocation::AUXILITY) + temp_dir_rel_path;
            disk->removeRecursive(temp_dir_path);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}
}
