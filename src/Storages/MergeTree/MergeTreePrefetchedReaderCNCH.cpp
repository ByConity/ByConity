#include <ctime>
#include <memory>

#include <Common/escapeForFileName.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/MapHelpers.h>
#include <DataTypes/ObjectUtils.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreePrefetchedReaderCNCH.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>

namespace DB
{

MergeTreePrefetchedReaderCNCH::MergeTreePrefetchedReaderCNCH(
    const MergeTreeMetaBase::DataPartPtr& data_part_,
    const NamesAndTypesList& columns_,
    const StorageMetadataPtr& metadata_snapshot_,
    MarkCache* mark_cache_,
    const MarkRanges& mark_ranges_,
    const MergeTreeReaderSettings& settings_,
    CnchMergePrefetcher::PartFutureFiles* future_files_,
    const ReadBufferFromFileBase::ProfileCallback& profile_callback_,
    clockid_t clock_type_)
    : MergeTreeReaderWide(
        data_part_,
        columns_,
        metadata_snapshot_,
        nullptr,
        mark_cache_,
        mark_ranges_,
        settings_,
        /* index_executor */nullptr,
        {},
        profile_callback_,
        clock_type_,
        false
    ), future_files(future_files_)
{
    // HACK: Mock a MergeTreeIndexGranularityInfo here so we won't need to
    // touch anything inside MergeTreeReaderStream
    MergeTreeIndexGranularityInfo mocked_index_granularity_info = data_part->index_granularity_info;
    mocked_index_granularity_info.marks_file_extension = "";

    try
    {
        /// need to use columns from IMergeTreeReader to read converted subcolumns of nested columns
        for (const NameAndTypePair & column : columns)
        {
            auto column_in_part = getColumnFromPart(column);
            if (column_in_part.type->isByteMap())
            {
                auto checksums = data_part->getChecksums();
                auto implicit_value_type = typeid_cast<const DataTypeMap &>(*column_in_part.type.get()).getValueTypeForImplicitColumn();
                auto serialization = implicit_value_type->getDefaultSerialization();

                auto [curr, end] = getMapColumnRangeFromOrderedFiles(column_in_part.name, checksums->files);
                for (; curr != end; ++curr)
                {
                    if (curr->second.is_deleted || !isMapImplicitDataFileNameNotBaseOfSpecialMapName(curr->first, column_in_part.name))
                        continue;
                    const String & implicit_key_name = parseImplicitColumnFromImplicitFileName(curr->first, column_in_part.name);
                    const String & key_name = parseKeyNameFromImplicitFileName(curr->first, column_in_part.name);
                    // Special handing if implicit key is referenced too
                    if (columns.contains(implicit_key_name))
                    {
                        dup_implicit_keys.insert(implicit_key_name);
                    }

                    addStreams({implicit_key_name, implicit_value_type}, profile_callback_, clock_type_, &mocked_index_granularity_info);
                    map_column_keys.insert({column_in_part.name, key_name});
                }
            }
            else
            {
                addStreams(column_in_part, profile_callback_, clock_type_, &mocked_index_granularity_info);
            }
        }
    }
    catch (...)
    {
        storage.reportBrokenPart(data_part->name);
        throw;
    }
}

MergeTreePrefetchedReaderCNCH::~MergeTreePrefetchedReaderCNCH()
{
    if (future_files->stream_to_mutation_index.empty()) /// optimize for small parts
        return;
    try
    {
        for (const NameAndTypePair& column : columns)
        {
            auto column_in_part = getColumnFromPart(column);
            auto serialization = data_part->getSerializationForColumn(column_in_part);
            serialization->enumerateStreams([&](const ISerialization::SubstreamPath& substream_path) {
                String stream_name = ISerialization::getFileNameForStream(column_in_part, substream_path);

                for (const auto& extension : {".bin", ".mrk"})
                    future_files->releaseSegment(stream_name + extension);
            });
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void MergeTreePrefetchedReaderCNCH::addStreams(const NameAndTypePair& name_and_type,
    ReadBufferFromFileBase::ProfileCallback profile_callback, clockid_t clock_type,
    MergeTreeIndexGranularityInfo* mocked_index_granularity_info)
{
    auto checksums = data_part->getChecksums();

    ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath& substream_path) {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);
        if (streams.count(stream_name))
            return;

        bool data_file_exists = checksums->files.count(stream_name + DATA_FILE_EXTENSION);
        /** If data file is missing then we will not try to open it.
          * It is necessary since it allows to add new column to structure of the table without creating new files for old parts.
          */
        if (!data_file_exists)
            return;

        String stream_bin = stream_name + ".bin";
        String stream_mrk = stream_name + ".mrk";

        auto [bin_disk, bin_path, bin_offset] = future_files->getFutureSegmentAndPrefetch(stream_bin)->get();
        auto [mrk_disk, mrk_path, mrk_offset] = future_files->getFutureSegmentAndPrefetch(stream_mrk)->get();
        bool is_lc_dict = isLowCardinalityDictionary(substream_path);
        streams.emplace(stream_name,
            std::make_unique<MergeTreeReaderStream>(
                IMergeTreeReaderStream::StreamFileMeta {
                    .disk = bin_disk,
                    .rel_path = bin_path,
                    .offset = data_part->getFileOffsetOrZero(stream_bin) - bin_offset,
                    .size = data_part->getFileSizeOrZero(stream_bin),
                },
                IMergeTreeReaderStream::StreamFileMeta {
                    .disk = mrk_disk,
                    .rel_path = mrk_path,
                    .offset = data_part->getFileOffsetOrZero(stream_mrk) - mrk_offset,
                    .size = data_part->getFileSizeOrZero(stream_mrk)
                },
                stream_name,
                data_part->getMarksCount(),
                all_mark_ranges,
                settings,
                mark_cache,
                uncompressed_cache,
                mocked_index_granularity_info,
                profile_callback,
                clock_type,
                is_lc_dict
            )
        );
    };

    auto serialization = data_part->getSerializationForColumn(name_and_type);
    serialization->enumerateStreams(callback);
    serializations.emplace(name_and_type.name, std::move(serialization));
}

}
