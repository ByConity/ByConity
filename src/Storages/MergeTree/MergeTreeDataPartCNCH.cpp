#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>

#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <IO/LimitReadBuffer.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include "Storages/MergeTree/MergeTreeReaderCNCH.h"
#include <Storages/DiskCache/DiskCacheFactory.h>
#include <Storages/DiskCache/MetaFileDiskCacheSegment.h>
#include "Common/Exception.h"

namespace ProfileEvents
{
}

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_FILE_IN_DATA_PART;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

static std::unique_ptr<ReadBufferFromFileBase> openForReading(const DiskPtr & disk, const String & path, size_t file_size)
{
    return disk->readFile(path, {.buffer_size = std::min(file_size, static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE))});
}

static LimitReadBuffer readPartFile(ReadBufferFromFileBase & in, off_t file_offset, size_t file_size)
{
    if (file_size == 0)
        throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "The size of file is zero");

    in.seek(file_offset);
    return LimitReadBuffer(in, file_size, false);
}

static std::pair<off_t, size_t> getFileOffsetAndSize(const IMergeTreeDataPart & data_part, const String & file_name)
{
    auto checksums = data_part.getChecksums();
    if (auto it = checksums->files.find(file_name); it != checksums->files.end())
    {
        return {it->second.file_offset, it->second.file_size};
    }
    else
    {
        throw Exception(fmt::format("Cannot find file {} in part {}", file_name, data_part.name), ErrorCodes::NO_FILE_IN_DATA_PART);
    }
}

MergeTreeDataPartCNCH::MergeTreeDataPartCNCH(
    const MergeTreeMetaBase & storage_,
    const String & name_,
    const VolumePtr & volume_,
    const std::optional<String> & relative_path_,
    const IMergeTreeDataPart * parent_part_)
    : IMergeTreeDataPart(storage_, name_, volume_, relative_path_, Type::CNCH, parent_part_)
{
}

MergeTreeDataPartCNCH::MergeTreeDataPartCNCH(
    const MergeTreeMetaBase & storage_,
    const String & name_,
    const MergeTreePartInfo & info_,
    const VolumePtr & volume_,
    const std::optional<String> & relative_path_,
    const IMergeTreeDataPart * parent_part_)
    : IMergeTreeDataPart(storage_, name_, info_, volume_, relative_path_, Type::CNCH, parent_part_)
{
}

IMergeTreeDataPart::MergeTreeReaderPtr MergeTreeDataPartCNCH::getReader(
    [[maybe_unused]] const NamesAndTypesList & columns_to_read,
    [[maybe_unused]] const StorageMetadataPtr & metadata_snapshot,
    [[maybe_unused]] const MarkRanges & mark_ranges,
    [[maybe_unused]] UncompressedCache * uncompressed_cache,
    [[maybe_unused]] MarkCache * mark_cache,
    [[maybe_unused]] const MergeTreeReaderSettings & reader_settings_,
    [[maybe_unused]] MergeTreeBitMapIndexReader * bitmap_index_reader,
    [[maybe_unused]] const ValueSizeMap & avg_value_size_hints,
    [[maybe_unused]] const ReadBufferFromFileBase::ProfileCallback & profile_callback) const
{
    auto new_settings = reader_settings_;
    new_settings.convert_nested_to_subcolumns = true;

    auto ptr = std::static_pointer_cast<const MergeTreeDataPartCNCH>(shared_from_this());
    return std::make_unique<MergeTreeReaderCNCH>(
        ptr, columns_to_read, metadata_snapshot, uncompressed_cache,
        mark_cache, mark_ranges, new_settings, bitmap_index_reader,
        avg_value_size_hints, profile_callback);
}

IMergeTreeDataPart::MergeTreeWriterPtr MergeTreeDataPartCNCH::getWriter(
    [[maybe_unused]] const NamesAndTypesList & columns_list,
    [[maybe_unused]] const StorageMetadataPtr & metadata_snapshot,
    [[maybe_unused]] const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    [[maybe_unused]] const CompressionCodecPtr & default_codec_,
    [[maybe_unused]] const MergeTreeWriterSettings & writer_settings,
    [[maybe_unused]] const MergeTreeIndexGranularity & computed_index_granularity) const
{
    return {};
}

bool MergeTreeDataPartCNCH::operator < (const MergeTreeDataPartCNCH & r) const
{
    if (!info.mutation || !r.info.mutation)
        return name < r.name;

    if (name < r.name)
        return true;
    else if (name == r.name)
        return info.mutation < r.info.mutation;
    else
        return false;
}

bool MergeTreeDataPartCNCH::operator > (const MergeTreeDataPartCNCH & r) const
{
    if (!info.mutation || !r.info.mutation)
        return name > r.name;

    if (name > r.name)
        return true;
    else if (name == r.name)
        return info.mutation > r.info.mutation;
    else
        return false;
}

String MergeTreeDataPartCNCH::getFileNameForColumn(const NameAndTypePair &) const
{
    return DATA_FILE_NAME;
}

bool MergeTreeDataPartCNCH::hasColumnFiles(const NameAndTypePair &) const
{
    return true;
};

void MergeTreeDataPartCNCH::loadIndexGranularity(
    [[maybe_unused]] size_t /*marks_count*/, [[maybe_unused]] const std::vector<size_t> & index_granularities)
{
    // if (index_granularities.empty())
    //     throw Exception("MergeTreeDataPartCNCH cannot be created with non-adaptive granulary.", ErrorCodes::NOT_IMPLEMENTED);

    // for (size_t granularity : index_granularities)
    //     index_granularity.appendMark(granularity);

    // index_granularity.setInitialized();
    loadIndexGranularity();
};

void MergeTreeDataPartCNCH::loadColumnsChecksumsIndexes([[maybe_unused]] bool require_columns_checksums, [[maybe_unused]] bool check_consistency)
{
}

void MergeTreeDataPartCNCH::loadFromFileSystem(bool load_hint_mutation)
{
    off_t meta_info_offset = 0;
    size_t meta_info_size = 0;
    getMetaInfoPosAndSize(meta_info_offset, meta_info_size);

    DiskPtr disk = volume->getDisk();
    String rel_path = getFullRelativePath() + "/data";
    auto reader = openForReading(disk, rel_path, meta_info_size);
    LimitReadBuffer limit_reader = readPartFile(*reader, meta_info_offset, meta_info_size);
    readPartBinary(*this, limit_reader, load_hint_mutation);
}

MergeTreeDataPartChecksums::FileChecksums MergeTreeDataPartCNCH::loadPartDataFooter() const
{
    const String data_file_path = getFullRelativeDataPath();
    size_t data_file_size = volume->getDisk()->getFileSize(data_file_path);
    if (!volume->getDisk()->exists(data_file_path))
        throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "No data file of part {} under path {}", name, data_file_path);

    auto data_file = openForReading(volume->getDisk(), data_file_path, MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE);
    data_file->seek(data_file_size - MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE);

    MergeTreeDataPartChecksums::FileChecksums file_checksums;
    auto add_file_checksum = [this, &buf = *data_file, &file_checksums] (const String & file_name) {
        Checksum file_checksum;
        file_checksum.mutation = info.mutation;
        readIntBinary(file_checksum.file_offset, buf);
        readIntBinary(file_checksum.file_size, buf);
        readIntBinary(file_checksum.file_hash, buf);
        LOG_DEBUG(&Poco::Logger::get("MergeTreeDataPartCNCH"), "{} infomation: file offset {}, file size {}, file hash {}-{}\n", file_name, file_checksum.file_offset, file_checksum.file_size, file_checksum.file_hash.first, file_checksum.file_hash.second);
        file_checksums[file_name] = std::move(file_checksum);
    };

    add_file_checksum("primary.idx");
    add_file_checksum("checksums.txt");
    add_file_checksum("metainfo.txt");
    add_file_checksum("unique_key.idx");

    return file_checksums;
}

bool MergeTreeDataPartCNCH::isDeleted() const
{
    return deleted;
}

String MergeTreeDataPartCNCH::getFullDataPath() const
{
    return fs::path(getFullPath()) / "data";
}

String MergeTreeDataPartCNCH::getFullRelativeDataPath() const
{
    return fs::path(getFullPath()) / "data";
}

void MergeTreeDataPartCNCH::checkConsistency([[maybe_unused]] bool require_part_metadata) const
{
}

void MergeTreeDataPartCNCH::loadIndex()
{
    /// It can be empty in case of mutations
    if (!index_granularity.isInitialized())
        throw Exception("Index granularity is not loaded before index loading", ErrorCodes::LOGICAL_ERROR);

    if (isPartial())
    {
        auto base_part = getBasePart();
        index = base_part->getIndex();
        return;
    }

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    size_t key_size = primary_key.column_names.size();

    if (!key_size)
        return;

    const bool enable_disk_cache = storage.getSettings()->enable_local_disk_cache;
    if (enable_disk_cache)
    {
        try
        {
            auto disk_cache = DiskCacheFactory::instance().getDefault().first;
            PrimaryIndexDiskCacheSegment segment(shared_from_this());
            auto [cache_disk, segment_path] = disk_cache->get(segment.getSegmentName());
            if (cache_disk && cache_disk->exists(segment_path))
            {
                auto cache_buf = openForReading(cache_disk, segment_path, cache_disk->getFileSize(segment_path));
                index = loadIndexFromBuffer(*cache_buf, primary_key);
                return;
            }
        }
        catch (...)
        {
            tryLogCurrentException("Could not load index from disk cache");
        }
    }

    auto checksums = getChecksums();
    auto [file_offset, file_size] = getFileOffsetAndSize(*this, "primary.idx");
    auto data_file = openForReading(volume->getDisk(), getFullRelativeDataPath(), file_size);
    LimitReadBuffer buf = readPartFile(*data_file, file_offset, file_size);
    index = loadIndexFromBuffer(buf, primary_key);

    if (enable_disk_cache)
    {
        auto index_seg = std::make_shared<PrimaryIndexDiskCacheSegment>(shared_from_this());
        auto disk_cache = DiskCacheFactory::instance().getDefault().first;
        disk_cache->cacheSegmentsToLocalDisk({std::move(index_seg)});
    }
}

IMergeTreeDataPart::ChecksumsPtr MergeTreeDataPartCNCH::loadChecksums([[maybe_unused]] bool require)
{
    ChecksumsPtr checksums = std::make_shared<Checksums>();
    checksums->storage_type = StorageType::ByteHDFS;
    if (deleted)
        return checksums;

    const bool enable_disk_cache = storage.getSettings()->enable_local_disk_cache;
    if (enable_disk_cache)
    {
        try
        {
            ChecksumsDiskCacheSegment checksums_segment(shared_from_this());
            auto disk_cache = DiskCacheFactory::instance().getDefault().first;
            auto [cache_disk, segment_path] = disk_cache->get(checksums_segment.getSegmentName());
            if (cache_disk && cache_disk->exists(segment_path))
            {
                auto cache_buf = openForReading(cache_disk, segment_path, cache_disk->getFileSize(segment_path));
                if (checksums->read(*cache_buf))
                    assertEOF(*cache_buf);
                return checksums;
            }
        }
        catch (...)
        {
            tryLogCurrentException("Could not load checksums from disk");
        }
    }

    String data_path = getFullRelativeDataPath();
    auto data_footer = loadPartDataFooter();
    const auto & checksum_file = data_footer["checksums.txt"];

    if (checksum_file.file_size == 0/* && isDeleted() */)
        throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "The size of checksums in part {} under path {} is zero", name, data_path);

    auto data_file = openForReading(volume->getDisk(), data_path, checksum_file.file_size);
    LimitReadBuffer buf = readPartFile(*data_file, checksum_file.file_offset, checksum_file.file_size);

    if (checksums->read(buf))
    {
        assertEOF(buf);
        /// bytes_on_disk += delta_checksums->getTotalSizeOnDisk();
    }
    else
    {
        /// bytes_on_disk += delta_checksums->getTotalSizeOnDisk();
    }

    // merge with data footer
    data_footer.merge(checksums->files);
    checksums->files.swap(data_footer);

    if (isPartial())
    {
        /// merge with previous checksums with current checksums
        const auto & prev_part = tryGetPreviousPart();
        auto prev_checksums = prev_part->getChecksums();

        /// insert checksum files from previous part if it's not in current checksums
        for (const auto & [name, file] : prev_checksums->files)
        {
            [[maybe_unused]] auto [it, inserted] = checksums->files.emplace(name, file);
        }
    }

    // remove deleted files in checksums
    for (auto it = checksums->files.begin(); it != checksums->files.end();)
    {
        const auto & file = it->second;
        if (file.is_deleted)
            it = checksums->files.erase(it);
        else
            ++it;
    }

    if (storage.getSettings()->enable_persistent_checksum || is_temp || isProjectionPart())
    {
        std::lock_guard lock(checksums_mutex);
        checksums_ptr = checksums;
    }

    /// store in disk cache
    if (enable_disk_cache)
    {
        auto segment = std::make_shared<ChecksumsDiskCacheSegment>(shared_from_this());
        auto disk_cache = DiskCacheFactory::instance().getDefault().first;
        disk_cache->cacheSegmentsToLocalDisk({std::move(segment)});
    }

    return checksums;
}

void MergeTreeDataPartCNCH::loadIndexGranularity()
{
    index_granularity.resizeWithFixedGranularity(marks_count, storage.getSettings()->index_granularity);
    index_granularity.setInitialized();
}

void MergeTreeDataPartCNCH::calculateEachColumnSizes(
    [[maybe_unused]] ColumnSizeByName & each_columns_size, [[maybe_unused]] ColumnSize & total_size) const
{
}

void MergeTreeDataPartCNCH::getMetaInfoPosAndSize(off_t & off, size_t & size)
{
    String data_rel_path = getFullRelativePath() + "/data";
    DiskPtr disk = volume->getDisk();
    size_t file_size = disk->getFileSize(data_rel_path);
    auto reader = openForReading(disk, data_rel_path, MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE);
    reader->seek(file_size - MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE + 2 * (2 * sizeof(size_t) + sizeof(CityHash_v1_0_2::uint128)));
    readIntBinary(off, *reader);
    readIntBinary(size, *reader);
}

}
