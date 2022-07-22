#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>

#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <IO/LimitReadBuffer.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include "Storages/MergeTree/MergeTreeReaderCNCH.h"

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

static std::unique_ptr<ReadBufferFromFileBase> openForReading(const DiskPtr & disk, const String & path, size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE)
{
    return disk->readFile(path, {.buffer_size = buffer_size});
}

static LimitReadBuffer readPartFile(ReadBufferFromFileBase & in, off_t file_offset, size_t file_size)
{
    if (file_size == 0)
        throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "The size of file is zero");

    in.seek(file_offset);
    return LimitReadBuffer(in, file_size, true);
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
    MergeTreeMetaBase & storage_,
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
    [[maybe_unused]] size_t marks_count, [[maybe_unused]] const std::vector<size_t> & index_granularities)
{
    if (index_granularities.empty())
        throw Exception("MergeTreeDataPartCNCH cannot be created with non-adaptive granulary.", ErrorCodes::NOT_IMPLEMENTED);

    for (size_t granularity : index_granularities)
        index_granularity.appendMark(granularity);

    index_granularity.setInitialized();
};

void MergeTreeDataPartCNCH::loadColumnsChecksumsIndexes([[maybe_unused]] bool require_columns_checksums, [[maybe_unused]] bool check_consistency)
{
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
    return fs::path(getFullPath()) / DATA_FILE_EXTENSION;
}

String MergeTreeDataPartCNCH::getFullRelativeDataPath() const
{
    return fs::path(getFullPath()) / DATA_FILE_EXTENSION;
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

    if (key_size)
    {
        auto data_file = openForReading(volume->getDisk(), getFullRelativeDataPath());
        auto checksums = getChecksums();
        auto [file_offset, file_size] = getFileOffsetAndSize(*this, "primary.idx");
        LimitReadBuffer buf = readPartFile(*data_file, file_offset, file_size);
        index = loadIndexFromBuffer(buf, primary_key);
    }
}

IMergeTreeDataPart::ChecksumsPtr MergeTreeDataPartCNCH::loadChecksums([[maybe_unused]] bool require)
{
    ChecksumsPtr checksums = std::make_shared<Checksums>();
    checksums->storage_type = StorageType::ByteHDFS;
    if (deleted)
        return checksums;

    String data_path = getFullRelativeDataPath();
    auto data_footer = loadPartDataFooter();
    const auto & checksum_file = data_footer["checksums.txt"];

    if (checksum_file.file_size == 0/* && isDeleted() */)
        throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "The size of checksums in part {} under path {} is zero", name, data_path);

    auto data_file = openForReading(volume->getDisk(), data_path);
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

    return checksums;
}

void MergeTreeDataPartCNCH::loadIndexGranularity()
{
}

void MergeTreeDataPartCNCH::calculateEachColumnSizes(
    [[maybe_unused]] ColumnSizeByName & each_columns_size, [[maybe_unused]] ColumnSize & total_size) const
{
}

}
