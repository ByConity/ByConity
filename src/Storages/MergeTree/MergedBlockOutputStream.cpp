#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Interpreters/Context.h>
#include <Parsers/queryToString.h>

#include <Interpreters/sortBlock.h>
#include <Storages/IndexFile/FilterPolicy.h>
#include <Storages/IndexFile/IndexFileWriter.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include <Common/Coding.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

MergedBlockOutputStream::MergedBlockOutputStream(
    const MergeTreeDataPartPtr & data_part,
    const StorageMetadataPtr & metadata_snapshot_,
    const NamesAndTypesList & columns_list_,
    const MergeTreeIndices & skip_indices,
    CompressionCodecPtr default_codec_,
    bool blocks_are_granules_size,
    bool is_merge)
    : IMergedBlockOutputStream(data_part, metadata_snapshot_)
    , columns_list(columns_list_)
    , default_codec(default_codec_)
{
    MergeTreeWriterSettings writer_settings(
        storage.getContext()->getSettings(),
        storage.getSettings(),
        data_part->index_granularity_info.is_adaptive,
        /* rewrite_primary_key = */ true,
        blocks_are_granules_size);

    if (!part_path.empty())
        volume->getDisk()->createDirectories(part_path);

    if (storage.isBitEngineMode())
    {
        auto new_columns_list = columns_list;
        NamesAndTypesList bitengine_columns;
        for (auto & it : columns_list)
        {
            if (isBitmap64(it.type) && it.type->isBitEngineEncode())
                bitengine_columns.emplace_back(it.name + BITENGINE_COLUMN_EXTENSION, it.type);
        }
        new_columns_list.insert(new_columns_list.end(), bitengine_columns.begin(), bitengine_columns.end());
        writer = data_part->getWriter(new_columns_list, metadata_snapshot, skip_indices, default_codec, writer_settings);
    }
    else
        writer = data_part->getWriter(columns_list, metadata_snapshot, skip_indices, default_codec, writer_settings);

    if (storage.merging_params.mode == MergeTreeData::MergingParams::Unique)
    {
        auto writer_wide = dynamic_cast<MergeTreeDataPartWriterWide *>(writer.get()); 
        if (!writer_wide)
            throw Exception("Unique table only supports wide format part right now.", ErrorCodes::NOT_IMPLEMENTED);
        writer_wide->setMergeStatus(is_merge);
    }
}

/// If data is pre-sorted.
void MergedBlockOutputStream::write(const Block & block)
{
    writeImpl(block, nullptr);
}

/** If the data is not sorted, but we pre-calculated the permutation, after which they will be sorted.
    * This method is used to save RAM, since you do not need to keep two blocks at once - the source and the sorted.
    */
void MergedBlockOutputStream::writeWithPermutation(const Block & block, const IColumn::Permutation * permutation)
{
    writeImpl(block, permutation);
}

void MergedBlockOutputStream::writeSuffix()
{
    throw Exception("Method writeSuffix is not supported by MergedBlockOutputStream", ErrorCodes::NOT_IMPLEMENTED);
}

void MergedBlockOutputStream::writeSuffixAndFinalizePart(
        MergeTreeData::MutableDataPartPtr & new_part,
        bool sync,
        const NamesAndTypesList * total_columns_list,
        MergeTreeData::DataPart::Checksums * additional_column_checksums)
{
    /// Finish write and get checksums.
    MergeTreeData::DataPart::ChecksumsPtr checksums_ptr = std::make_shared<MergeTreeData::DataPart::Checksums>();

    if (additional_column_checksums)
        *checksums_ptr = std::move(*additional_column_checksums);

    /// Finish columns serialization.
    writer->finish(*checksums_ptr, sync);

    for (const auto & [projection_name, projection_part] : new_part->getProjectionParts())
        checksums_ptr->addFile(
            projection_name + ".proj",
            projection_part->getChecksums()->getTotalSizeOnDisk(),
            projection_part->getChecksums()->getTotalChecksumUInt128());

    NamesAndTypesList part_columns;
    if (!total_columns_list)
        part_columns = columns_list;
    else
        part_columns = *total_columns_list;

    if (new_part->isStoredOnDisk())
        finalizePartOnDisk(new_part, part_columns, *checksums_ptr, sync);

    new_part->setColumns(part_columns);
    new_part->rows_count = rows_count;
    new_part->modification_time = time(nullptr);
    *(new_part->index) = writer->releaseIndexColumns();
    new_part->checksums_ptr = checksums_ptr;
    new_part->setBytesOnDisk(checksums_ptr->getTotalSizeOnDisk());
    new_part->index_granularity = writer->getIndexGranularity();
    new_part->calculateColumnsSizesOnDisk();
    if (default_codec != nullptr)
        new_part->default_codec = default_codec;
    new_part->storage.lockSharedData(*new_part);
}

void MergedBlockOutputStream::finalizePartOnDisk(
    const MergeTreeData::MutableDataPartPtr & new_part,
    NamesAndTypesList & part_columns,
    MergeTreeData::DataPart::Checksums & checksums,
    bool sync)
{

    if (new_part->isProjectionPart())
    {
        if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING || isCompactPart(new_part))
        {
            auto count_out = volume->getDisk()->writeFile(part_path + "count.txt", 4096);
            HashingWriteBuffer count_out_hashing(*count_out);
            writeIntText(rows_count, count_out_hashing);
            count_out_hashing.next();
            checksums.files["count.txt"].file_size = count_out_hashing.count();
            checksums.files["count.txt"].file_hash = count_out_hashing.getHash();
        }
    }
    else
    {
        if (new_part->uuid != UUIDHelpers::Nil)
        {
            auto out = volume->getDisk()->writeFile(fs::path(part_path) / IMergeTreeDataPart::UUID_FILE_NAME, 4096);
            HashingWriteBuffer out_hashing(*out);
            writeUUIDText(new_part->uuid, out_hashing);
            checksums.files[IMergeTreeDataPart::UUID_FILE_NAME].file_size = out_hashing.count();
            checksums.files[IMergeTreeDataPart::UUID_FILE_NAME].file_hash = out_hashing.getHash();
            out->finalize();
            if (sync)
                out->sync();
        }

        if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING || isCompactPart(new_part))
        {
            new_part->partition.store(storage, volume->getDisk(), part_path, checksums);
            if (new_part->minmax_idx.initialized)
                new_part->minmax_idx.store(storage, volume->getDisk(), part_path, checksums);
            else if (rows_count)
                throw Exception("MinMax index was not initialized for new non-empty part " + new_part->name
                        + ". It is a bug.", ErrorCodes::LOGICAL_ERROR);

            auto count_out = volume->getDisk()->writeFile(fs::path(part_path) / "count.txt", 4096);
            HashingWriteBuffer count_out_hashing(*count_out);
            writeIntText(rows_count, count_out_hashing);
            count_out_hashing.next();
            checksums.files["count.txt"].file_size = count_out_hashing.count();
            checksums.files["count.txt"].file_hash = count_out_hashing.getHash();
            count_out->finalize();
            if (sync)
                count_out->sync();
        }
    }

    {
        /// Write a file with versions
        auto out = volume->getDisk()->writeFile(fs::path(part_path) / "versions.txt", 4096);
        HashingWriteBuffer out_hashing(*out);
        new_part->versions->write(out_hashing);
        checksums.files["versions.txt"].file_size = out_hashing.count();
        checksums.files["versions.txt"].file_hash = out_hashing.getHash();
        out->finalize();
        if (sync)
            out->sync();
    }

    if (!new_part->ttl_infos.empty())
    {
        /// Write a file with ttl infos in json format.
        auto out = volume->getDisk()->writeFile(fs::path(part_path) / "ttl.txt", 4096);
        HashingWriteBuffer out_hashing(*out);
        new_part->ttl_infos.write(out_hashing);
        checksums.files["ttl.txt"].file_size = out_hashing.count();
        checksums.files["ttl.txt"].file_hash = out_hashing.getHash();
        out->finalize();
        if (sync)
            out->sync();
    }

    removeEmptyColumnsFromPart(new_part, part_columns, checksums);

    {
        /// Write a file with a description of columns.
        auto out = volume->getDisk()->writeFile(fs::path(part_path) / "columns.txt", 4096);
        part_columns.writeText(*out);
        out->finalize();
        if (sync)
            out->sync();
    }

    if (default_codec != nullptr)
    {
        auto out = volume->getDisk()->writeFile(part_path + IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME, 4096);
        DB::writeText(queryToString(default_codec->getFullCodecDesc()), *out);
        out->finalize();
    }
    else
    {
        throw Exception("Compression codec have to be specified for part on disk, empty for" + new_part->name
                + ". It is a bug.", ErrorCodes::LOGICAL_ERROR);
    }

    {
        /// Write file with checksums.
        auto out = volume->getDisk()->writeFile(fs::path(part_path) / "checksums.txt", 4096);
        checksums.versions = new_part->versions;
        checksums.write(*out);
        out->finalize();
        if (sync)
            out->sync();
    }
}

void MergedBlockOutputStream::writeImpl(const Block & block, const IColumn::Permutation * permutation)
{
    block.checkNumberOfRows();
    size_t rows = block.rows();
    if (!rows)
        return;

    writer->write(block, permutation);
    write_row_store_cost = writer->getWriteRowStoreCost();
    rows_count += rows;
}

}
