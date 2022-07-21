#include "Storages/MergeTree/MergeTreeCNCHDataDumper.h"

#include <Disks/HDFS/DiskHDFS.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Common/escapeForFileName.h>

#include <chrono>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_CNCH_DATA_FILE;
    extern const int NOT_CONFIG_CLOUD_STORAGE;
    extern const int FILE_DOESNT_EXIST;
}

MergeTreeCNCHDataDumper::MergeTreeCNCHDataDumper(
    MergeTreeMetaBase & data_, const String & magic_code_, const MergeTreeDataFormatVersion version_)
    : data(data_), log(&Poco::Logger::get(data.getLogName() + "(CNCHDumper)"))
    , magic_code(magic_code_), version(version_)
{
}

void MergeTreeCNCHDataDumper::writeDataFileHeader(WriteBuffer & to, MutableMergeTreeDataPartCNCHPtr & part)
{
    writeString(magic_code, to);
    writeIntBinary(version.toUnderType(), to);
    writeBoolText(part->deleted, to);
    writeNull(MERGE_TREE_STORAGE_CNCH_DATA_HEADER_SIZE - to.count(), to);
}

void MergeTreeCNCHDataDumper::writeDataFileFooter(WriteBuffer & to, const CNCHDataMeta & meta)
{
    writeIntBinary(meta.index_offset, to);
    writeIntBinary(meta.index_size, to);
    writeIntBinary(meta.index_checksum, to);
    writeIntBinary(meta.checksums_offset, to);
    writeIntBinary(meta.checksums_size, to);
    writeIntBinary(meta.checksums_checksum, to);
    writeIntBinary(meta.meta_info_offset, to);
    writeIntBinary(meta.meta_info_size, to);
    writeIntBinary(meta.meta_info_checksum, to);
    writeIntBinary(meta.unique_key_index_offset, to);
    writeIntBinary(meta.unique_key_index_size, to);
    writeIntBinary(meta.unique_key_index_checksum, to);
    /// TODO: FIX write related function
    // writePODBinary(meta.key, to);
    writeNull(MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE - sizeof(meta), to);
}

/// Check correctness of data file in remote storage,
/// Now we only check data file length.
size_t MergeTreeCNCHDataDumper::check(MergeTreeDataPartCNCHPtr remote_part, const std::shared_ptr<MergeTreeDataPartChecksums> & checksums, const CNCHDataMeta & meta)
{
    DiskPtr remote_disk = remote_part->volume->getDisk();
    String part_data_rel_path = remote_part->getFullRelativePath() + "data";

    size_t cnch_data_file_size = remote_disk->getFileSize(part_data_rel_path);
    size_t data_files_size = MERGE_TREE_STORAGE_CNCH_DATA_HEADER_SIZE;
    if(checksums)
    {
        for(auto & file : checksums->files)
        {
            data_files_size += file.second.file_size;
        }
    }
    data_files_size += (meta.index_size + meta.checksums_size + meta.meta_info_size + meta.unique_key_index_size);
    data_files_size += MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE;

    if(data_files_size != cnch_data_file_size)
    {
        throw Exception(fmt::format("Failed to check data in remote, path: {}, size: {}, local_size: {}",
            fullPath(remote_disk, part_data_rel_path), cnch_data_file_size, data_files_size),
            ErrorCodes::BAD_CNCH_DATA_FILE);
    }

    if(meta.checksums_size != 0)
    {
        std::unique_ptr<ReadBufferFromFileBase> reader = remote_disk->readFile(
            part_data_rel_path);
        reader->seek(meta.checksums_offset);
        assertString("checksums format version: ", *reader);
    }
    return data_files_size;
}

/// Dump local part to vfs
MutableMergeTreeDataPartCNCHPtr MergeTreeCNCHDataDumper::dumpTempPart(
    const IMutableMergeTreeDataPartPtr & local_part,
    [[maybe_unused]]const HDFSConnectionParams & hdfs_params,
    bool is_temp_prefix,
    const DiskPtr & remote_disk )
{
    const String TMP_PREFIX = "tmp_dump_";
    String partition_id = local_part->info.partition_id;
    Int64 min_block = local_part->info.min_block;
    Int64 max_block = local_part->info.max_block;
    UInt32 merge_tree_level = local_part->info.level;
    Int64 mutation = local_part->info.mutation;
    Int64 hint_mutation = local_part->info.hint_mutation;
    MergeTreePartInfo new_part_info(partition_id, min_block, max_block, merge_tree_level, mutation, hint_mutation);
    new_part_info.storage_type = StorageType::HDFS;

    String part_name = new_part_info.getPartName();
    String relative_path;
    if(is_temp_prefix)
        relative_path = fs::path(data.getRelativeDataPath()) / (TMP_PREFIX + new_part_info.getPartName(true));
    else
        relative_path = fs::path(data.getRelativeDataPath()) / new_part_info.getPartName(true);

    DiskPtr disk = remote_disk == nullptr ? data.getStoragePolicy()->getAnyDisk() : remote_disk;
    VolumeSingleDiskPtr volume = std::make_shared<SingleDiskVolume>("temp_volume", disk);
    MutableMergeTreeDataPartCNCHPtr new_part = std::make_shared<MergeTreeDataPartCNCH>(
        data, part_name, new_part_info, volume, relative_path
    );

    new_part->partition.assign(local_part->partition);
    if (local_part->prepared_checksums)
    {
        new_part->prepared_checksums = std::make_shared<MergeTreeDataPartChecksums>(*local_part->prepared_checksums);
        new_part->prepared_checksums->storage_type = StorageType::HDFS;
    }
    new_part->minmax_idx = local_part->minmax_idx;
    new_part->rows_count = local_part->rows_count;
    /// TODO:
    // new_part->marks_count = local_part->marks_count;
    new_part->columns_ptr = std::make_shared<NamesAndTypesList>(*(local_part->columns_ptr));
    new_part->setPreparedIndex(local_part->getPreparedIndex());
    new_part->has_bitmap = local_part->has_bitmap.load();
    new_part->deleted = local_part->deleted;
    new_part->bucket_number = local_part->bucket_number;
    /// TODO:
    // new_part->bytes_on_disk = local_part->bytes_on_disk.load();
    new_part->table_definition_hash = data.getTableHashForClusterBy();
    new_part->columns_commit_time = local_part->columns_commit_time;
    new_part->mutation_commit_time = local_part->mutation_commit_time;
    new_part->min_unique_key = local_part->min_unique_key;
    new_part->max_unique_key = local_part->max_unique_key;
    /// TODO:
    // new_part->setAesEncrypter(local_part->getAesEncrypter());
    new_part->secondary_txn_id = local_part->secondary_txn_id;

    String new_part_rel_path = new_part->getFullRelativePath();
    if (disk->exists(new_part_rel_path))
    {
        LOG_WARNING(log, "Removing old temporary directory  {}", disk->getPath() + new_part_rel_path);
        disk->removeRecursive(new_part_rel_path);
    }
    disk->createDirectories(new_part_rel_path);

    /// CheckSums & Primary Index will be stored in cloud data file,
    /// Other meta info will be stored to catalog serice,
    /// Here, we clear meta files in checksums.
    auto erase_file_in_checksums = [new_part](const String & file_name)
    {
        if(new_part->prepared_checksums == nullptr)
            return;

        auto file = new_part->prepared_checksums->files.find(file_name);
        if(file != new_part->prepared_checksums->files.end())
            new_part->prepared_checksums->files.erase(file_name);
    };
    erase_file_in_checksums("ttl.txt");
    erase_file_in_checksums("count.txt");
    erase_file_in_checksums("columns.txt");
    erase_file_in_checksums("partition.dat");
    MergeTreeDataPartChecksum index_checksum;
    if (new_part->prepared_checksums && new_part->prepared_checksums->files.find("primary.idx") != new_part->prepared_checksums->files.end())
        index_checksum = new_part->prepared_checksums->files.at("primary.idx");
    erase_file_in_checksums("primary.idx");
    size_t minmax_idx_size = data.minmax_idx_column_types.size();
    for (size_t i = 0; i < minmax_idx_size; ++i)
    {
        String file_name = "minmax_" + escapeForFileName(data.minmax_idx_columns[i]) + ".idx";
        erase_file_in_checksums(file_name);
    }
    MergeTreeDataPartChecksum uki_checksum; /// unique key index checksum
    if (new_part->prepared_checksums && new_part->prepared_checksums->files.find("unique_key.idx") != new_part->prepared_checksums->files.end())
        uki_checksum = new_part->prepared_checksums->files.at("unique_key.idx");
    erase_file_in_checksums("unique_key.idx");

    std::vector<MergeTreeDataPartChecksums::FileChecksums::value_type *> reordered_checksums;

    /// Data files offset
    size_t data_file_offset = MERGE_TREE_STORAGE_CNCH_DATA_HEADER_SIZE;
    if (new_part->prepared_checksums)
    {
        auto & checksums_files = new_part->prepared_checksums->files;
        reordered_checksums.reserve(checksums_files.size());

        ///TODO: fix IDataType::SubstreamPath
        std::unordered_set<String> key_streams;
        // for (auto & [column_name, column_type] : getKeyColumns())
        // {
        //     column_type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path) {
        //         String stream_name = IDataType::getFileNameForStream(column_name, substream_path);
        //         for (auto & extension : {".bin", ".mrk"})
        //         {
        //             if (auto it = checksums_files.find(stream_name + extension); it != checksums_files.end() && !it->second.is_deleted)
        //             {
        //                 reordered_checksums.push_back(&*it);
        //                 key_streams.emplace(stream_name + extension);
        //             }
        //         }
        //     });
        // }

        for (auto & file : checksums_files)
        {
            if (!file.second.is_deleted && !key_streams.count(file.first))
                reordered_checksums.push_back(&file);
        }

        for (auto & file : reordered_checksums)
        {
            file->second.file_offset = data_file_offset;
            file->second.mutation = mutation;
            data_file_offset += file->second.file_size;
        }
    }
    // off_t index_offset = data_file_offset;

    /// Write data file
    CNCHDataMeta meta;
    {
        std::unique_ptr<WriteBufferFromFileBase> data_out = disk->writeFile(fs::path(relative_path) / "data");
        writeDataFileHeader(*data_out, new_part);

        // const DiskPtr& local_part_disk = local_part->getDisk();

        auto space_reservation = data.reserveSpace(local_part->getBytesOnDisk());
        const DiskPtr & local_part_disk = space_reservation->getDisk();

        if (new_part->prepared_checksums)
        {
            for (auto file_ptr : reordered_checksums)
            {
                auto & file = *file_ptr;

                String file_rel_path = local_part->getFullRelativePath() + file.first;
                String file_full_path = local_part->getFullPath() + file.first;
                if (!local_part_disk->exists(file_rel_path))
                    throw Exception("Fail to dump local file: " + file_full_path, ErrorCodes::FILE_DOESNT_EXIST);

                ReadBufferFromFile from(file_full_path);
                copyData(from, *data_out);
                data_out->next();
                ///TODO: fix getPositionInFile
                // if (file.second.file_offset + file.second.file_size != (UInt64)(data_out->getPositionInFile()))
                // {
                //     throw Exception(file.first + " in data part "  + part_name + " check error, checksum offset: " +
                //         std::to_string(file.second.file_offset) + " checksums size: " + std::to_string(file.second.file_size) +
                //         "disk size: " + std::to_string(local_part_disk->getFileSize(file_rel_path)), ErrorCodes::BAD_CNCH_DATA_FILE);
                // }
            }
        }

        /// Primary index
        String index_file_rel_path = local_part->getFullRelativePath() + "primary.idx";
        String index_file_full_path = local_part->getFullPath() + "primary.idx";
        size_t index_size = 0;
        uint128 index_hash;
        if (local_part_disk->exists(index_file_rel_path))
        {
            ReadBufferFromFile from(index_file_full_path);
            copyData(from, *data_out);
            index_size = index_checksum.file_size;
            index_hash = index_checksum.file_hash;
            data_out->next();
            ///TODO: fix getPositionInFile
            // if (index_offset + index_size != (UInt64)(data_out->getPositionInFile()))
            // {
            //      throw Exception("primary.idx in data part "  + part_name + " check error, checksum offset: " +
            //             std::to_string(index_offset) + " checksums size: " + std::to_string(index_size) +
            //             "disk size: " + std::to_string(local_part_disk->getFileSize(index_file_rel_path)), ErrorCodes::BAD_CNCH_DATA_FILE);
            // }
        }

        /// Checksums
        // off_t checksums_offset = index_offset + index_size;
        // size_t checksums_size = 0;
        uint128 checksums_hash;
        if (new_part->prepared_checksums)
        {
            HashingWriteBuffer checksums_hashing(*data_out);
            new_part->prepared_checksums->write(checksums_hashing);
            checksums_hashing.next();
            ///TODO: fix getPositionInFile
            // checksums_size = data_out->getPositionInFile() - checksums_offset;
            checksums_hash = checksums_hashing.getHash();
        }

        /// MetaInfo
        // off_t meta_info_offset = checksums_offset + checksums_size;
        // size_t meta_info_size = 0;
        uint128 meta_info_hash;
        {
            HashingWriteBuffer meta_info_hashing(*data_out);
            /// TODO fix writePartBinary
            // writePartBinary(*new_part, meta_info_hashing);
            meta_info_hashing.next();
            ///TODO: fix getPositionInFile
            // meta_info_size = data_out->getPositionInFile() - meta_info_offset;
            meta_info_hash = meta_info_hashing.getHash();
        }

        /// Unique Key Index
        /// TODO : fix unique key
        // if (data.hasUniqueKey() && new_part->rows_count > 0 && !new_part->isPartial())
        // {
        //     uki_checksum.file_offset = meta_info_offset + meta_info_size;
        //     String file_rel_path = local_part->getRelativePath() + "unique_key.idx";
        //     String file_full_path = local_part->getFullPath() + "unique_key.idx";
        //     if (!local_part_disk->exists(file_rel_path))
        //         throw Exception("unique_key.idx not found in part " + part_name + ", table " + data.getStorageID().getNameForLogs(),
        //                         ErrorCodes::FILE_DOESNT_EXIST);
        //     HashingWriteBuffer hashing_out(*data_out);
        //     ReadBufferFromFile from(file_full_path);
        //     copyData(from, hashing_out);
        //     hashing_out.next();
        //     uki_checksum.file_hash = hashing_out.getHash();
        // }

        /// TODO : fix AesKeyByteArray
        // AesEncrypt::AesKeyByteArray key {};
        // if(new_part->storage.settings.encrypt_table)
        // {
        //     key = new_part->getAesEncrypter()->getKeyByteArray();
        // }

        /// Data footer
        // meta = CNCHDataMeta{index_offset, index_size, index_hash,
        //                     checksums_offset, checksums_size, checksums_hash,
        //                     meta_info_offset, meta_info_size, meta_info_hash,
        //                     static_cast<off_t>(uki_checksum.file_offset), uki_checksum.file_size, uki_checksum.file_hash,
        //                     key};
        // writeDataFileFooter(*data_out, meta);
        data_out->next();
        data_out->sync();
    }

    size_t bytes_on_disk = check(new_part, new_part->prepared_checksums, meta);

    new_part->modification_time = time(nullptr);
    /// Merge fetcher may use this value to calculate segment size,
    /// so bytes_on_disk uses checked value to ensure accuracy.
    new_part->bytes_on_disk = bytes_on_disk;

    return new_part;
}

NamesAndTypesList MergeTreeCNCHDataDumper::getKeyColumns()
{
    Names sort_key_columns_vec = data.sorting_key_expr->getRequiredColumns();
    std::set<String> key_columns(sort_key_columns_vec.cbegin(), sort_key_columns_vec.cend());
    /// TODO : fix skip_indices
    // for (const auto & index : data.skip_indices)
    // {
    //     Names index_columns_vec = index->expr->getRequiredColumns();
    //     std::copy(index_columns_vec.cbegin(), index_columns_vec.cend(), std::inserter(key_columns, key_columns.end()));
    // }

    auto & merging_params = data.merging_params;

    /// Force sign column for Collapsing mode
    if (merging_params.mode == MergeTreeMetaBase::MergingParams::Collapsing)
        key_columns.emplace(merging_params.sign_column);

    /// Force version column for Replacing mode
    if (merging_params.mode == MergeTreeMetaBase::MergingParams::Replacing)
        key_columns.emplace(merging_params.version_column);

    /// Force sign column for VersionedCollapsing mode. Version is already in primary key.
    if (merging_params.mode == MergeTreeMetaBase::MergingParams::VersionedCollapsing)
        key_columns.emplace(merging_params.sign_column);

    NamesAndTypesList merging_columns;
    /// TODO: fix getColumns
    // auto all_columns = data.getColumns().getAllPhysical();
    // for (const auto & column : all_columns)
    // {
    //     if (key_columns.count(column.name))
    //         merging_columns.emplace_back(column);
    // }
    return merging_columns;
}

}
