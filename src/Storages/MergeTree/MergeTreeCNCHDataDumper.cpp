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

#include "Storages/MergeTree/MergeTreeCNCHDataDumper.h"

#include <metric_helper.h>
#include <Core/UUID.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Disks/HDFS/DiskHDFS.h>
#include <DataTypes/MapHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>
#include <IO/copyData.h>
#include <IO/LimitReadBuffer.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH.h>
#include <Storages/MergeTree/S3ObjectMetadata.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/filesystemHelpers.h>
#include <common/logger_useful.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <utility>

namespace ProfileEvents
{
extern const Event CnchDumpParts;
extern const Event CnchDumpPartsElapsedMilliseconds;
extern const Event CnchDumpPartsBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_CNCH_DATA_FILE;
    extern const int NOT_CONFIG_CLOUD_STORAGE;
    extern const int FILE_DOESNT_EXIST;
    extern const int NO_FILE_IN_DATA_PART;
}

static const std::vector<String> EXTENSION_LIST_FOR_TEMP_PART{
    DATA_FILE_EXTENSION,
    MARKS_FILE_EXTENSION,
    BITMAP_IDX_EXTENSION,
    BITMAP_IRK_EXTENSION,
    SEGMENT_BITMAP_IDX_EXTENSION,
    SEGMENT_BITMAP_TABLE_EXTENSION,
    SEGMENT_BITMAP_DIRECTORY_EXTENSION,
    COMPRESSED_DATA_INDEX_EXTENSION,
    GIN_SEGMENT_ID_FILE_EXTENSION,
    GIN_SEGMENT_METADATA_FILE_EXTENSION,
    GIN_DICTIONARY_FILE_EXTENSION,
    GIN_POSTINGS_FILE_EXTENSION};

MergeTreeCNCHDataDumper::MergeTreeCNCHDataDumper(
    MergeTreeMetaBase & data_,
    const S3ObjectMetadata::PartGeneratorID & generator_id_,
    const String & magic_code_,
    const MergeTreeDataFormatVersion version_)
    : data(data_)
    , generator_id(generator_id_)
    , log(&Poco::Logger::get(data.getLogName() + "(CNCHDumper)"))
    , magic_code(magic_code_)
    , version(version_)
{
}

void MergeTreeCNCHDataDumper::writeDataFileHeader(WriteBuffer & to, MutableMergeTreeDataPartCNCHPtr & part) const
{
    writeString(magic_code, to);
    writeIntBinary(version.toUnderType(), to);
    writeBoolText(part->deleted, to);
    writeNull(MERGE_TREE_STORAGE_CNCH_DATA_HEADER_SIZE - to.count(), to);
}

void MergeTreeCNCHDataDumper::writeDataFileFooter(WriteBuffer & to, const CNCHDataMeta & meta) const
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
    writeNull(MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE - sizeof(meta), to);
}

/// Check correctness of data file in remote storage,
/// Now we only check data file length.
size_t MergeTreeCNCHDataDumper::check(
    MergeTreeDataPartCNCHPtr remote_part, const std::shared_ptr<MergeTreeDataPartChecksums> & checksums, const CNCHDataMeta & meta, bool skip_check) const
{
    size_t data_files_size = MERGE_TREE_STORAGE_CNCH_DATA_HEADER_SIZE;
    if (checksums)
    {
        for (auto & file : checksums->files)
        {
            if (!file.second.is_deleted)
                data_files_size += file.second.file_size;
        }
    }
    data_files_size += (meta.index_size + meta.checksums_size + meta.meta_info_size + meta.unique_key_index_size);
    data_files_size += MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE;

    if (skip_check)
        return data_files_size;

    DiskPtr remote_disk = remote_part->volume->getDisk();
    String part_data_rel_path = remote_part->getFullRelativePath() + "data";
    LOG_DEBUG(&Poco::Logger::get("MergeTreeCNCHDataDumper::check"), "Checking part {} from {}\n", remote_part->name, part_data_rel_path);

    size_t cnch_data_file_size = remote_disk->getFileSize(part_data_rel_path);

    if (data_files_size != cnch_data_file_size)
    {
        throw Exception(
            fmt::format(
                "Failed to check data in remote, path: {}, size: {}, local_size: {}",
                fullPath(remote_disk, part_data_rel_path),
                cnch_data_file_size,
                data_files_size),
            ErrorCodes::BAD_CNCH_DATA_FILE);
    }

    if (meta.checksums_size != 0)
    {
        std::unique_ptr<ReadBufferFromFileBase> reader = remote_disk->readFile(part_data_rel_path, data.getContext()->getReadSettings());
        reader->seek(meta.checksums_offset);
        assertString("checksums format version: ", *reader);
    }
    return data_files_size;
}

/// Dump local part to vfs
MutableMergeTreeDataPartCNCHPtr MergeTreeCNCHDataDumper::dumpTempPart(
    const IMutableMergeTreeDataPartPtr & local_part, const DiskPtr & remote_disk, bool is_temp_prefix) const
{
    ProfileEvents::increment(ProfileEvents::CnchDumpParts);
    Stopwatch watch;
    SCOPE_EXIT({
        ProfileEvents::increment(ProfileEvents::CnchDumpPartsElapsedMilliseconds, watch.elapsedMilliseconds(), Metrics::MetricType::Timer);
    });

    const String TMP_PREFIX = "tmp_dump_";
    MergeTreePartInfo new_part_info(
        local_part->info.partition_id,
        local_part->info.min_block,
        local_part->info.max_block,
        local_part->info.level,
        local_part->info.mutation,
        local_part->info.hint_mutation);

    /// if local part has remote disk name, select remote disk by name, else select
    /// remote disk with RR
    DiskPtr disk = remote_disk == nullptr ? data.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk() : remote_disk;
    VolumeSingleDiskPtr volume = std::make_shared<SingleDiskVolume>("temp_volume", disk);
    MutableMergeTreeDataPartCNCHPtr new_part = nullptr;

    new_part_info.storage_type = disk->getType();
    String part_name = new_part_info.getPartName();

    LOG_DEBUG(log, "Disk type : " + DiskType::toString(disk->getType()));

    switch (disk->getType())
    {
        case DiskType::Type::ByteHDFS: {
            String relative_path
                = is_temp_prefix ? TMP_PREFIX + new_part_info.getPartNameWithHintMutation() : new_part_info.getPartNameWithHintMutation();
            new_part = std::make_shared<MergeTreeDataPartCNCH>(data, part_name, new_part_info, volume, relative_path);
            break;
        }
        case DiskType::Type::ByteS3: {
            if (is_temp_prefix)
            {
                throw Exception("Temp prefix is not supported for s3 part", ErrorCodes::LOGICAL_ERROR);
            }
            UUID part_id = local_part->uuid;
            String relative_path = UUIDHelpers::UUIDToString(part_id);
            LOG_DEBUG(log, "Relative path : " + relative_path);
            new_part = std::make_shared<MergeTreeDataPartCNCH>(data, part_name, new_part_info, volume, relative_path, nullptr, part_id);
            break;
        }
        default:
            throw Exception("Unsupported disk type when dump part to remote", ErrorCodes::LOGICAL_ERROR);
    }
    new_part->fromLocalPart(*local_part);
    String new_part_rel_path = new_part->getFullRelativePath();
    disk->createDirectories(new_part_rel_path);

    /// CheckSums & Primary Index will be stored in cloud data file,
    /// Other meta info will be stored to catalog serice,
    /// Here, we clear meta files in checksums.
    auto erase_file_in_checksums = [new_part](const String & file_name) {
        if (new_part->checksums_ptr == nullptr)
            return;

        new_part->checksums_ptr->files.erase(file_name);
    };
    erase_file_in_checksums("ttl.txt");
    erase_file_in_checksums("count.txt");
    erase_file_in_checksums("columns.txt");
    erase_file_in_checksums("partition.dat");
    MergeTreeDataPartChecksum index_checksum;
    if (new_part->checksums_ptr && new_part->checksums_ptr->files.find("primary.idx") != new_part->checksums_ptr->files.end())
        index_checksum = new_part->checksums_ptr->files.at("primary.idx");
    erase_file_in_checksums("primary.idx");

    /// remove minmax index in checksums
    {
        auto metadata_snapshot = data.getInMemoryMetadataPtr();
        const auto & partition_key = metadata_snapshot->getPartitionKey();
        auto minmax_column_names = data.getMinMaxColumnsNames(partition_key);
        for (const auto & minmax_column_name : minmax_column_names)
        {
            String minmax_file_name = "minmax_" + escapeForFileName(minmax_column_name) + ".idx";
            erase_file_in_checksums(minmax_file_name);
        }
    }

    MergeTreeDataPartChecksum uki_checksum; /// unique key index checksum
    if (new_part->checksums_ptr && new_part->checksums_ptr->files.find("unique_key.idx") != new_part->checksums_ptr->files.end())
        uki_checksum = new_part->checksums_ptr->files.at("unique_key.idx");
    erase_file_in_checksums("unique_key.idx");

    std::vector<MergeTreeDataPartChecksums::FileChecksums::value_type *> reordered_checksums;

    /// Data files offset
    size_t data_file_offset = MERGE_TREE_STORAGE_CNCH_DATA_HEADER_SIZE;
    if (new_part->checksums_ptr)
    {
        auto & checksums_files = new_part->checksums_ptr->files;
        reordered_checksums.reserve(checksums_files.size());

        std::unordered_set<String> key_streams;
        ISerialization::SubstreamPath path;
        for (const auto & k_it : getKeyColumns())
        {
            const auto & column_name = k_it.name;
            const auto & column_type = k_it.type;
            column_type->enumerateStreams(
                IDataType::getSerialization(k_it), [&](const ISerialization::SubstreamPath & substream_path, const IDataType &) {
                    String stream_name = ISerialization::getFileNameForStream(column_name, substream_path);
                    for (const auto & extension : EXTENSION_LIST_FOR_TEMP_PART)
                    {
                        if (auto it = checksums_files.find(stream_name + extension); it != checksums_files.end() && !it->second.is_deleted)
                        {
                            reordered_checksums.push_back(&*it);
                            key_streams.emplace(stream_name + extension);
                        }
                    }
                });
        }

        for (auto & file : checksums_files)
        {
            // do not add the projection directory here
            if (!file.second.is_deleted && !key_streams.count(file.first) && !endsWith(file.first, ".proj"))
                reordered_checksums.push_back(&file);
        }

        for (auto & file : reordered_checksums)
        {
            file->second.file_offset = data_file_offset;
            data_file_offset += file->second.file_size;
        }
    }

    /// Write data file
    String data_file_rel_path = joinPaths({new_part_rel_path, "data"});
    CNCHDataMeta meta;
    {
        /// When we write part, we will attach generator's id into this part.
        /// And if transaction is rollback, we will compare this metadata and
        /// determinte if this part is generated by us and valid to remove
        LOG_TRACE(log, "Writing part {} to {}", new_part->name, new_part_rel_path);

        WriteSettings write_settings;
        write_settings.mode = WriteMode::Create;
        write_settings.file_meta.insert(std::pair<String, String>(S3ObjectMetadata::PART_GENERATOR_ID, generator_id.str()));

        auto data_out = disk->writeFile(data_file_rel_path, write_settings);
        SCOPE_EXIT({
            if (data_out)
                ProfileEvents::increment(ProfileEvents::CnchDumpPartsBytes, data_out->count(), Metrics::MetricType::Rate);
        });

        writeDataFileHeader(*data_out, new_part);

        const DiskPtr & local_part_disk = local_part->volume->getDisk();
        bool enable_compact_map_data = local_part->versions->enable_compact_map_data;
        LOG_TRACE(log, "Getting local disk {} at {}, enable_compact_map_data is {}\n",
            local_part_disk->getName(), local_part_disk->getPath(), enable_compact_map_data);

        if (new_part->checksums_ptr)
        {
            for (auto * file_ptr : reordered_checksums)
            {
                auto & file = *file_ptr;

                if (enable_compact_map_data && isMapImplicitKey(file.first))
                {
                    String file_path = local_part->getFullPath() + "/" + getMapFileNameFromImplicitFileName(file.first);
                    auto & local_part_checksums_files = local_part->checksums_ptr->files;
                    UInt64 offset = local_part_checksums_files[file.first].file_offset;
                    UInt64 file_size = local_part_checksums_files[file.first].file_size;
                    ReadBufferFromFile from(file_path);
                    from.seek(offset, SEEK_SET);
                    LimitReadBuffer in(from, file_size, false);
                    copyData(in, *data_out);
                }
                else
                {
                    String file_rel_path = local_part->getFullRelativePath() + file.first;
                    String file_full_path = local_part->getFullPath() + file.first;
                    if (!local_part_disk->fileExists(file_rel_path))
                        throw Exception(
                            "Fail to dump local file: " + file_rel_path + " because file doesn't exists", ErrorCodes::FILE_DOESNT_EXIST);

                    ReadBufferFromFile from(file_full_path);
                    copyData(from, *data_out);
                }

                data_out->next();

                /// TODO: fix getPositionInFile
                if (file.second.file_offset + file.second.file_size != static_cast<UInt64>(data_out->count()))
                {
                    throw Exception(
                        file.first + " in data part " + part_name + " check error, checksum offset: "
                            + std::to_string(file.second.file_offset) + " checksums size: " + std::to_string(file.second.file_size),
                        ErrorCodes::BAD_CNCH_DATA_FILE);
                }
            }
        }

        /// Write projection parts
        for (const auto & [projection_name, projection_part] : new_part->getProjectionParts())
        {
            if (auto it = new_part->checksums_ptr->files.find(projection_name + ".proj");
                it != new_part->checksums_ptr->files.end() && !it->second.is_deleted)
            {
                projection_part->info.storage_type = StorageType::ByteHDFS;
                if (projection_part->checksums_ptr)
                    projection_part->checksums_ptr->storage_type = StorageType::ByteHDFS;
                else
                {
                    /// anyway we need a checksums
                    projection_part->checksums_ptr = std::make_shared<MergeTreeDataPartChecksums>();
                    projection_part->checksums_ptr->storage_type = StorageType::ByteHDFS;
                }
                size_t current_file_offset = writeProjectionPart(projection_name, projection_part, data_out.get(), data_file_offset);
                it->second.file_offset = data_file_offset;
                it->second.file_size = current_file_offset - data_file_offset;
                data_file_offset = current_file_offset;
            }
            else
            {
                throw Exception("Projection " + projection_name + " is missed in data part " + part_name, ErrorCodes::NO_FILE_IN_DATA_PART);
            }
        }

        /// Primary index
        off_t index_offset = data_file_offset;
        String index_file_rel_path = local_part->getFullRelativePath() + "primary.idx";
        String index_file_full_path = local_part->getFullPath() + "primary.idx";
        size_t index_size = 0;
        uint128 index_hash;
        if (local_part_disk->fileExists(index_file_rel_path))
        {
            ReadBufferFromFile from(index_file_full_path);
            copyData(from, *data_out);
            index_size = index_checksum.file_size;
            index_hash = index_checksum.file_hash;
            if (index_offset + index_size != static_cast<UInt64>(data_out->count()))
            {
                throw Exception(
                    ErrorCodes::BAD_CNCH_DATA_FILE,
                    "primary.idx in data part {} check error, index offset: {} index size: {} disk size: {}",
                    part_name,
                    index_offset,
                    index_size,
                    local_part_disk->getFileSize(index_file_rel_path));
            }
        }

        /// Checksums
        off_t checksums_offset = index_offset + index_size;
        size_t checksums_size = 0;
        uint128 checksums_hash;
        if (new_part->checksums_ptr)
        {
            HashingWriteBuffer checksums_hashing(*data_out);
            new_part->checksums_ptr->write(checksums_hashing);
            checksums_hashing.next();
            checksums_size = data_out->count() - checksums_offset;
            checksums_hash = checksums_hashing.getHash();
            if (checksums_offset + checksums_size != static_cast<UInt64>(data_out->count()))
            {
                throw Exception(
                    "checksums.txt in data part " + part_name + " check error, checksum offset: " + std::to_string(index_offset)
                        + " checksums size: " + std::to_string(index_size)
                        + "disk size: " + std::to_string(local_part_disk->getFileSize(index_file_rel_path)),
                    ErrorCodes::BAD_CNCH_DATA_FILE);
            }
        }

        /// MetaInfo
        off_t meta_info_offset = checksums_offset + checksums_size;
        size_t meta_info_size = 0;
        uint128 meta_info_hash;
        {
            HashingWriteBuffer meta_info_hashing(*data_out);
            writePartBinary(*new_part, meta_info_hashing);
            meta_info_hashing.next();
            meta_info_size = data_out->count() - meta_info_offset;
            meta_info_hash = meta_info_hashing.getHash();
            if (meta_info_offset + meta_info_size != static_cast<UInt64>(data_out->count()))
            {
                throw Exception(
                    "meta info in data part " + part_name + " check error, meta offset: " + std::to_string(index_offset) + " meta size: "
                        + std::to_string(index_size) + "disk size: " + std::to_string(local_part_disk->getFileSize(index_file_rel_path)),
                    ErrorCodes::BAD_CNCH_DATA_FILE);
            }
        }

        /// Unique Key Index
        if (data.getInMemoryMetadataPtr()->hasUniqueKey() && new_part->rows_count > 0 && !new_part->isPartial())
        {
            uki_checksum.file_offset = meta_info_offset + meta_info_size;
            String file_rel_path = local_part->getFullRelativePath() + "unique_key.idx";
            String file_full_path = local_part->getFullPath() + "unique_key.idx";
            if (!local_part_disk->fileExists(file_rel_path))
                throw Exception(
                    "unique_key.idx not found in part " + part_name + ", table " + data.getStorageID().getNameForLogs(),
                    ErrorCodes::FILE_DOESNT_EXIST);
            ReadBufferFromFile from(file_full_path);
            copyData(from, *data_out);
            data_out->next();
        }

        /// Data footer
        meta = CNCHDataMeta{
            index_offset,
            index_size,
            index_hash,
            checksums_offset,
            checksums_size,
            checksums_hash,
            meta_info_offset,
            meta_info_size,
            meta_info_hash,
            static_cast<off_t>(uki_checksum.file_offset),
            uki_checksum.file_size,
            uki_checksum.file_hash};
        writeDataFileFooter(*data_out, meta);
        data_out->finalize();
    }

    size_t bytes_on_disk = check(new_part, new_part->checksums_ptr, meta, local_part->deleted);

    new_part->modification_time = time(nullptr);
    /// Merge fetcher may use this value to calculate segment size,
    /// so bytes_on_disk uses checked value to ensure accuracy.
    new_part->bytes_on_disk = bytes_on_disk;

    return new_part;
}

NamesAndTypesList MergeTreeCNCHDataDumper::getKeyColumns() const
{
    Names sort_key_columns_vec = data.getInMemoryMetadataPtr()->getSortingKeyColumns();
    std::set<String> key_columns(sort_key_columns_vec.cbegin(), sort_key_columns_vec.cend());

    auto & secondary_indices = data.getInMemoryMetadataPtr()->getSecondaryIndices();
    for (const auto & index : secondary_indices)
    {
        const auto & index_columns_vec = index.column_names;
        std::copy(index_columns_vec.cbegin(), index_columns_vec.cend(), std::inserter(key_columns, key_columns.end()));
    }

    const auto & merging_params = data.merging_params;

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
    auto all_columns = data.getInMemoryMetadataPtr()->getColumns().getAllPhysical();
    for (const auto & column : all_columns)
    {
        if (key_columns.count(column.name))
            merging_columns.emplace_back(column);
    }
    return merging_columns;
}

size_t MergeTreeCNCHDataDumper::writeProjectionPart(
    const String & projection_name, const IMutableMergeTreeDataPartPtr projection_part, WriteBuffer * out, size_t data_file_offset) const
{
    const auto & projection_description = data.getInMemoryMetadataPtr()->getProjections().get(projection_name);
    auto erase_file_in_checksums = [projection_part](const String & file_name) {
        if (projection_part->checksums_ptr == nullptr)
            return;

        projection_part->checksums_ptr->files.erase(file_name);
    };
    erase_file_in_checksums("ttl.txt");
    erase_file_in_checksums("count.txt");
    erase_file_in_checksums("columns.txt");
    //    erase_file_in_checksums("partition.dat");
    MergeTreeDataPartChecksum index_checksum;
    if (projection_part->checksums_ptr
        && projection_part->checksums_ptr->files.find("primary.idx") != projection_part->checksums_ptr->files.end())
        index_checksum = projection_part->checksums_ptr->files.at("primary.idx");
    erase_file_in_checksums("primary.idx");

    MergeTreeDataPartChecksum uki_checksum; /// unique key index checksum
    if (projection_part->checksums_ptr
        && projection_part->checksums_ptr->files.find("unique_key.idx") != projection_part->checksums_ptr->files.end())
        uki_checksum = projection_part->checksums_ptr->files.at("unique_key.idx");
    erase_file_in_checksums("unique_key.idx");

    std::vector<MergeTreeDataPartChecksums::FileChecksums::value_type *> reordered_checksums;

    if (projection_part->checksums_ptr)
    {
        auto & checksums_files = projection_part->checksums_ptr->files;
        reordered_checksums.reserve(checksums_files.size());
        for (const auto & col_name : projection_description.column_names)
        {
            const auto & name = ISerialization::getFileNameForStream(col_name, {});
            for (const auto & extension : {".bin", ".mrk"})
            {
                if (auto it = checksums_files.find(name + extension); it != checksums_files.end() && !it->second.is_deleted)
                {
                    reordered_checksums.push_back(&*it);
                }
                else
                {
                    LOG_ERROR(log, "Fail to find column {} in projection {}", name + extension, projection_name);
                }
            }
        }
        for (auto & file : reordered_checksums)
        {
            file->second.file_offset = data_file_offset;
            data_file_offset += file->second.file_size;
        }
    }

    // Write data file
    CNCHDataMeta meta;
    {
        const DiskPtr & proj_part_disk = projection_part->volume->getDisk();
        LOG_TRACE(log, "Getting local disk for projection {} at {}\n", proj_part_disk->getName(), proj_part_disk->getPath());

        if (projection_part->checksums_ptr)
        {
            for (auto * file_ptr : reordered_checksums)
            {
                auto & file = *file_ptr;

                String file_rel_path = projection_part->getFullRelativePath() + file.first;
                String file_full_path = projection_part->getFullPath() + file.first;
                if (!proj_part_disk->fileExists(file_rel_path))
                    throw Exception(
                        "Fail to dump projection file: " + file_rel_path + " because file doesn't exists", ErrorCodes::FILE_DOESNT_EXIST);

                ReadBufferFromFile from(file_full_path);
                copyData(from, *out);
                out->next();
                /// TODO: fix getPositionInFile
                if (file.second.file_offset + file.second.file_size != static_cast<UInt64>(out->count()))
                {
                    throw Exception(
                        file.first + " in projection part " + projection_name + " check error, checksum offset: "
                            + std::to_string(file.second.file_offset) + " checksums size: " + std::to_string(file.second.file_size)
                            + "disk size: " + std::to_string(proj_part_disk->getFileSize(file_rel_path)),
                        ErrorCodes::BAD_CNCH_DATA_FILE);
                }
            }
        }

        /// Primary index
        off_t index_offset = data_file_offset;
        String index_file_rel_path = projection_part->getFullRelativePath() + "primary.idx";
        String index_file_full_path = projection_part->getFullPath() + "primary.idx";
        size_t index_size = 0;
        uint128 index_hash;
        if (proj_part_disk->fileExists(index_file_rel_path))
        {
            ReadBufferFromFile from(index_file_full_path);
            copyData(from, *out);
            index_size = index_checksum.file_size;
            index_hash = index_checksum.file_hash;
            out->next();
            if (index_offset + index_size != static_cast<UInt64>(out->count()))
            {
                throw Exception(
                    ErrorCodes::BAD_CNCH_DATA_FILE,
                    "primary.idx in projection part {} check error, index offset: {} index size: {} disk size: {}",
                    projection_name,
                    index_offset,
                    index_size,
                    proj_part_disk->getFileSize(index_file_rel_path));
            }
        }

        /// Checksums
        off_t checksums_offset = index_offset + index_size;
        size_t checksums_size = 0;
        String checksum_file_rel_path = projection_part->getFullRelativePath() + "checksums.txt";
        uint128 checksums_hash;
        if (projection_part->checksums_ptr)
        {
            HashingWriteBuffer checksums_hashing(*out);
            projection_part->checksums_ptr->write(checksums_hashing);
            checksums_hashing.next();
            checksums_size = out->count() - checksums_offset;
            checksums_hash = checksums_hashing.getHash();
            if (checksums_offset + checksums_size != static_cast<UInt64>(out->count()))
            {
                throw Exception(
                    "checksums.txt in projection part " + projection_name + " check error, checksum offset: "
                        + std::to_string(checksums_offset) + " checksums size: " + std::to_string(checksums_size)
                        + "disk size: " + std::to_string(proj_part_disk->getFileSize(checksum_file_rel_path)),
                    ErrorCodes::BAD_CNCH_DATA_FILE);
            }
        }

        /// MetaInfo
        off_t meta_info_offset = checksums_offset + checksums_size;
        size_t meta_info_size = 0;
        uint128 meta_info_hash;
        {
            HashingWriteBuffer meta_info_hashing(*out);
            writeProjectionBinary(*projection_part, meta_info_hashing);
            meta_info_hashing.next();
            meta_info_size = out->count() - meta_info_offset;
            meta_info_hash = meta_info_hashing.getHash();
            if (meta_info_offset + meta_info_size != static_cast<UInt64>(out->count()))
            {
                throw Exception(
                    "meta info in projection part " + projection_name + " check error, meta offset: " + std::to_string(meta_info_offset)
                        + " meta size: " + std::to_string(meta_info_size),
                    ErrorCodes::BAD_CNCH_DATA_FILE);
            }
        }

        /// Data footer
        off_t footer_offset = meta_info_offset + meta_info_size;
        meta = CNCHDataMeta{
            index_offset,
            index_size,
            index_hash,
            checksums_offset,
            checksums_size,
            checksums_hash,
            meta_info_offset,
            meta_info_size,
            meta_info_hash,
            static_cast<off_t>(uki_checksum.file_offset),
            uki_checksum.file_size,
            uki_checksum.file_hash};
        writeDataFileFooter(*out, meta);
        out->next();
        data_file_offset = footer_offset + MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE;
        if (data_file_offset != static_cast<UInt64>(out->count()))
        {
            throw Exception(
                "data footer in projection part " + projection_name + " check error, footer offset: " + std::to_string(footer_offset)
                    + " footer size: " + std::to_string(MERGE_TREE_STORAGE_CNCH_DATA_FOOTER_SIZE),
                ErrorCodes::BAD_CNCH_DATA_FILE);
        }
    }

    return data_file_offset;
}
}

























