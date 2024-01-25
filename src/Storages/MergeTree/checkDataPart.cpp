/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <algorithm>
#include <optional>

#include <Poco/DirectoryIterator.h>

#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/HashingReadBuffer.h>
#include <IO/LimitReadBuffer.h>
#include <Common/CurrentMetrics.h>
#include <DataTypes/DataTypeMap.h>


namespace CurrentMetrics
{
    extern const Metric ReplicatedChecks;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int UNKNOWN_PART_TYPE;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_MUNMAP;
    extern const int CANNOT_MREMAP;
    extern const int UNEXPECTED_FILE_IN_DATA_PART;
    extern const int CHECKSUM_DOESNT_MATCH;
}


bool isNotEnoughMemoryErrorCode(int code)
{
    /// Don't count the part as broken if there is not enough memory to load it.
    /// In fact, there can be many similar situations.
    /// But it is OK, because there is a safety guard against deleting too many parts.
    return code == ErrorCodes::MEMORY_LIMIT_EXCEEDED
        || code == ErrorCodes::CANNOT_ALLOCATE_MEMORY
        || code == ErrorCodes::CANNOT_MUNMAP
        || code == ErrorCodes::CANNOT_MREMAP;
}


void genCompactMapChecksums(
    MergeTreeData::DataPartPtr data_part,
    const DiskPtr & disk,
    const String & path,
    const NamesAndTypesList & map_columns_list,
    const IMergeTreeDataPart::Checksums & checksums_txt,
    IMergeTreeDataPart::Checksums & checksums_data,
    NameSet & skipped_files)
{
    const String & mrk_ext = data_part->getMarksFileExtension();

    /// This function calculates checksum for both compressed and decompressed contents of compact file.
    auto checksum_compact_file = [](const DiskPtr & disk_, const String & file_path, size_t offset, size_t size) {
        auto file_buf = disk_->readFile(file_path);
        file_buf->seek(offset);
        LimitReadBuffer limit_read_buf(*file_buf, size, false);
        HashingReadBuffer compressed_hashing_buf(limit_read_buf);
        CompressedReadBuffer uncompressing_buf(compressed_hashing_buf);
        HashingReadBuffer uncompressed_hashing_buf(uncompressing_buf);

        uncompressed_hashing_buf.ignoreAll();
        return IMergeTreeDataPart::Checksums::Checksum{
            compressed_hashing_buf.count(),
            compressed_hashing_buf.getHash(),
            uncompressed_hashing_buf.count(),
            uncompressed_hashing_buf.getHash()};
    };

    /// This function calculates checksum of compact mrk2 file, without compressed info.
    auto checksum_compact_mrk_file = [](const DiskPtr & disk_, const String & file_path, size_t offset, size_t size) {
        auto file_buf = disk_->readFile(file_path);
        file_buf->seek(offset);
        LimitReadBuffer limit_read_buf(*file_buf, size, false);
        HashingReadBuffer hashing_buf(limit_read_buf);

        hashing_buf.ignoreAll();
        return IMergeTreeDataPart::Checksums::Checksum{hashing_buf.count(), hashing_buf.getHash()};
    };

    for (const auto & column : map_columns_list)
    {
        const DataTypePtr & null_val_type_ptr = typeid_cast<const DataTypeMap &>(*column.type).getValueTypeForImplicitColumn();
        auto null_val_serial = null_val_type_ptr->getDefaultSerialization();

        null_val_serial->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path) {
            /// checksum_txt doesn't contain compact map files, need to skip in checkEqual step
            String base_file_name = ISerialization::getFileNameForStream(column.name, substream_path);
            skipped_files.insert(base_file_name + ".bin");
            skipped_files.insert(base_file_name + mrk_ext);
        });

        /// need to find all map keys from checksums
        for (const auto & it : checksums_txt.files)
        {
            const String & it_file_name = it.first;
            if (isMapImplicitDataFileNameNotBaseOfSpecialMapName(it_file_name, column.name))
            {
                const String & key_name = parseKeyNameFromImplicitFileName(it_file_name, column.name);
                const String & implicit_column = getImplicitColNameForMapKey(column.name, key_name);

                null_val_serial->enumerateStreams(
                    [&](const ISerialization::SubstreamPath & substream_path) {
                        /// for each stream, we need to add both .bin and .mrk2/.mrk3 to checksums
                        std::vector<String> bin_and_mrks_postfix{".bin", mrk_ext};
                        for (size_t i = 0; i < bin_and_mrks_postfix.size(); i++)
                        {
                            const String & postfix = bin_and_mrks_postfix[i];
                            String file_name = ISerialization::getFileNameForStream(implicit_column, substream_path) + postfix;
                            String map_file_name = ISerialization::getFileNameForStream(column.name, substream_path) + postfix;

                            /// get file offset and size of current stream
                            auto checksum_it = checksums_txt.files.find(file_name);
                            if (checksum_it == checksums_txt.files.end())
                                throw Exception(
                                    "Missing stream file " + file_name + " in checksums, column " + implicit_column + ".",
                                    ErrorCodes::CHECKSUM_DOESNT_MATCH);

                            size_t file_offset = checksum_it->second.file_offset;
                            size_t file_size = checksum_it->second.file_size;
                            if (postfix == String(".bin"))
                                checksums_data.files[file_name] = checksum_compact_file(disk, path + map_file_name, file_offset, file_size);
                            else
                                checksums_data.files[file_name]
                                    = checksum_compact_mrk_file(disk, path + map_file_name, file_offset, file_size);
                        }
                    });
            }
        }
    }
}


void genMapChecksums(
    const DiskPtr & disk, const String & path, const NamesAndTypesList & map_columns_list, IMergeTreeDataPart::Checksums & checksums_data)
{
    /// This function calculates checksum for both compressed and decompressed contents of compressed file.
    auto checksum_compressed_file = [](const DiskPtr & disk_, const String & file_path) {
        auto file_buf = disk_->readFile(file_path);
        HashingReadBuffer compressed_hashing_buf(*file_buf);
        CompressedReadBuffer uncompressing_buf(compressed_hashing_buf);
        HashingReadBuffer uncompressed_hashing_buf(uncompressing_buf);

        uncompressed_hashing_buf.ignoreAll();
        return IMergeTreeDataPart::Checksums::Checksum{
            compressed_hashing_buf.count(),
            compressed_hashing_buf.getHash(),
            uncompressed_hashing_buf.count(),
            uncompressed_hashing_buf.getHash()};
    };

    for (const auto & column : map_columns_list)
    {
        const DataTypePtr & null_val_type_ptr = typeid_cast<const DataTypeMap &>(*column.type).getValueTypeForImplicitColumn();
        auto null_val_serial = null_val_type_ptr->getDefaultSerialization();

        for (auto it = disk->iterateDirectory(path); it->isValid(); it->next())
        {
            const String & it_file_name = it->name();
            // find all implicit columns through files
            if (isMapImplicitDataFileNameOfSpecialMapName(it_file_name, column.name))
            {
                String implicit_column;
                if (isMapBaseFile(it_file_name))
                {
                    /// map base
                    implicit_column = getBaseNameForMapCol(column.name);
                }
                else
                {
                    /// map keys
                    const String & key_name = parseKeyNameFromImplicitFileName(it_file_name, column.name);
                    implicit_column = getImplicitColNameForMapKey(column.name, key_name);
                }

                null_val_serial->enumerateStreams(
                    [&](const ISerialization::SubstreamPath & substream_path) {
                        String file_name = ISerialization::getFileNameForStream(implicit_column, substream_path) + ".bin";
                        checksums_data.files[file_name] = checksum_compressed_file(disk, path + file_name);
                    });
            }
        }
    }
}


IMergeTreeDataPart::Checksums checkDataPart(
    MergeTreeData::DataPartPtr data_part,
    const DiskPtr & disk,
    const String & full_relative_path,
    const NamesAndTypesList & columns_list,
    const MergeTreeDataPartType & part_type,
    const NameSet & files_without_checksums,
    bool require_checksums,
    std::function<bool()> is_cancelled)
{
    /** Responsibility:
      * - read list of columns from columns.txt;
      * - read checksums if exist;
      * - validate list of columns and checksums
      */

    CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedChecks};

    String path = full_relative_path;
    if (!path.empty() && path.back() != '/')
        path += "/";

    NamesAndTypesList columns_txt;

    {
        auto buf = disk->readFile(fs::path(path) / "columns.txt");
        columns_txt.readText(*buf);
        assertEOF(*buf);
    }

    if (columns_txt != columns_list)
        throw Exception("Columns doesn't match in part " + path
            + ". Expected: " + columns_list.toString()
            + ". Found: " + columns_txt.toString(), ErrorCodes::CORRUPTED_DATA);

    /// Real checksums based on contents of data. Must correspond to checksums.txt. If not - it means the data is broken.
    IMergeTreeDataPart::Checksums checksums_data;

    /// This function calculates checksum for both compressed and decompressed contents of compressed file.
    auto checksum_compressed_file = [](const DiskPtr & disk_, const String & file_path)
    {
        auto file_buf = disk_->readFile(file_path);
        HashingReadBuffer compressed_hashing_buf(*file_buf);
        CompressedReadBuffer uncompressing_buf(compressed_hashing_buf);
        HashingReadBuffer uncompressed_hashing_buf(uncompressing_buf);

        uncompressed_hashing_buf.ignoreAll();
        return IMergeTreeDataPart::Checksums::Checksum
        {
            compressed_hashing_buf.count(), compressed_hashing_buf.getHash(),
            uncompressed_hashing_buf.count(), uncompressed_hashing_buf.getHash()
        };
    };

    /// This function calculates only checksum of file content (compressed or uncompressed).
    /// It also calculates checksum of projections.
    auto checksum_file = [&](const String & file_path, const String & file_name)
    {
        if (disk->isDirectory(file_path) && endsWith(file_name, ".proj") && !startsWith(file_name, "tmp_")) // ignore projection tmp merge dir
        {
            auto projection_name = file_name.substr(0, file_name.size() - sizeof(".proj") + 1);
            auto pit = data_part->getProjectionParts().find(projection_name);
            if (pit == data_part->getProjectionParts().end())
            {
                if (require_checksums)
                    throw Exception("Unexpected file " + file_name + " in data part", ErrorCodes::UNEXPECTED_FILE_IN_DATA_PART);
                else
                    return;
            }

            const auto & projection = pit->second;
            IMergeTreeDataPart::Checksums projection_checksums_data;
            const auto & projection_path = file_path;

            if (part_type == MergeTreeDataPartType::COMPACT)
            {
                auto proj_path = file_path + MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION;
                auto file_buf = disk->readFile(proj_path);
                HashingReadBuffer hashing_buf(*file_buf);
                hashing_buf.ignoreAll();
                projection_checksums_data.files[MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION] = IMergeTreeDataPart::Checksums::Checksum(hashing_buf.count(), hashing_buf.getHash());
            }
            else
            {
                const NamesAndTypesList & projection_columns_list = projection->getColumns();
                for (const auto & projection_column : projection_columns_list)
                {
                    auto serialization = IDataType::getSerialization(projection_column, [&](const String & stream_name)
                    {
                        return disk->exists(stream_name + DATA_FILE_EXTENSION);
                    });

                    serialization->enumerateStreams(
                        [&](const ISerialization::SubstreamPath & substream_path)
                        {
                            String projection_file_name = ISerialization::getFileNameForStream(projection_column, substream_path) + ".bin";
                            checksums_data.files[projection_file_name] = checksum_compressed_file(disk, projection_path + projection_file_name);
                        });
                }
            }

            IMergeTreeDataPart::Checksums projection_checksums_txt;

            if (require_checksums || disk->exists(projection_path + "checksums.txt"))
            {
                auto buf = disk->readFile(projection_path + "checksums.txt");
                projection_checksums_txt.read(*buf);
                assertEOF(*buf);
            }

            const auto & projection_checksum_files_txt = projection_checksums_txt.files;
            for (auto projection_it = disk->iterateDirectory(projection_path); projection_it->isValid(); projection_it->next())
            {
                const String & projection_file_name = projection_it->name();
                auto projection_checksum_it = projection_checksums_data.files.find(projection_file_name);

                /// Skip files that we already calculated. Also skip metadata files that are not checksummed.
                if (projection_checksum_it == projection_checksums_data.files.end() && !files_without_checksums.count(projection_file_name))
                {
                    auto projection_txt_checksum_it = projection_checksum_files_txt.find(file_name);
                    if (projection_txt_checksum_it == projection_checksum_files_txt.end()
                        || projection_txt_checksum_it->second.uncompressed_size == 0)
                    {
                        auto projection_file_buf = disk->readFile(projection_it->path());
                        HashingReadBuffer projection_hashing_buf(*projection_file_buf);
                        projection_hashing_buf.ignoreAll();
                        projection_checksums_data.files[projection_file_name] = IMergeTreeDataPart::Checksums::Checksum(
                            projection_hashing_buf.count(), projection_hashing_buf.getHash());
                    }
                    else
                    {
                        projection_checksums_data.files[projection_file_name] = checksum_compressed_file(disk, projection_it->path());
                    }
                }
            }
            checksums_data.files[file_name] = IMergeTreeDataPart::Checksums::Checksum(
                projection_checksums_data.getTotalSizeOnDisk(), projection_checksums_data.getTotalChecksumUInt128());

            if (require_checksums || !projection_checksums_txt.files.empty())
                projection_checksums_txt.checkEqual(projection_checksums_data, false);
        }
        else
        {
            auto file_buf = disk->readFile(file_path);
            HashingReadBuffer hashing_buf(*file_buf);
            hashing_buf.ignoreAll();
            checksums_data.files[file_name] = IMergeTreeDataPart::Checksums::Checksum(hashing_buf.count(), hashing_buf.getHash());
        }
    };

    bool check_uncompressed = true;
    /// First calculate checksums for columns data
    if (part_type == MergeTreeDataPartType::COMPACT)
    {
        const auto & file_name = MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION;
        checksum_file(path + file_name, file_name);
        /// Uncompressed checksums in compact parts are computed in a complex way.
        /// We check only checksum of compressed file.
        check_uncompressed = false;
    }
    else if (part_type == MergeTreeDataPartType::WIDE)
    {
        for (const auto & column : columns_list)
        {
            // check map columns later
            if (column.type->isByteMap())
                continue;

            auto serialization = IDataType::getSerialization(column,
                [&](const String & stream_name)
                {
                    return disk->exists(stream_name + DATA_FILE_EXTENSION);
                });

            serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
            {
                String file_name = ISerialization::getFileNameForStream(column, substream_path) + ".bin";
                checksums_data.files[file_name] = checksum_compressed_file(disk, path + file_name);
            });
        }
    }
    else
    {
        throw Exception("Unknown type in part " + path, ErrorCodes::UNKNOWN_PART_TYPE);
    }

    /// Checksums from the rest files listed in checksums.txt. May be absent. If present, they are subsequently compared with the actual data checksums.
    IMergeTreeDataPart::Checksums checksums_txt;
    IMergeTreeDataPart::Versions versions(std::make_shared<MergeTreeDataPartVersions>(false));

    if (require_checksums || disk->exists(fs::path(path) / "checksums.txt"))
    {
        if (disk->exists(fs::path(path) / "versions.txt"))
        {
            auto buf = disk->readFile(fs::path(path) / "versions.txt");
            versions->read(*buf);
        }
        checksums_txt.versions = versions;

        auto buf = disk->readFile(fs::path(path) / "checksums.txt");
        checksums_txt.read(*buf);
        assertEOF(*buf);
    }

    /// get checksums_data of map keys
    NameSet skipped_files;
    NamesAndTypesList map_columns_list;
    for (const auto & column : columns_list)
    {
        if (column.type->isByteMap())
            map_columns_list.push_back(column);
    }
    if (data_part->versions->enable_compact_map_data)
        genCompactMapChecksums(data_part, disk, path, map_columns_list, checksums_txt, checksums_data, skipped_files);
    else
        genMapChecksums(disk, path, map_columns_list, checksums_data);

    const auto & checksum_files_txt = checksums_txt.files;
    for (auto it = disk->iterateDirectory(path); it->isValid(); it->next())
    {
        const String & file_name = it->name();

        if (file_name.ends_with(".gin_dict") || file_name.ends_with(".gin_post") || file_name.ends_with(".gin_seg") || file_name.ends_with(".gin_sid"))
            continue;

        auto checksum_it = checksums_data.files.find(file_name);

        /// Skip files that we already calculated. Also skip metadata files that are not checksummed.
        if (checksum_it == checksums_data.files.end() && !files_without_checksums.count(file_name) && !skipped_files.count(file_name))
        {
            auto txt_checksum_it = checksum_files_txt.find(file_name);
            if (txt_checksum_it == checksum_files_txt.end() || txt_checksum_it->second.uncompressed_size == 0)
            {
                /// The file is not compressed.
                checksum_file(it->path(), file_name);
            }
            else /// If we have both compressed and uncompressed in txt, then calculate them
            {
                checksums_data.files[file_name] = checksum_compressed_file(disk, it->path());
            }
        }
    }

    if (is_cancelled())
        return {};

    if (require_checksums || !checksums_txt.files.empty())
        checksums_txt.checkEqual(checksums_data, check_uncompressed);

    return checksums_data;
}

IMergeTreeDataPart::Checksums checkDataPartInMemory(const DataPartInMemoryPtr & data_part)
{
    IMergeTreeDataPart::Checksums data_checksums;
    data_checksums.files["data.bin"] = data_part->calculateBlockChecksum();
    data_part->getChecksums()->checkEqual(data_checksums, true);
    return data_checksums;
}

IMergeTreeDataPart::Checksums checkDataPart(
    MergeTreeData::DataPartPtr data_part,
    bool require_checksums,
    std::function<bool()> is_cancelled)
{
    if (auto part_in_memory = asInMemoryPart(data_part))
        return checkDataPartInMemory(part_in_memory);

    return checkDataPart(
        data_part,
        data_part->volume->getDisk(),
        data_part->getFullRelativePath(),
        data_part->getColumns(),
        data_part->getType(),
        data_part->getFileNamesWithoutChecksums(),
        require_checksums,
        is_cancelled);
}

}
