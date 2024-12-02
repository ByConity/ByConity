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

#pragma once
#include <map>
#include <optional>
#include <city.h>
#include <common/types.h>
#include <Disks/IDisk.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Storages/MergeTree/MergeTreeDataPartVersions.h>


class SipHash;


namespace DB
{
using StorageType = DiskType::Type;

/// Checksum of one file.
struct PACKED_LINLINE MergeTreeDataPartChecksum
{
    using uint128 = CityHash_v1_0_2::uint128;

    UInt64 file_offset {};
    UInt64 file_size {};
    uint128 file_hash {};

    Int64 mutation = 0;

    UInt64 uncompressed_size {};
    uint128 uncompressed_hash {};

    bool is_compressed = false;
    /// MOCK for MergeTreeCNCHDataDumper
    bool is_deleted = false;

    UInt16 padding1 {};
    UInt32 padding2 {};

    MergeTreeDataPartChecksum() = default;
    MergeTreeDataPartChecksum(UInt64 file_size_, uint128 file_hash_) : file_size(file_size_), file_hash(file_hash_) {}
    MergeTreeDataPartChecksum(UInt64 file_offset_, UInt64 file_size_, uint128 file_hash_)
        : file_offset(file_offset_), file_size(file_size_), file_hash(file_hash_)
    {}
    MergeTreeDataPartChecksum(UInt64 file_size_, uint128 file_hash_, UInt64 uncompressed_size_, uint128 uncompressed_hash_)
        : file_size(file_size_), file_hash(file_hash_),
        uncompressed_size(uncompressed_size_), uncompressed_hash(uncompressed_hash_), is_compressed(true) {}

    void checkEqual(const MergeTreeDataPartChecksum & rhs, bool have_uncompressed, const String & name) const;
    void checkSize(const DiskPtr & disk, const String & path) const;
};


/** Checksums of all non-temporary files.
  * For compressed files, the check sum and the size of the decompressed data are stored to not depend on the compression method.
  */
struct MergeTreeDataPartChecksums
{
    using Checksum = MergeTreeDataPartChecksum;

    /// The order is important.
    using FileChecksums = std::map<String, Checksum>;
    using Versions = std::shared_ptr<MergeTreeDataPartVersions>;
    FileChecksums files;

    StorageType storage_type = StorageType::Local ;

    Versions versions = std::make_shared<MergeTreeDataPartVersions>(false);

    void addFile(const String & file_name, UInt64 file_size, Checksum::uint128 file_hash);

    void addFile(const String & file_name, UInt64 file_offset, UInt64 file_size, Checksum::uint128 file_hash);

    void add(MergeTreeDataPartChecksums && rhs_checksums);

    bool has(const String & file_name) const { return files.find(file_name) != files.end(); }

    bool empty() const
    {
        return files.empty();
    }

    /// Checks that the set of columns and their checksums are the same. If not, throws an exception.
    /// If have_uncompressed, for compressed files it compares the checksums of the decompressed data.
    /// Otherwise, it compares only the checksums of the files.
    void checkEqual(const MergeTreeDataPartChecksums & rhs, bool have_uncompressed) const;

    /// Checks that if offset of implicit key is same with rhs, only handle the case for compact map.
    /// For compact map, we need to adjust offset because it may be differ from source replica due to clear map key commands.
    /// For compact map, clear map key only remove checksum item, only when all keys of the map column has been removed, we will delete compated files.
    bool adjustDiffImplicitKeyOffset(const MergeTreeDataPartChecksums & rhs);

    /// Return if the checksums of the target column are same.
    bool isEqual(const MergeTreeDataPartChecksums & rhs, const String & col_name) const;

    static bool isBadChecksumsErrorCode(int code);

    /// Checks that the directory contains all the needed files of the correct size. Does not check the checksum.
    void checkSizes(const DiskPtr & disk, const String & path) const;

    /// Returns false if the checksum is too old.
    bool read(ReadBuffer & in);
    /// Assume that header with version (the first line) is read
    bool read(ReadBuffer & in, size_t format_version);
    bool readV2(ReadBuffer & in);
    bool readV3(ReadBuffer & in);
    bool readV4(ReadBuffer & from);
    /// CNCH
    bool readV5(ReadBuffer & in);
    bool readV6(ReadBuffer & in);
    // CNCH, encryption
    bool readV7(ReadBuffer & in);

    void write(WriteBuffer & to) const;
    /// For rewrite checksum of ClickhouseDumper
    void writeLocal(WriteBuffer & to) const;

    /// Checksum from the set of checksums of .bin files (for deduplication).
    void computeTotalChecksumDataOnly(SipHash & hash) const;

    /// SipHash of all all files hashes represented as hex string
    String getTotalChecksumHex() const;

    Checksum::uint128 getTotalChecksumUInt128() const;

    String getSerializedString() const;
    static MergeTreeDataPartChecksums deserializeFrom(const String & s);

    UInt64 getTotalSizeOnDisk() const;

    Strings collectImplicitColumnFilesForByteMap(const String & map_column) const;
};


/// A kind of MergeTreeDataPartChecksums intended to be stored in ZooKeeper (to save its RAM)
/// MinimalisticDataPartChecksums and MergeTreeDataPartChecksums have the same serialization format
///  for versions less than MINIMAL_VERSION_WITH_MINIMALISTIC_CHECKSUMS.
struct MinimalisticDataPartChecksums
{
    UInt64 num_compressed_files = 0;
    UInt64 num_uncompressed_files = 0;

    using uint128 = MergeTreeDataPartChecksum::uint128;
    uint128 hash_of_all_files {};
    uint128 hash_of_uncompressed_files {};
    uint128 uncompressed_hash_of_compressed_files {};

    bool operator==(const MinimalisticDataPartChecksums & other) const
    {
        return num_compressed_files == other.num_compressed_files
            && num_uncompressed_files == other.num_uncompressed_files
            && hash_of_all_files == other.hash_of_all_files
            && hash_of_uncompressed_files == other.hash_of_uncompressed_files
            && uncompressed_hash_of_compressed_files == other.uncompressed_hash_of_compressed_files;
    }

    /// Is set only for old formats
    std::optional<MergeTreeDataPartChecksums> full_checksums;

    static constexpr size_t MINIMAL_VERSION_WITH_MINIMALISTIC_CHECKSUMS = 5;

    MinimalisticDataPartChecksums() = default;
    void computeTotalChecksums(const MergeTreeDataPartChecksums & full_checksums);

    bool deserialize(ReadBuffer & in);
    void deserializeWithoutHeader(ReadBuffer & in);
    static MinimalisticDataPartChecksums deserializeFrom(const String & s);

    void serialize(WriteBuffer & to) const;
    void serializeWithoutHeader(WriteBuffer & to) const;
    String getSerializedString() const;
    static String getSerializedString(const MergeTreeDataPartChecksums & full_checksums, bool minimalistic);

    void checkEqual(const MinimalisticDataPartChecksums & rhs, bool check_uncompressed_hash_in_compressed_files) const;
    void checkEqual(const MergeTreeDataPartChecksums & rhs, bool check_uncompressed_hash_in_compressed_files) const;
    void checkEqualImpl(const MinimalisticDataPartChecksums & rhs, bool check_uncompressed_hash_in_compressed_files) const;
};


}
