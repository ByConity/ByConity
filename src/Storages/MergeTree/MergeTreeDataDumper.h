#pragma once

#include <IO/HashingWriteBuffer.h>
#include <Storages/HDFS/ReadBufferFromByteHDFS.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Common/config.h>

namespace DB
{

using StorageLevel = UInt32;

/** Dump staled parts to high level storage
  */
class MergeTreeDataDumper
{
public:
    using uint128 = CityHash_v1_0_2::uint128;

    MergeTreeDataDumper(
        MergeTreeMetaBase & data_,
        const StorageType & type_ = StorageType::HDFS,
        const String magic_code_ = "CHHF",
        const MergeTreeDataFormatVersion version_ = MERGE_TREE_DATA_STORAGTE_LEVEL_1_VERSION)
        : data(data_)
        , log(&Poco::Logger::get(data.getLogName() + "(Dumper)"))
        , type(type_)
        , magic_code(magic_code_)
        , version(version_)
    {}

    /** Dump stabled part,
      * Returns part with unique name starting with 'tmp_' in corresponding level storage,
      * yet not added to MergeTreeMetaBase.
      *
      * There is two files per part: meta and data
      *
      * Meta file format:
      * ------------meta header---------
      * magic_code(4 bytes)
      * version(8 bytes)
      * marks count(8 bytes)
      * rows count(8 bytes)
      * reserved size(256 - 28 bytes)
      * ------------meta data-----------
      * length(8 bytes)
      * primary.idx
      * length(8 bytes)
      * partition.dat
      * length(8 bytes)
      * minmax file
      * length(8 bytes)
      * columns.txt
      * data file checksums with offset
      *-------------meta footer---------
      * meta checksum
      * --------------------------------
      *
      * Data file format
      * ------------data header---------
      * magic_code(4 bytes)
      * version(8 bytes)
      * reserved size(256 - 12 bytes)
      * -----------data content--------
      * columns data files & idx files
      * -------------------------------
      */
    MergeTreeMetaBase::MutableDataPartPtr dumpTempPart(MergeTreeMetaBase::DataPartPtr staled_part, StorageLevel storage_level = 1);

private:
    std::unique_ptr<WriteBuffer> createWriteBuffer(const String & file_name, const StorageType & /*type*/);
    std::unique_ptr<ReadBuffer> createReadBuffer(const String & file_name, const StorageType & /*type*/);
    void writeMetaFileHeader(size_t marks_count, size_t rows_count, WriteBuffer & to);
    void readMetaFileFooter(uint128 & checksum, ReadBuffer & from);
    void writeMetaFileFooter(uint128 checksum, WriteBuffer & to);
    void writeDataFileHeader(WriteBuffer & to);
    void writeFileNamesAndOffsets(const String name, size_t offset, size_t size, WriteBuffer & to);
    void copyMetaFile(ReadBuffer & from, WriteBuffer & to, const size_t file_size);
    bool checkMeta(MergeTreeMetaBase::DataPartPtr remote_part, uint128 meta_checksum);
    bool checkData(MergeTreeMetaBase::DataPartPtr remote_part);
    void check(MergeTreeMetaBase::DataPartPtr remote_part, uint128 meta_checksum);

private:
    MergeTreeMetaBase & data;
    Poco::Logger * log;
    StorageType type{StorageType::HDFS};
    String magic_code{"CHHF"};
    MergeTreeDataFormatVersion version{MERGE_TREE_DATA_STORAGTE_LEVEL_1_VERSION};
};
}
