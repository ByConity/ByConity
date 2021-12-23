#pragma once
#include <Storages/IndexFile/IndexFileWriter.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>


namespace rocksdb
{
class DB;
}

namespace IndexFile
{
struct IndexFileInfo;
}

namespace DB
{

struct StreamNameAndMark
{
    String stream_name;
    MarkInCompressedFile mark;
};

using StreamsWithMarks = std::vector<StreamNameAndMark>;
using ColumnNameToMark = std::unordered_map<String, StreamsWithMarks>;

/// Writes data part in wide format.
class MergeTreeDataPartWriterWide : public MergeTreeDataPartWriterOnDisk
{
public:
    MergeTreeDataPartWriterWide(
        const MergeTreeData::DataPartPtr & data_part,
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec,
        const MergeTreeWriterSettings & settings,
        const MergeTreeIndexGranularity & index_granularity);

    ~MergeTreeDataPartWriterWide() override;

    void write(const Block & block, const IColumn::Permutation * permutation) override;

    void finish(IMergeTreeDataPart::Checksums & checksums, bool sync) final;

    void setEnableDiskBasedKeyIndex(bool enable_disk_based_key_index_) { enable_disk_based_key_index = enable_disk_based_key_index_; }

private:
    /// Finish serialization of data: write final mark if required and compute checksums
    /// Also validate written data in debug mode
    void finishDataSerialization(IMergeTreeDataPart::Checksums & checksums, bool sync);

    /// Write data of one column.
    /// Return how many marks were written and
    /// how many rows were written for last mark
    void writeColumn(
        const NameAndTypePair & name_and_type,
        const IColumn & column,
        WrittenOffsetColumns & offset_columns,
        const Granules & granules);

    /// Write single granule of one column.
    void writeSingleGranule(
        const NameAndTypePair & name_and_type,
        const IColumn & column,
        WrittenOffsetColumns & offset_columns,
        ISerialization::SerializeBinaryBulkStatePtr & serialization_state,
        ISerialization::SerializeBinaryBulkSettings & serialize_settings,
        const Granule & granule);

    /// Take offsets from column and return as MarkInCompressed file with stream name
    StreamsWithMarks getCurrentMarksForColumn(
        const NameAndTypePair & column,
        WrittenOffsetColumns & offset_columns,
        ISerialization::SubstreamPath & path);

    /// Write mark to disk using stream and rows count
    void flushMarkToFile(
        const StreamNameAndMark & stream_with_mark,
        size_t rows_in_mark);

    /// Write mark for column taking offsets from column stream
    void writeSingleMark(
        const NameAndTypePair & column,
        WrittenOffsetColumns & offset_columns,
        size_t number_of_rows,
        ISerialization::SubstreamPath & path);

    void writeFinalMark(
        const NameAndTypePair & column,
        WrittenOffsetColumns & offset_columns,
        ISerialization::SubstreamPath & path);

    void addStreams(
        const NameAndTypePair & column,
        const ASTPtr & effective_codec_desc);

    /// Method for self check (used in debug-build only). Checks that written
    /// data and corresponding marks are consistent. Otherwise throws logical
    /// errors.
    void validateColumnOfFixedSize(const String & name, const IDataType & type);

    void fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block) override;

    /// Use information from just written granules to shift current mark
    /// in our index_granularity array.
    void shiftCurrentMark(const Granules & granules_written);

    /// Change rows in the last mark in index_granularity to new_rows_in_last_mark.
    /// Flush all marks from last_non_written_marks to disk and increment current mark if already written rows
    /// (rows_written_in_last_granule) equal to new_rows_in_last_mark.
    ///
    /// This function used when blocks change granularity drastically and we have unfinished mark.
    /// Also useful to have exact amount of rows in last (non-final) mark.
    void adjustLastMarkIfNeedAndFlushToDisk(size_t new_rows_in_last_mark);

    ISerialization::OutputStreamGetter createStreamGetter(const NameAndTypePair & column, WrittenOffsetColumns & offset_columns) const;

    using SerializationState = ISerialization::SerializeBinaryBulkStatePtr;
    using SerializationStates = std::unordered_map<String, SerializationState>;

    SerializationStates serialization_states;

    using ColumnStreams = std::map<String, StreamPtr>;
    ColumnStreams column_streams;

    /// Non written marks to disk (for each column). Waiting until all rows for
    /// this marks will be written to disk.
    using MarksForColumns = std::unordered_map<String, StreamsWithMarks>;
    MarksForColumns last_non_written_marks;

    /// How many rows we have already written in the current mark.
    /// More than zero when incoming blocks are smaller then their granularity.
    size_t rows_written_in_last_mark = 0;

    /// ------------ Unique Table Only -----------------------------------------
    void writeToTempUniqueKeyIndex(Block & block, size_t first_rid, rocksdb::DB & temp_index);
    void writeFinalUniqueKeyIndex(IndexFile::IndexFileInfo & file_info);
    void closeTempUniqueKeyIndex();

    bool enable_disk_based_key_index = false;

    size_t rows_count = 0;

    /// If the part contains only one block (normal insert case), we generate the key index file
    /// directly from the buffered block, avoiding the overhead of "temp_unique_key_index"
    Block buffered_unique_key_block;
    /// If the part contains more than one blocks (merge case), we first use "temp_unique_key_index"
    /// to sort and persist index entries, then generate the key index file from "temp_unique_key_index"
    String temp_unique_key_index_dir;
    rocksdb::DB * temp_unique_key_index = nullptr;
};

}
