#pragma once

#include <memory>
#include <string>

#include <Compression/CompressedReadBufferFromFile.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>

namespace DB
{

struct SampleColumnIndexGranularityInfo
{
public:
    /// Marks file extension '.mrk' or '.mrk2'
    String marks_file_extension;

    /// Is stride in rows between marks non fixed?
    bool is_adaptive = false;

    SampleColumnIndexGranularityInfo(const String & path_to_part);

    String getMarksFilePath(const String & path_prefix) const
    {
        return path_prefix + marks_file_extension;
    }

    size_t getMarkSizeInBytes() const;
    size_t getMarksCount(const String & path_prefix) const;
    size_t getMarksTotalSizeInBytes(const String & path_prefix) const;

private:
    MergeTreeDataPartType part_type;
    std::optional<std::string> getMarksExtensionFromFilesystem(const String & path_to_part);
    std::string getAdaptiveMrkExtension(MergeTreeDataPartType part_type);
};

class SampleColumnMarksLoader
{
public:
    using MarksPtr = std::shared_ptr<MarksInCompressedFile>;

    SampleColumnMarksLoader(
        const String & path_prefix_,
        const String & stream_name_,
        size_t marks_count_,
        const SampleColumnIndexGranularityInfo & index_granularity_info_,
        off_t mark_file_offset_,
        size_t mark_file_size_);

    const MarkInCompressedFile & getMark(size_t row_index);

    bool initialized() const { return marks != nullptr; }

private:
    String mrk_path;
    String stream_name; // for compacted map
    size_t marks_count;

    off_t mark_file_offset;
    size_t mark_file_size;

    SampleColumnIndexGranularityInfo index_granularity_info;

    MarksPtr marks;

    void loadMarks();
    MarksPtr loadMarksImpl();
};

class SampleColumnReaderStream
{
public:
    SampleColumnReaderStream(
        const String & path_prefix_, const String & stream_name_, const String & data_file_extension_,
        const SampleColumnIndexGranularityInfo * index_granularity_info_,
        size_t max_rows_to_read_);

    virtual ~SampleColumnReaderStream() = default;

    void seekToMark(size_t index);

    void seekToStart();

    ReadBuffer * data_buffer;

private:
    std::string path_prefix;
    off_t data_file_offset = 0;
    off_t mark_file_offset = 0;
    [[maybe_unused]] size_t max_rows_to_read;

    std::unique_ptr<CompressedReadBufferFromFile> non_cached_buffer;

    SampleColumnMarksLoader marks_loader;
};

using SampleFileStreams = std::map<std::string, std::unique_ptr<SampleColumnReaderStream>>;
using DeserializeBinaryBulkStateMap = std::map<std::string, ISerialization::DeserializeBinaryBulkStatePtr>;

class SampleColumnReader
{
private:
    const std::string path_to_part;
    size_t from_mark;
    size_t max_rows_to_read;

    SampleFileStreams streams;

    /// Stores states for IDataType::deserializeBinaryBulk
    DeserializeBinaryBulkStateMap deserialize_binary_bulk_state_map;

public:
    SampleColumnReader(
        std::string path_to_part_,
        size_t from_mark_,
        size_t max_rows_to_read_);

    virtual ~SampleColumnReader() = default;

    ReadBuffer * getStream(
        bool stream_for_prefix,
        const ISerialization::SubstreamPath & substream_path,
        const NameAndTypePair & name_and_type,
        size_t from_mark);

    ColumnPtr readColumn(const NameAndTypePair & name_and_type);
};

}
