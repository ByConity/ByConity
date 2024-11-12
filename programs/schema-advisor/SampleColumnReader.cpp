#include "SampleColumnReader.h"

#include <filesystem>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <IO/ReadBufferFromFile.h>
#include <Poco/DirectoryIterator.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CORRUPTED_DATA;
    extern const int NO_FILE_IN_DATA_PART;
    extern const int UNKNOWN_PART_TYPE;
}

SampleColumnIndexGranularityInfo::SampleColumnIndexGranularityInfo(const String & path_to_part)
{
    auto mrk_ext = getMarksExtensionFromFilesystem(path_to_part);
    if (*mrk_ext == getNonAdaptiveMrkExtension())
    {
        is_adaptive = false;
        part_type = MergeTreeDataPartType::WIDE;
        marks_file_extension = *mrk_ext;
    }
    else if (*mrk_ext == getAdaptiveMrkExtension(MergeTreeDataPartType::WIDE))
    {
        is_adaptive = true;
        part_type = MergeTreeDataPartType::WIDE;
        marks_file_extension = *mrk_ext;
    }
    else
    {
        throw Exception("Can't determine part type, because of unsupported mark extension " + *mrk_ext, ErrorCodes::UNKNOWN_PART_TYPE);
    }
}

std::optional<std::string> SampleColumnIndexGranularityInfo::getMarksExtensionFromFilesystem(const String & path_to_part)
{
    if (std::filesystem::exists(path_to_part))
    {
        Poco::DirectoryIterator end;
        for (Poco::DirectoryIterator it(path_to_part); it != end; ++it)
        {
            const auto & ext = std::filesystem::path(it->path()).extension();
            if (ext == getNonAdaptiveMrkExtension()
                || ext == getAdaptiveMrkExtension(MergeTreeDataPartType::WIDE)
                || ext == getAdaptiveMrkExtension(MergeTreeDataPartType::COMPACT))
                return ext;
        }
    }
    return {};
}

std::string SampleColumnIndexGranularityInfo::getAdaptiveMrkExtension(MergeTreeDataPartType part_type_)
{
    if (part_type_ == MergeTreeDataPartType::WIDE)
        return ".mrk2";
    else if (part_type_ == MergeTreeDataPartType::COMPACT)
        return ".mrk3";
    else if (part_type_ == MergeTreeDataPartType::IN_MEMORY)
        return "";
    else
        throw Exception("Unknown part type", ErrorCodes::UNKNOWN_PART_TYPE);
}

size_t SampleColumnIndexGranularityInfo::getMarkSizeInBytes() const
{
    if (part_type == MergeTreeDataPartType::WIDE)
        return is_adaptive ? getAdaptiveMrkSizeWide() : getNonAdaptiveMrkSizeWide();
    else
        throw Exception("Unsupported type: " + part_type.toString(), ErrorCodes::UNKNOWN_PART_TYPE);
}

size_t SampleColumnIndexGranularityInfo::getMarksCount(const String & path_prefix) const
{
    std::string marks_file_path = getMarksFilePath(path_prefix);
    if (!std::filesystem::exists(marks_file_path))
        throw Exception("Marks file '" + marks_file_path + "' doesn't exist", ErrorCodes::NO_FILE_IN_DATA_PART);

    size_t marks_file_size = std::filesystem::file_size(marks_file_path);
    return marks_file_size / getMarkSizeInBytes();
}

size_t SampleColumnIndexGranularityInfo::getMarksTotalSizeInBytes(const String & path_prefix) const
{
    std::string marks_file_path = getMarksFilePath(path_prefix);
    if (!std::filesystem::exists(marks_file_path))
        throw Exception("Marks file '" + marks_file_path + "' doesn't exist", ErrorCodes::NO_FILE_IN_DATA_PART);

    return std::filesystem::file_size(marks_file_path);
}

SampleColumnMarksLoader::SampleColumnMarksLoader(
    const String & path_prefix_,
    const String & stream_name_,
    size_t marks_count_,
    const SampleColumnIndexGranularityInfo & index_granularity_info_,
    off_t mark_file_offset_,
    size_t mark_file_size_)
    : mrk_path(index_granularity_info_.getMarksFilePath(path_prefix_))
    , stream_name(stream_name_)
    , marks_count(marks_count_)
    , mark_file_offset(mark_file_offset_)
    , mark_file_size(mark_file_size_)
    , index_granularity_info(index_granularity_info_) {}

const MarkInCompressedFile & SampleColumnMarksLoader::getMark(size_t row_index)
{
    if (!marks)
        loadMarks();

    return (*marks)[row_index];
}

SampleColumnMarksLoader::MarksPtr SampleColumnMarksLoader::loadMarksImpl()
{
    /// Memory for marks must not be accounted as memory usage for query, because they are stored in shared cache.
    MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

    size_t mark_size = index_granularity_info.getMarkSizeInBytes();
    size_t expected_file_size = mark_size * marks_count;

    if (expected_file_size != mark_file_size)
        throw Exception(
            "Bad size of marks file '" + mrk_path + "' for stream '" + stream_name + "': " + std::to_string(mark_file_size) + ", must be: " + std::to_string(expected_file_size),
            ErrorCodes::CORRUPTED_DATA);

    auto res = std::make_shared<MarksInCompressedFile>(marks_count);

    if (!index_granularity_info.is_adaptive)
    {
        /// Read directly to marks.
        auto buffer = std::make_unique<ReadBufferFromFile>(mrk_path);
        if (buffer->seek(mark_file_offset, SEEK_SET) != mark_file_offset)
            throw Exception("Cannot seek to mark file  " + mrk_path + " for stream " + stream_name, ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

        if (buffer->eof() || buffer->buffer().size() != mark_file_size)
            throw Exception("Cannot read all marks from file " + mrk_path + ", eof: " + std::to_string(buffer->eof())
            + ", buffer size: " + std::to_string(buffer->buffer().size()) + ", file size: " + std::to_string(mark_file_size), ErrorCodes::CANNOT_READ_ALL_DATA);

        buffer->readStrict(reinterpret_cast<char *>(res->data()), mark_file_size);
    }
    else
    {
        auto buffer = std::make_unique<ReadBufferFromFile>(mrk_path);
        if (buffer->seek(mark_file_offset, SEEK_SET) != mark_file_offset)
            throw Exception("Cannot seek to mark file  " + mrk_path + " for stream " + stream_name, ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

        size_t i = 0;
        off_t limit_offset_in_file = mark_file_offset + mark_file_size;
        while (buffer->getPosition() < limit_offset_in_file)
        {
            res->read(*buffer, i, 1);
            buffer->seek(sizeof(size_t), SEEK_CUR);
            ++i;
        }

        if (i * mark_size != mark_file_size)
            throw Exception("Cannot read all marks from file " + mrk_path, ErrorCodes::CANNOT_READ_ALL_DATA);
    }
    res->protect();
    return res;
}

void SampleColumnMarksLoader::loadMarks()
{
    String mrk_name = index_granularity_info.getMarksFilePath(stream_name);
    marks = loadMarksImpl();

    if (!marks)
        throw Exception("Failed to load marks: " + mrk_name + " from path:" + mrk_path, ErrorCodes::LOGICAL_ERROR);
}

SampleColumnReaderStream::SampleColumnReaderStream(
        const String & path_prefix_, const String & stream_name_, const String & data_file_extension_,
        const SampleColumnIndexGranularityInfo * index_granularity_info_,
        size_t max_rows_to_read_)
        : path_prefix(path_prefix_)
        , max_rows_to_read(max_rows_to_read_)
        , marks_loader(
            path_prefix_
            , stream_name_
            , index_granularity_info_->getMarksCount(path_prefix_)
            , *index_granularity_info_
            , mark_file_offset
            , index_granularity_info_->getMarksTotalSizeInBytes(path_prefix_))
{
    std::string data_file_path = path_prefix_ + data_file_extension_;
    /// Initialize the objects that shall be used to perform read operations.
    auto buffer = std::make_unique<CompressedReadBufferFromFile>(
        std::make_unique<ReadBufferFromFile>(data_file_path),
        /* allow_different_codecs = */true,
        data_file_offset,
        std::filesystem::file_size(data_file_path),
        /* is_limit = */true);

    /* if (!settings.checksum_on_read) */
    buffer->disableChecksumming();

    non_cached_buffer = std::move(buffer);
    data_buffer = non_cached_buffer.get();
}

void SampleColumnReaderStream::seekToMark(size_t index)
{
    MarkInCompressedFile mark = marks_loader.getMark(index);

    try
    {
        non_cached_buffer->seek(mark.offset_in_compressed_file + data_file_offset, mark.offset_in_decompressed_block);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        if (e.code() == ErrorCodes::ARGUMENT_OUT_OF_BOUND)
            e.addMessage("(while seeking to mark " + toString(index)
                         + " of column " + path_prefix + "; offsets are: "
                         + toString(mark.offset_in_compressed_file + data_file_offset) + " "
                         + toString(mark.offset_in_decompressed_block) + ")");

        throw;
    }
}

void SampleColumnReaderStream::seekToStart()
{
    try
    {
        non_cached_buffer->seek(data_file_offset, 0);
#ifdef ENABLE_QPL_COMPRESSION
        if (non_cached_async_buffer)
            non_cached_async_buffer->seek(data_file_offset, 0);
#endif
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        if (e.code() == ErrorCodes::ARGUMENT_OUT_OF_BOUND)
            e.addMessage("(while seeking to start of column " + path_prefix + ")");

        throw;
    }
}

SampleColumnReader::SampleColumnReader(
    std::string path_to_part_, size_t from_mark_, size_t max_rows_to_read_)
    : path_to_part(std::move(path_to_part_))
    , from_mark(from_mark_)
    , max_rows_to_read(max_rows_to_read_) {}

ReadBuffer * SampleColumnReader::getStream(
        [[maybe_unused]] bool stream_for_prefix,
        const ISerialization::SubstreamPath & substream_path,
        const NameAndTypePair & name_and_type,
        size_t from_mark_)
{
    String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

    auto it = streams.find(stream_name);
    if (it == streams.end())
        return nullptr;

    SampleColumnReaderStream & stream = *it->second;

    if (stream_for_prefix)
        stream.seekToStart();
    else
        stream.seekToMark(from_mark_);

    return stream.data_buffer;
}


ColumnPtr SampleColumnReader::readColumn(const NameAndTypePair & name_and_type)
{
    SampleColumnIndexGranularityInfo index_granularity_info(path_to_part);

    ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path) {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

        if (streams.count(stream_name))
            return;
/*
        auto check_validity
            = [&](String & stream_name_) -> bool { return data_part->getChecksums()->files.count(stream_name_ + DATA_FILE_EXTENSION); };

        // If data file is missing then we will not try to open it.
        // It is necessary since it allows to add new column to structure of the table without creating new files for old parts.
        //
        if ((!name_and_type.type->isKVMap() && !check_validity(stream_name))
            || (name_and_type.type->isKVMap() && !tryConvertToValidKVStreamName(stream_name, check_validity)))
            return;
*/
        std::string path_prefix = path_to_part + stream_name;
        streams.emplace(
            stream_name,
            std::make_unique<SampleColumnReaderStream>(
                path_prefix,
                stream_name,
                DATA_FILE_EXTENSION,
                &index_granularity_info,
                max_rows_to_read
            ));
    };

    auto serialization = name_and_type.type->getDefaultSerialization();
    serialization->enumerateStreams(callback);

    ColumnPtr column = name_and_type.type->createColumn();
    //double & avg_value_size_hint = avg_value_size_hints[name_and_type.name];
    ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
    // deserialize_settings.avg_value_size_hint = avg_value_size_hint;

    const auto & name = name_and_type.name;

    if (deserialize_binary_bulk_state_map.count(name) == 0)
    {
        deserialize_settings.getter = [&](const ISerialization::SubstreamPath & substream_path)
        {
            return getStream(true, substream_path, name_and_type, from_mark);
        };
        serialization->deserializeBinaryBulkStatePrefix(deserialize_settings, deserialize_binary_bulk_state_map[name]);
    }

    deserialize_settings.getter = [&](const ISerialization::SubstreamPath & substream_path)
    {
        return getStream(false, substream_path, name_and_type, from_mark);
    };
    deserialize_settings.continuous_reading = 0;
    auto & deserialize_state = deserialize_binary_bulk_state_map[name];

    serialization->deserializeBinaryBulkWithMultipleStreams(column, max_rows_to_read, deserialize_settings, deserialize_state, nullptr);
    return column;
}

}
