#pragma once

#include <Columns/IColumn.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <Formats/FormatSettings.h>
#include <Formats/SharedParsingThreadPool.h>
#include <Interpreters/Context_fwd.h>
#include <IO/BufferWithOwnMemory.h>
#include <common/types.h>

#include <boost/noncopyable.hpp>

#include <functional>
#include <memory>
#include <unordered_map>

namespace DB
{

class Block;
struct Settings;
struct FormatFactorySettings;
struct ReadSettings;

class ReadBuffer;
class WriteBuffer;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;

class IInputFormat;
class IOutputFormat;

struct RowInputFormatParams;
struct RowOutputFormatParams;

class ISchemaReader;
using SchemaReaderPtr = std::shared_ptr<ISchemaReader>;

using InputFormatPtr = std::shared_ptr<IInputFormat>;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

FormatSettings getFormatSettings(ContextPtr context);

template <typename T>
FormatSettings getFormatSettings(ContextPtr context, const T & settings);

using FileExtensionFormats = std::unordered_map<String, String>;

/** Allows to create an IBlockInputStream or IBlockOutputStream by the name of the format.
  * Note: format and compression are independent things.
  */
class FormatFactory final : private boost::noncopyable
{
public:
    /// This callback allows to perform some additional actions after reading a single row.
    /// It's initial purpose was to extract payload for virtual columns from Kafka Consumer ReadBuffer.
    using ReadCallback = std::function<void()>;

    /** Fast reading data from buffer and save result to memory.
      * Reads at least min_chunk_bytes and some more until the end of the chunk, depends on the format.
      * Used in ParallelParsingBlockInputStream.
      */
    using FileSegmentationEngine = std::function<std::pair<bool, size_t>(
        ReadBuffer & buf,
        DB::Memory<> & memory,
        size_t min_chunk_bytes)>;

    using SchemaReaderCreator = std::function<SchemaReaderPtr(ReadBuffer & in, const FormatSettings & settings)>;

    /// Some formats can extract different schemas from the same source depending on
    /// some settings. To process this case in schema cache we should add some additional
    /// information to a cache key. This getter should return some string with information
    /// about such settings. For example, for Protobuf format it's the path to the schema
    /// and the name of the message.
    using AdditionalInfoForSchemaCacheGetter = std::function<String(const FormatSettings & settings)>;


    /// This callback allows to perform some additional actions after writing a single row.
    /// It's initial purpose was to flush Kafka message for each row.
    using WriteCallback = std::function<void(
        const Columns & columns,
        size_t row)>;

private:
    using InputCreator = std::function<BlockInputStreamPtr(
        ReadBuffer & buf,
        const Block & sample,
        UInt64 max_block_size,
        ReadCallback callback,
        const FormatSettings & settings)>;

    // Incompatible with FileSegmentationEngine.
    using RandomAccessInputCreator = std::function<InputFormatPtr(
            ReadBuffer & buf,
            const Block & header,
            const FormatSettings & settings,
            const ReadSettings& read_settings,
            bool is_remote_fs,
            size_t max_download_threads,
            size_t max_parsing_threads,
            SharedParsingThreadPoolPtr parsing_thread_pool)>;

    using OutputCreator = std::function<BlockOutputStreamPtr(
        WriteBuffer & buf,
        const Block & sample,
        WriteCallback callback,
        const FormatSettings & settings)>;

    using InputProcessorCreatorFunc = InputFormatPtr(
        ReadBuffer & buf,
        const Block & header,
        const RowInputFormatParams & params,
        const FormatSettings & settings);

    using InputProcessorCreator = std::function<InputProcessorCreatorFunc>;

    using OutputProcessorCreator = std::function<OutputFormatPtr(
            WriteBuffer & buf,
            const Block & sample,
            const RowOutputFormatParams & params,
            const FormatSettings & settings)>;

    struct Creators
    {
        InputCreator input_creator;
        RandomAccessInputCreator random_access_input_creator;
        OutputCreator output_creator;
        InputProcessorCreator input_processor_creator;
        OutputProcessorCreator output_processor_creator;
        FileSegmentationEngine file_segmentation_engine;
        SchemaReaderCreator schema_reader_creator;
        bool supports_parallel_formatting{false};
        bool is_column_oriented{false};
        AdditionalInfoForSchemaCacheGetter additional_info_for_schema_cache_getter;
    };

    using FormatsDictionary = std::unordered_map<String, Creators>;

public:
    static FormatFactory & instance();

    InputFormatPtr getInput(
        const String & name,
        ReadBuffer & buf,
        const Block & sample,
        ContextPtr context,
        UInt64 max_block_size,
        const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    /// Checks all preconditions. Returns ordinary stream if parallel formatting cannot be done.
    /// Currently used only in Client. Don't use it something else! Better look at getOutputFormatParallelIfPossible.
    BlockOutputStreamPtr getOutputStreamParallelIfPossible(
        const String & name,
        WriteBuffer & buf,
        const Block & sample,
        ContextPtr context,
        WriteCallback callback = {},
        const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    /// Currently used only in Client. Don't use it something else! Better look at getOutputFormat.
    BlockOutputStreamPtr getOutputStream(
        const String & name,
        WriteBuffer & buf,
        const Block & sample,
        ContextPtr context,
        WriteCallback callback = {},
        const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    InputFormatPtr getInputFormat(
        const String & name,
        ReadBuffer & buf,
        const Block & sample,
        ContextPtr context,
        UInt64 max_block_size,
        const std::optional<FormatSettings> & format_settings = std::nullopt,
        std::optional<size_t> max_parsing_threads = std::nullopt,
        std::optional<size_t> max_download_threads = std::nullopt,
        // affects things like buffer sizes and parallel reading
        bool is_remote_fs = false,
        SharedParsingThreadPoolPtr shared_pool = nullptr) const;

    /// Checks all preconditions. Returns ordinary format if parallel formatting cannot be done
    /// For exporting into multiple files, ParallelFormat can't be used because of concurrency calculation
    /// of accumulated file.
    OutputFormatPtr getOutputFormatParallelIfPossible(
        const String & name,
        WriteBuffer & buf,
        const Block & sample,
        ContextPtr context,
        bool out_to_directory = false,
        WriteCallback callback = {},
        const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    OutputFormatPtr getOutputFormat(
        const String & name,
        WriteBuffer & buf,
        const Block & sample,
        ContextPtr context,
        WriteCallback callback = {},
        const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    SchemaReaderPtr getSchemaReader(
        const String & name,
        ReadBuffer & buf,
        const ContextPtr & context,
        const std::optional<FormatSettings> & format_settings = std::nullopt) const;

    /// Register format by its name.
    void registerInputFormat(const String & name, InputCreator input_creator);
    void registerRandomAccessInputFormat(const String & name, RandomAccessInputCreator input_creator);
    void registerOutputFormat(const String & name, OutputCreator output_creator);
    void registerFileSegmentationEngine(const String & name, FileSegmentationEngine file_segmentation_engine);

    void registerSchemaReader(const String & name, SchemaReaderCreator schema_reader_creator);

    void registerInputFormatProcessor(const String & name, InputProcessorCreator input_creator);
    void registerOutputFormatProcessor(const String & name, OutputProcessorCreator output_creator);

    void markOutputFormatSupportsParallelFormatting(const String & name);
    void markFormatAsColumnOriented(const String & name);

    bool checkIfFormatIsColumnOriented(const String & name);
    bool checkIfFormatHasSchemaReader(const String & name) const;
    // bool checkIfFormatHasExternalSchemaReader(const String & name) const;

    void registerAdditionalInfoForSchemaCacheGetter(const String & name, AdditionalInfoForSchemaCacheGetter additional_info_for_schema_cache_getter);
    String getAdditionalInfoForSchemaCache(const String & name, ContextPtr context, const std::optional<FormatSettings> & format_settings_ = std::nullopt);

    /// Register file extension for format
    void registerFileExtension(const String & extension, const String & format_name);
    String getFormatFromFileName(String file_name, bool throw_if_not_found = false, String format_name = "");
    String getFormatFromFileDescriptor(int fd);
    void checkFormatName(const String & name) const;
    bool exists(const String & name) const;

    const FormatsDictionary & getAllFormats() const
    {
        return dict;
    }

private:
    FormatsDictionary dict;
    FileExtensionFormats file_extension_formats;

    const Creators & getCreators(const String & name) const;
};

}
