#include <Formats/FormatFactory.h>

#include <algorithm>
#include <Common/Exception.h>
#include <Common/KnownObjectNames.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <DataStreams/MaterializingBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Processors/Formats/OutputStreamToOutputFormat.h>
#include <Processors/Formats/Impl/ValuesBlockInputFormat.h>
#include <Processors/Formats/Impl/MySQLOutputFormat.h>
#include <Processors/Formats/Impl/NativeFormat.h>
#include <Processors/Formats/Impl/ParallelParsingInputFormat.h>
#include <Processors/Formats/Impl/ParallelFormattingOutputFormat.h>
#include <Poco/URI.h>
#include <IO/CompressionMethod.h>
#include <boost/algorithm/string/case_conv.hpp>

#include <IO/ReadHelpers.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT;
    extern const int LOGICAL_ERROR;
    extern const int FORMAT_IS_NOT_SUITABLE_FOR_INPUT;
    extern const int FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT;
    extern const int UNSUPPORTED_METHOD;
    extern const int BAD_ARGUMENTS;
}

const FormatFactory::Creators & FormatFactory::getCreators(const String & name) const
{
    auto it = dict.find(name);
    if (dict.end() != it)
        return it->second;
    throw Exception("Unknown format " + name, ErrorCodes::UNKNOWN_FORMAT);
}

FormatSettings getFormatSettings(ContextPtr context)
{
    const auto & settings = context->getSettingsRef();

    return getFormatSettings(context, settings);
}

template <typename Settings>
FormatSettings getFormatSettings(ContextPtr context, const Settings & settings)
{
    FormatSettings format_settings;

    format_settings.avro.allow_missing_fields = settings.input_format_avro_allow_missing_fields;
    format_settings.avro.output_codec = settings.output_format_avro_codec;
    format_settings.avro.output_sync_interval = settings.output_format_avro_sync_interval;
    format_settings.avro.schema_registry_url = settings.format_avro_schema_registry_url.toString();
    format_settings.csv.allow_double_quotes = settings.format_csv_allow_double_quotes;
    format_settings.csv.allow_single_quotes = settings.format_csv_allow_single_quotes;
    format_settings.csv.write_utf8_with_bom = settings.format_csv_write_utf8_with_bom;
    format_settings.csv.crlf_end_of_line = settings.output_format_csv_crlf_end_of_line;
    format_settings.csv.delimiter = settings.format_csv_delimiter;
    format_settings.csv.empty_as_default = settings.input_format_defaults_for_omitted_fields;
    format_settings.csv.input_format_enum_as_number = settings.input_format_csv_enum_as_number;
    format_settings.csv.unquoted_null_literal_as_null = settings.input_format_csv_unquoted_null_literal_as_null;
    format_settings.csv.input_format_arrays_as_nested_csv = settings.input_format_csv_arrays_as_nested_csv;
    format_settings.custom.escaping_rule = settings.format_custom_escaping_rule;
    format_settings.custom.field_delimiter = settings.format_custom_field_delimiter;
    format_settings.custom.result_after_delimiter = settings.format_custom_result_after_delimiter;
    format_settings.custom.result_before_delimiter = settings.format_custom_result_before_delimiter;
    format_settings.custom.row_after_delimiter = settings.format_custom_row_after_delimiter;
    format_settings.custom.row_before_delimiter = settings.format_custom_row_before_delimiter;
    format_settings.custom.row_between_delimiter = settings.format_custom_row_between_delimiter;
    format_settings.date_time_input_format = settings.date_time_input_format;
    format_settings.date_time_output_format = settings.date_time_output_format;
    format_settings.bool_true_representation = settings.bool_true_representation;
    format_settings.bool_false_representation = settings.bool_false_representation;
    format_settings.enable_streaming = settings.output_format_enable_streaming;
    format_settings.import_nested_json = settings.input_format_import_nested_json;
    format_settings.input_allow_errors_num = settings.input_format_allow_errors_num;
    format_settings.input_allow_errors_ratio = settings.input_format_allow_errors_ratio;
    format_settings.json.read_objects_as_strings = settings.input_format_json_read_objects_as_strings;
    format_settings.json.array_of_rows = settings.output_format_json_array_of_rows;
    format_settings.json.escape_forward_slashes = settings.output_format_json_escape_forward_slashes;
    format_settings.json.write_named_tuples_as_objects = settings.output_format_json_named_tuples_as_objects;
    format_settings.json.read_named_tuples_as_objects = settings.input_format_json_named_tuples_as_objects;
    format_settings.json.defaults_for_missing_elements_in_named_tuple = settings.input_format_json_defaults_for_missing_elements_in_named_tuple;
    format_settings.json.quote_64bit_integers = settings.output_format_json_quote_64bit_integers;
    format_settings.json.quote_denormals = settings.output_format_json_quote_denormals;
    format_settings.orc.output_string_as_string = settings.output_format_orc_string_as_string;
    format_settings.orc.allow_out_of_range = settings.input_orc_date_type_out_of_range;
    format_settings.orc.use_fast_decoder = settings.input_format_orc_use_fast_decoder;
    format_settings.orc.filter_push_down = settings.input_format_orc_filter_push_down;
    format_settings.orc.import_nested = settings.input_format_orc_import_nested;
    format_settings.orc.case_insensitive_column_matching = settings.input_format_orc_case_insensitive_column_matching;
    format_settings.orc.use_footer_cache = settings.input_format_orc_use_footer_cache;
    format_settings.seekable_read = settings.input_format_allow_seeks;
    format_settings.defaults_for_omitted_fields = settings.input_format_defaults_for_omitted_fields;
    format_settings.max_rows_to_read_for_schema_inference = settings.input_format_max_rows_to_read_for_schema_inference;
    format_settings.max_bytes_to_read_for_schema_inference = settings.input_format_max_rows_to_read_for_schema_inference;
    format_settings.schema_inference_make_columns_nullable = settings.schema_inference_make_columns_nullable;
    format_settings.avoid_buffering = settings.input_format_arrow_avoid_buffering;
    format_settings.null_as_default = settings.input_format_null_as_default;
    format_settings.parquet.row_group_rows = settings.output_format_parquet_row_group_size;
    format_settings.parquet.row_group_bytes = settings.output_format_parquet_row_group_size_bytes;
    format_settings.parquet.output_version = settings.output_format_parquet_version;
    format_settings.parquet.output_compression_method = settings.output_format_parquet_compression_method;
    format_settings.parquet.output_string_as_string = settings.output_format_parquet_string_as_string;
    format_settings.parquet.output_fixed_string_as_fixed_byte_array = settings.output_format_parquet_fixed_string_as_fixed_byte_array;
    format_settings.parquet.allow_missing_columns = settings.input_format_parquet_allow_missing_columns;
    format_settings.parquet.preserve_order = settings.input_format_parquet_preserve_order;
    format_settings.parquet.coalesce_read = settings.input_format_parquet_coalesce_read;
    format_settings.parquet.import_nested = settings.input_format_parquet_import_nested;
    format_settings.parquet.case_insensitive_column_matching = settings.input_format_parquet_case_insensitive_column_matching;
    format_settings.parquet.max_block_size = settings.input_format_parquet_max_block_size;
    format_settings.parquet.min_bytes_for_seek = settings.input_format_parquet_min_bytes_for_seek;
    format_settings.parquet.max_buffer_size = settings.input_format_parquet_max_buffer_size;
    format_settings.parquet.filter_push_down = settings.input_format_parquet_filter_push_down;
    format_settings.parquet.use_footer_cache = settings.input_format_parquet_use_footer_cache;
    format_settings.parquet.use_native_reader = settings.input_format_parquet_use_native_reader;
    format_settings.parquet.use_threads = settings.input_format_parquet_use_threads;
    format_settings.pretty.charset = settings.output_format_pretty_grid_charset.toString() == "ASCII" ? FormatSettings::Pretty::Charset::ASCII : FormatSettings::Pretty::Charset::UTF8;
    format_settings.pretty.color = settings.output_format_pretty_color;
    format_settings.pretty.max_column_pad_width = settings.output_format_pretty_max_column_pad_width;
    format_settings.pretty.max_rows = settings.output_format_pretty_max_rows;
    format_settings.pretty.max_value_width = settings.output_format_pretty_max_value_width;
    format_settings.pretty.output_format_pretty_row_numbers = settings.output_format_pretty_row_numbers;
    format_settings.regexp.escaping_rule = settings.format_regexp_escaping_rule;
    format_settings.regexp.regexp = settings.format_regexp;
    format_settings.regexp.skip_unmatched = settings.format_regexp_skip_unmatched;
    format_settings.schema.format_schema = settings.format_schema;
    /// Prefer to get local format_schema_path for each table to support multi-tanants;
    format_settings.schema.format_schema_path = !settings.format_custom_schema_path.value.empty() ?
                                                settings.format_custom_schema_path :
                                                context->getFormatSchemaPath();
    format_settings.schema.is_server = context->hasGlobalContext() && (context->getGlobalContext()->getApplicationType() == Context::ApplicationType::SERVER);
    format_settings.skip_unknown_fields = settings.input_format_skip_unknown_fields;
    format_settings.template_settings.resultset_format = settings.format_template_resultset;
    format_settings.template_settings.row_between_delimiter = settings.format_template_rows_between_delimiter;
    format_settings.template_settings.row_format = settings.format_template_row;
    format_settings.tsv.crlf_end_of_line = settings.output_format_tsv_crlf_end_of_line;
    format_settings.tsv.empty_as_default = settings.input_format_tsv_empty_as_default;
    format_settings.tsv.input_format_enum_as_number = settings.input_format_tsv_enum_as_number;
    format_settings.tsv.null_representation = settings.output_format_tsv_null_representation;
    format_settings.values.accurate_types_of_literals = settings.input_format_values_accurate_types_of_literals;
    format_settings.values.deduce_templates_of_expressions = settings.input_format_values_deduce_templates_of_expressions;
    format_settings.values.interpret_expressions = settings.input_format_values_interpret_expressions;
    format_settings.with_names_use_header = settings.input_format_with_names_use_header;
    format_settings.write_statistics = settings.output_format_write_statistics;
    format_settings.arrow.low_cardinality_as_dictionary = settings.output_format_arrow_low_cardinality_as_dictionary;
    format_settings.arrow.allow_missing_columns = settings.input_format_arrow_allow_missing_columns;
    format_settings.arrow.output_string_as_string = settings.output_format_arrow_string_as_string;
    format_settings.arrow.output_fixed_string_as_fixed_byte_array = settings.output_format_arrow_fixed_string_as_fixed_byte_array;
    format_settings.arrow.import_nested = settings.input_format_arrow_import_nested;
    format_settings.orc.import_nested = settings.input_format_orc_import_nested;
    format_settings.orc.output_string_as_string = settings.output_format_orc_string_as_string;
    format_settings.orc.allow_missing_columns = settings.input_format_orc_allow_missing_columns;
    format_settings.protobuf.enable_multiple_message = settings.input_format_protobuf_enable_multiple_message;
    format_settings.protobuf.default_length_parser = settings.input_format_protobuf_default_length_parser;
    format_settings.map.parse_null_map_as_empty = settings.input_format_parse_null_map_as_empty;
    format_settings.map.skip_null_map_value = settings.input_format_skip_null_map_value;
    format_settings.map.max_map_key_length = settings.input_format_max_map_key_long;
    format_settings.check_data_overflow = settings.check_data_overflow;
    format_settings.date_time_overflow_behavior = settings.date_time_overflow_behavior;

    /// Validate avro_schema_registry_url with RemoteHostFilter when non-empty and in Server context
    if (format_settings.schema.is_server)
    {
        const Poco::URI & avro_schema_registry_url = settings.format_avro_schema_registry_url;
        if (!avro_schema_registry_url.empty())
            context->getRemoteHostFilter().checkURL(avro_schema_registry_url);
    }

    return format_settings;
}

template FormatSettings getFormatSettings<FormatFactorySettings>(ContextPtr context, const FormatFactorySettings & settings);

template FormatSettings getFormatSettings<Settings>(ContextPtr context, const Settings & settings);


InputFormatPtr FormatFactory::getInput(
    const String & name,
    ReadBuffer & buf,
    const Block & sample,
    ContextPtr context,
    UInt64 max_block_size,
    const std::optional<FormatSettings> & _format_settings) const
{
    if (name == "Native")
        return std::make_shared<NativeInputFormatFromNativeBlockInputStream>(sample, buf);

    const auto & creators = getCreators(name);
    if (!creators.input_processor_creator && !creators.random_access_input_creator)
        throw Exception(ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_INPUT, "Format {} is not suitable for input", name);

    auto format_settings = _format_settings
        ? *_format_settings : getFormatSettings(context);

    const Settings & settings = context->getSettingsRef();
    const auto & file_segmentation_engine = getCreators(name).file_segmentation_engine;

    // Doesn't make sense to use parallel parsing with less than four threads
    // (segmentator + two parsers + reader).
    bool parallel_parsing = settings.input_format_parallel_parsing && file_segmentation_engine && !creators.random_access_input_creator && settings.max_threads >= 4;

    if (settings.max_memory_usage && settings.min_chunk_bytes_for_parallel_parsing * settings.max_threads * 2 > settings.max_memory_usage)
        parallel_parsing = false;

    if (settings.max_memory_usage_for_user && settings.min_chunk_bytes_for_parallel_parsing * settings.max_threads * 2 > settings.max_memory_usage_for_user)
        parallel_parsing = false;

    if (parallel_parsing && name == "JSONEachRow")
    {
        /// FIXME ParallelParsingBlockInputStream doesn't support formats with non-trivial readPrefix() and readSuffix()

        /// For JSONEachRow we can safely skip whitespace characters
        skipWhitespaceIfAny(buf);
        if (buf.eof() || *buf.position() == '[')
            parallel_parsing = false; /// Disable it for JSONEachRow if data is in square brackets (see JSONEachRowRowInputFormat)
    }

    if (parallel_parsing)
    {
        const auto & input_getter = getCreators(name).input_processor_creator;

        RowInputFormatParams row_input_format_params;
        row_input_format_params.max_block_size = max_block_size;
        row_input_format_params.allow_errors_num = format_settings.input_allow_errors_num;
        row_input_format_params.allow_errors_ratio = format_settings.input_allow_errors_ratio;
        row_input_format_params.max_execution_time = settings.max_execution_time;
        row_input_format_params.timeout_overflow_mode = settings.timeout_overflow_mode;

        /// Const reference is copied to lambda.
        auto parser_creator = [input_getter, sample, row_input_format_params, format_settings]
            (ReadBuffer & input) -> InputFormatPtr
            { return input_getter(input, sample, row_input_format_params, format_settings); };


        ParallelParsingInputFormat::Params params{
            buf, sample, parser_creator, file_segmentation_engine, name, settings.max_threads, settings.min_chunk_bytes_for_parallel_parsing};
        return std::make_shared<ParallelParsingInputFormat>(params);
    }


    auto format = getInputFormat(name, buf, sample, context, max_block_size, format_settings);
    return format;
}

BlockOutputStreamPtr FormatFactory::getOutputStreamParallelIfPossible(
    const String & name,
    WriteBuffer & buf,
    const Block & sample,
    ContextPtr context,
    WriteCallback callback,
    const std::optional<FormatSettings> & _format_settings) const
{
    const auto & output_getter = getCreators(name).output_processor_creator;

    const Settings & settings = context->getSettingsRef();
    bool parallel_formatting = settings.output_format_parallel_formatting;

    if (output_getter && parallel_formatting && getCreators(name).supports_parallel_formatting
        && !settings.output_format_json_array_of_rows)
    {
        auto format_settings = _format_settings
        ? *_format_settings : getFormatSettings(context);

        auto formatter_creator = [output_getter, sample, callback, format_settings]
            (WriteBuffer & output) -> OutputFormatPtr
            { return output_getter(output, sample, {std::move(callback)}, format_settings);};

        ParallelFormattingOutputFormat::Params params{buf, sample, formatter_creator, settings.max_threads};
        auto format = std::make_shared<ParallelFormattingOutputFormat>(params);

        /// Enable auto-flush for streaming mode. Currently it is needed by INSERT WATCH query.
        if (format_settings.enable_streaming)
            format->setAutoFlush();

        return std::make_shared<MaterializingBlockOutputStream>(std::make_shared<OutputStreamToOutputFormat>(format), sample);
    }

    return getOutputStream(name, buf, sample, context, callback, _format_settings);
}


BlockOutputStreamPtr FormatFactory::getOutputStream(
    const String & name,
    WriteBuffer & buf,
    const Block & sample,
    ContextPtr context,
    WriteCallback callback,
    const std::optional<FormatSettings> & _format_settings) const
{
    auto format_settings = _format_settings ? *_format_settings : getFormatSettings(context);

    if (!getCreators(name).output_processor_creator)
    {
        const auto & output_getter = getCreators(name).output_creator;
        if (!output_getter)
            throw Exception("Format " + name + " is not suitable for output", ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT);

        /**  Materialization is needed, because formats can use the functions `IDataType`,
          *  which only work with full columns.
          */
        return std::make_shared<MaterializingBlockOutputStream>(
            output_getter(buf, sample, std::move(callback), format_settings),
            sample);
    }

    auto format = getOutputFormat(name, buf, sample, context, std::move(callback), _format_settings);
    return std::make_shared<MaterializingBlockOutputStream>(std::make_shared<OutputStreamToOutputFormat>(format), sample);
}


InputFormatPtr FormatFactory::getInputFormat(
    const String & name,
    ReadBuffer & buf,
    const Block & sample,
    ContextPtr context,
    UInt64 max_block_size,
    const std::optional<FormatSettings> & _format_settings,
    std::optional<size_t> _max_parsing_threads,
    std::optional<size_t> _max_download_threads,
    bool is_remote_fs,
    SharedParsingThreadPoolPtr parsing_thread_pool) const
{
    const auto& creators = getCreators(name);
    if (!creators.input_processor_creator && !creators.random_access_input_creator)
        throw Exception(ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_INPUT, "Format {} is not suitable for input", name);

    const Settings & settings = context->getSettingsRef();
    if (context->hasQueryContext() && settings.log_queries)
        context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Format, name);

    auto format_settings = _format_settings ? *_format_settings : getFormatSettings(context);
    size_t max_parsing_threads = _max_parsing_threads.value_or(settings.max_parsing_threads);
    size_t max_download_threads = _max_download_threads.value_or(settings.max_download_threads);

    RowInputFormatParams params;
    params.max_block_size = max_block_size;
    params.allow_errors_num = format_settings.input_allow_errors_num;
    params.allow_errors_ratio = format_settings.input_allow_errors_ratio;
    params.max_execution_time = settings.max_execution_time;
    params.timeout_overflow_mode = settings.timeout_overflow_mode;

    InputFormatPtr format;
    if (creators.input_processor_creator)
    {
        format = creators.input_processor_creator(buf, sample, params, format_settings);
    }
    else if (creators.random_access_input_creator)
    {
        format = creators.random_access_input_creator(
            buf,
            sample,
            format_settings,
            context->getReadSettings(),
            is_remote_fs,
            max_download_threads,
            max_parsing_threads,
            parsing_thread_pool);
    }
    else
    {
        UNREACHABLE();
    }

    /// It's a kludge. Because I cannot remove context from values format.
    if (auto * values = typeid_cast<ValuesBlockInputFormat *>(format.get()))
        values->setContext(context);

    return format;
}

OutputFormatPtr FormatFactory::getOutputFormatParallelIfPossible(
    const String & name,
    WriteBuffer & buf,
    const Block & sample,
    ContextPtr context,
    bool out_to_directory,
    WriteCallback callback,
    const std::optional<FormatSettings> & _format_settings) const
{
    const auto & output_getter = getCreators(name).output_processor_creator;
    if (!output_getter)
        throw Exception(ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT, "Format {} is not suitable for output (with processors)", name);

    auto format_settings = _format_settings ? *_format_settings : getFormatSettings(context);

    const Settings & settings = context->getSettingsRef();

    if (!out_to_directory && settings.output_format_parallel_formatting
        && getCreators(name).supports_parallel_formatting
        && !settings.output_format_json_array_of_rows)
    {
        auto formatter_creator = [output_getter, sample, callback, format_settings]
        (WriteBuffer & output) -> OutputFormatPtr
        { return output_getter(output, sample, {std::move(callback)}, format_settings);};

        ParallelFormattingOutputFormat::Params builder{buf, sample, formatter_creator, settings.max_threads};

        if (context->hasQueryContext() && settings.log_queries)
            context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Format, name);

        return std::make_shared<ParallelFormattingOutputFormat>(builder);
    }

    return getOutputFormat(name, buf, sample, context, callback, _format_settings);
}


OutputFormatPtr FormatFactory::getOutputFormat(
    const String & name,
    WriteBuffer & buf,
    const Block & sample,
    ContextPtr context,
    WriteCallback callback,
    const std::optional<FormatSettings> & _format_settings) const
{
    const auto & output_getter = getCreators(name).output_processor_creator;
    if (!output_getter)
        throw Exception(ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT, "Format {} is not suitable for output (with processors)", name);

    if (context->hasQueryContext() && context->getSettingsRef().log_queries)
        context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Format, name);

    RowOutputFormatParams params;
    params.callback = std::move(callback);

    auto format_settings = _format_settings ? *_format_settings : getFormatSettings(context);

    /// If we're handling MySQL protocol connection right now then MySQLWire or Null is only allowed output format.
    if (format_settings.mysql_wire.sequence_id && (name != "MySQLWire" && name != "Null"))
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "MySQL protocol does not support custom output formats");

    /** TODO: Materialization is needed, because formats can use the functions `IDataType`,
      *  which only work with full columns.
      */
    auto format = output_getter(buf, sample, params, format_settings);

    /// Enable auto-flush for streaming mode. Currently it is needed by INSERT WATCH query.
    if (format_settings.enable_streaming)
        format->setAutoFlush();

    /// It's a kludge. Because I cannot remove context from MySQL format.
    if (auto * mysql = typeid_cast<MySQLOutputFormat *>(format.get()))
        mysql->setContext(context);

    return format;
}

void FormatFactory::registerAdditionalInfoForSchemaCacheGetter(
    const String & name, AdditionalInfoForSchemaCacheGetter additional_info_for_schema_cache_getter)
{
    auto & target = dict[name].additional_info_for_schema_cache_getter;
    if (target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: additional info for schema cache getter {} is already registered", name);
    target = std::move(additional_info_for_schema_cache_getter);
}

String FormatFactory::getAdditionalInfoForSchemaCache(const String & name, ContextPtr context, const std::optional<FormatSettings> & format_settings_)
{
    const auto & additional_info_getter = getCreators(name).additional_info_for_schema_cache_getter;
    if (!additional_info_getter)
        return "";

    auto format_settings = format_settings_ ? *format_settings_ : getFormatSettings(context);
    return additional_info_getter(format_settings);
}

SchemaReaderPtr FormatFactory::getSchemaReader(
    const String & name,
    ReadBuffer & buf,
    const ContextPtr & context,
    const std::optional<FormatSettings> & _format_settings) const
{
    const auto & schema_reader_creator = getCreators(name).schema_reader_creator;
    if (!schema_reader_creator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: Format {} doesn't support schema inference.", name);

    auto format_settings = _format_settings ? *_format_settings : getFormatSettings(context);
    auto schema_reader = schema_reader_creator(buf, format_settings);
    // if (schema_reader->needContext())
    //     schema_reader->setContext(context);
    return schema_reader;
}

void FormatFactory::registerSchemaReader(const String & name, SchemaReaderCreator schema_reader_creator)
{
    auto & target = dict[name].schema_reader_creator;
    if (target)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FormatFactory: Schema reader {} is already registered", name);
    target = std::move(schema_reader_creator);
}

void FormatFactory::registerInputFormat(const String & name, InputCreator input_creator)
{
    auto & target = dict[name].input_creator;
    if (target)
        throw Exception("FormatFactory: Input format " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = std::move(input_creator);
    registerFileExtension(name, name);
    KnownFormatNames::instance().add(name);
}

void FormatFactory::registerOutputFormat(const String & name, OutputCreator output_creator)
{
    auto & target = dict[name].output_creator;
    if (target)
        throw Exception("FormatFactory: Output format " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = std::move(output_creator);
    registerFileExtension(name, name);
    KnownFormatNames::instance().add(name);
}

void FormatFactory::registerFileExtension(const String & extension, const String & format_name)
{
    file_extension_formats[boost::to_lower_copy(extension)] = format_name;
}

String FormatFactory::getFormatFromFileName(String file_name, bool throw_if_not_found, String format_name)
{
    if (!format_name.empty() && format_name != "auto")
        return format_name;

    if (file_name == "stdin")
        return getFormatFromFileDescriptor(STDIN_FILENO);

    CompressionMethod compression_method = chooseCompressionMethod(file_name, "");
    if (CompressionMethod::None != compression_method)
    {
        auto pos = file_name.find_last_of('.');
        if (pos != String::npos)
            file_name = file_name.substr(0, pos);
    }

    auto pos = file_name.find_last_of('.');
    if (pos == String::npos)
    {
        if (throw_if_not_found)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot determine the file format by it's extension");
        return "";
    }

    String file_extension = file_name.substr(pos + 1, String::npos);
    boost::algorithm::to_lower(file_extension);
    auto it = file_extension_formats.find(file_extension);
    if (it == file_extension_formats.end())
    {
        if (throw_if_not_found)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot determine the file format by it's extension");
        return "";
    }
    return it->second;
}

String FormatFactory::getFormatFromFileDescriptor(int fd)
{
#ifdef OS_LINUX
    std::string proc_path = fmt::format("/proc/self/fd/{}", fd);
    char file_path[PATH_MAX] = {'\0'};
    if (readlink(proc_path.c_str(), file_path, sizeof(file_path) - 1) != -1)
        return getFormatFromFileName(file_path, false);
    return "";
#elif defined(OS_DARWIN)
    char file_path[PATH_MAX] = {'\0'};
    if (fcntl(fd, F_GETPATH, file_path) != -1)
        return getFormatFromFileName(file_path, false);
    return "";
#else
    (void)fd;
    return "";
#endif
}

void FormatFactory::checkFormatName(const String & name) const
{
    auto it = dict.find(name);
    if (it == dict.end())
        throw Exception("Unknown format " + name, ErrorCodes::UNKNOWN_FORMAT);
}

bool FormatFactory::exists(const String & name) const
{
    return dict.find(name) != dict.end();
}

void FormatFactory::registerInputFormatProcessor(const String & name, InputProcessorCreator input_creator)
{
    auto & target = dict[name].input_processor_creator;
    if (target)
        throw Exception("FormatFactory: Input format " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = std::move(input_creator);
    registerFileExtension(name, name);
}

void FormatFactory::registerRandomAccessInputFormat(const String & name, RandomAccessInputCreator input_creator)
{
    auto & target = dict[name].random_access_input_creator;
    if (target)
        throw Exception("FormatFactory: Input format " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = std::move(input_creator);
}

void FormatFactory::registerOutputFormatProcessor(const String & name, OutputProcessorCreator output_creator)
{
    auto & target = dict[name].output_processor_creator;
    if (target)
        throw Exception("FormatFactory: Output format " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = std::move(output_creator);
    registerFileExtension(name, name);
}

void FormatFactory::registerFileSegmentationEngine(const String & name, FileSegmentationEngine file_segmentation_engine)
{
    auto & target = dict[name].file_segmentation_engine;
    if (target)
        throw Exception("FormatFactory: File segmentation engine " + name + " is already registered", ErrorCodes::LOGICAL_ERROR);
    target = std::move(file_segmentation_engine);
}


void FormatFactory::markOutputFormatSupportsParallelFormatting(const String & name)
{
    auto & target = dict[name].supports_parallel_formatting;
    if (target)
        throw Exception("FormatFactory: Output format " + name + " is already marked as supporting parallel formatting", ErrorCodes::LOGICAL_ERROR);
    target = true;
}


void FormatFactory::markFormatAsColumnOriented(const String & name)
{
    auto & target = dict[name].is_column_oriented;
    if (target)
        throw Exception("FormatFactory: Format " + name + " is already marked as column oriented", ErrorCodes::LOGICAL_ERROR);
    target = true;
}


bool FormatFactory::checkIfFormatIsColumnOriented(const String & name)
{
    const auto & target = getCreators(name);
    return target.is_column_oriented;
}

bool FormatFactory::checkIfFormatHasSchemaReader(const String & name) const
{
    const auto & target = getCreators(name);
    return bool(target.schema_reader_creator);
}


// bool FormatFactory::checkIfFormatHasExternalSchemaReader(const String & name) const
// {
//     const auto & target = getCreators(name);
//     return bool(target.external_schema_reader_creator);
// }

FormatFactory & FormatFactory::instance()
{
    static FormatFactory ret;
    return ret;
}

}
