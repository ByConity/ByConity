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

#include <common/types.h>
#include <unordered_set>
#include <map>
#include <vector>
#include <Core/Defines.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

namespace DB
{
/**
  * Various tweaks for input/output formats. Text serialization/deserialization
  * of data types also depend on some of these settings. It is different from
  * FormatFactorySettings in that it has all necessary user-provided settings
  * combined with information from context etc, that we can use directly during
  * serialization. In contrast, FormatFactorySettings' job is to reflect the
  * changes made to user-visible format settings, such as when tweaking the
  * the format for File engine.
  * NOTE Parameters for unrelated formats and unrelated data types are collected
  * in this struct - it prevents modularity, but they are difficult to separate.
  */
struct FormatSettings
{
    /// Format will be used for streaming. Not every formats support it
    /// Option means that each chunk of data need to be formatted independently. Also each chunk will be flushed at the end of processing.
    bool enable_streaming = false;

    bool skip_unknown_fields = false;
    bool with_names_use_header = false;
    bool write_statistics = true;
    bool import_nested_json = false;
    bool null_as_default = true;
    bool defaults_for_omitted_fields = true;
    bool decimal_trailing_zeros = false;
    bool check_data_overflow = false;

    bool seekable_read = true;
    UInt64 max_rows_to_read_for_schema_inference = 100;
    UInt64 max_bytes_to_read_for_schema_inference = 32 * 1024 * 1024;

    bool avoid_buffering = true;
    enum class DateTimeInputFormat
    {
        Basic,      /// Default format for fast parsing: YYYY-MM-DD hh:mm:ss (ISO-8601 without fractional part and timezone) or NNNNNNNNNN unix timestamp.
        BestEffort,  /// Use sophisticated rules to parse whatever possible.
        BestEffortUS  /// Use sophisticated rules to parse American style: mm/dd/yyyy
    };

    DateTimeInputFormat date_time_input_format = DateTimeInputFormat::Basic;

    enum class DateTimeOutputFormat
    {
        Simple,
        ISO,
        UnixTimestamp
    };

    DateTimeOutputFormat date_time_output_format = DateTimeOutputFormat::Simple;

    enum class DateTimeOverflowBehavior
    {
        Ignore,
        Throw,
        Saturate
    };

    DateTimeOverflowBehavior date_time_overflow_behavior = DateTimeOverflowBehavior::Ignore;


    UInt64 input_allow_errors_num = 0;
    Float32 input_allow_errors_ratio = 0;

    bool schema_inference_make_columns_nullable = true;

    UInt64 max_parser_depth = DBMS_DEFAULT_MAX_PARSER_DEPTH;

    struct
    {
        UInt64 row_group_size = 1000000;
        bool low_cardinality_as_dictionary = false;
        bool import_nested = false;
        bool allow_missing_columns = false;
        bool skip_columns_with_unsupported_types_in_schema_inference = false;
        bool case_insensitive_column_matching = false;
        bool output_string_as_string = false;
        bool output_fixed_string_as_fixed_byte_array = true;
    } arrow;

    struct
    {
        String schema_registry_url;
        String output_codec;
        UInt64 output_sync_interval = 16 * 1024;
        bool allow_missing_fields = false;
    } avro;

    String bool_true_representation = "true";
    String bool_false_representation = "false";

    struct CSV
    {
        char delimiter = ',';
        bool allow_single_quotes = true;
        bool allow_double_quotes = true;
        bool write_utf8_with_bom = false;
        bool unquoted_null_literal_as_null = false;
        bool empty_as_default = false;
        bool crlf_end_of_line = false;
        bool input_format_enum_as_number = false;
        bool input_format_arrays_as_nested_csv = false;
    } csv;

    struct Custom
    {
        std::string result_before_delimiter;
        std::string result_after_delimiter;
        std::string row_before_delimiter;
        std::string row_after_delimiter;
        std::string row_between_delimiter;
        std::string field_delimiter;
        std::string escaping_rule;
    } custom;

    struct
    {
        bool array_of_rows = false;
        bool quote_64bit_integers = false;
        bool quote_denormals = true;
        bool escape_forward_slashes = true;
        bool read_named_tuples_as_objects = false;
        bool write_named_tuples_as_objects = false;
        bool defaults_for_missing_elements_in_named_tuple = false;
        bool serialize_as_strings = false;
        bool read_bools_as_numbers = true;
        bool read_numbers_as_strings = true;
        bool quota_json_string = true;
        bool read_objects_as_strings = false;
        bool allow_object_type = false;
        bool try_infer_numbers_from_strings = false;
    } json;

    enum class ParquetVersion
    {
        V1_0,
        V2_4,
        V2_6,
        V2_LATEST,
    };

    enum class ParquetCompression
    {
        NONE,
        SNAPPY,
        ZSTD,
        LZ4,
        GZIP,
        BROTLI,
    };

    struct
    {
        UInt64 row_group_rows = 1000000;
        UInt64 row_group_bytes = 512 * 1024 * 1024;
        bool import_nested = false;
        bool allow_missing_columns = false;
        bool skip_columns_with_unsupported_types_in_schema_inference = false;
        std::unordered_set<int> skip_row_groups;
        bool output_string_as_string = false;
        bool output_fixed_string_as_fixed_byte_array = true;
        bool preserve_order = false;
        bool coalesce_read = false;
        bool case_insensitive_column_matching = false;
        UInt64 max_block_size = 8192;
        ParquetVersion output_version = ParquetVersion::V2_LATEST;
        ParquetCompression output_compression_method = ParquetCompression::SNAPPY;
        bool output_compliant_nested_types = true;
        size_t min_bytes_for_seek = DBMS_DEFAULT_BUFFER_SIZE;
        size_t max_buffer_size = 8 * DBMS_DEFAULT_BUFFER_SIZE;
        bool filter_push_down = true;
        bool use_footer_cache = false;
        bool use_native_reader = false;
        bool use_threads = false;
    } parquet;

    struct Orc
    {
        bool allow_missing_columns = false;
        int64_t row_batch_size = 100000;
        bool case_insensitive_column_matching = false;
        bool import_nested = false;
        std::unordered_set<int> skip_stripes = {};
        bool output_string_as_string = false;
        size_t use_fast_decoder = 0;
        bool allow_out_of_range = false;
        size_t current_file_offset = 0;
        size_t range_bytes = 0;
        bool filter_push_down = true;
        bool use_footer_cache = false;
    } orc;

    struct Pretty
    {
        UInt64 max_rows = 10000;
        UInt64 max_column_pad_width = 250;
        UInt64 max_value_width = 10000;
        bool color = true;

        bool output_format_pretty_row_numbers = false;

        enum class Charset
        {
            UTF8,
            ASCII,
        };

        Charset charset = Charset::UTF8;
    } pretty;

    struct
    {
        /**
         * Some buffers (kafka / rabbit) split the rows internally using callback,
         * and always send one row per message, so we can push there formats
         * without framing / delimiters (like ProtobufSingle). In other cases,
         * we have to enforce exporting at most one row in the format output,
         * because Protobuf without delimiters is not generally useful.
         */
        bool allow_multiple_rows_without_delimiter = false;
        /**
         * Some buffers like kafka only has one row each message without length
         *  delimiter. To be compatible with previous version, add this setting
         * to force ProtobufReader only consume one row in each buffer.
         */
        bool enable_multiple_message = false;
        /**
         * Only has meaning when enable_multiple_message is set to true.
         * When set to true, parse a varint header as message length. Otherwise, a 8 bytes fixed length header
         * will be read before each message.
         */
        bool default_length_parser = false;
    } protobuf;

    struct
    {
        uint32_t client_capabilities = 0;
        size_t max_packet_size = 0;
        uint8_t * sequence_id = nullptr; /// Not null if it's MySQLWire output format used to handle MySQL protocol connections.
        /**
         * COM_QUERY uses Text ResultSet
         * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
         * COM_STMT_EXECUTE uses Binary Protocol ResultSet
         * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute_response.html
         * By default, use Text ResultSet.
         */
        bool binary_protocol = false;
    } mysql_wire;

    struct
    {
        std::string regexp;
        std::string escaping_rule;
        bool skip_unmatched = false;
    } regexp;

    struct
    {
        bool parse_null_map_as_empty = true;
        bool skip_null_map_value = true;
        /**
         *  Because of the maximum filename length limit (255) of ext4 filesystem and the HTTP encode of map key.
         *  The maximum key length is approximately 255/3 ~= 85, which, in fact is not abosultely safe after
         *  accounting the map name and some auxiliary symbols.
         *
         *  Pick up a smaller and lucky number, hopes it work at most time.
         */
        uint64_t max_map_key_length = 80;
    } map;

    struct
    {
        std::string format_schema;
        std::string format_schema_path;
        bool is_server = false;
    } schema;

    struct
    {
        String resultset_format;
        String row_format;
        String row_between_delimiter;
    } template_settings;

    struct
    {
        bool empty_as_default = false;
        bool crlf_end_of_line = false;
        String null_representation = "\\N";
        bool input_format_enum_as_number = false;
    } tsv;

    struct
    {
        bool interpret_expressions = true;
        bool deduce_templates_of_expressions = true;
        bool accurate_types_of_literals = true;
    } values;

    /// For capnProto format we should determine how to
    /// compare ClickHouse Enum and Enum from schema.
    enum class EnumComparingMode
    {
        BY_NAMES, // Names in enums should be the same, values can be different.
        BY_NAMES_CASE_INSENSITIVE, // Case-insensitive name comparison.
        BY_VALUES, // Values should be the same, names can be different.
    };

    struct
    {
        EnumComparingMode enum_comparing_mode = EnumComparingMode::BY_VALUES;
        bool skip_fields_with_unsupported_types_in_schema_inference = false;
    } capn_proto;
};

}
