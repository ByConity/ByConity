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

#include <Common/SettingsChanges.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Protos/plan_node_utils.pb.h>

namespace DB
{
namespace
{
    SettingChange * find(SettingsChanges & changes, const std::string_view & name)
    {
        auto it = std::find_if(changes.begin(), changes.end(), [&name](const SettingChange & change) { return change.name == name; });
        if (it == changes.end())
            return nullptr;
        return &*it;
    }

    const SettingChange * find(const SettingsChanges & changes, const std::string_view & name)
    {
        auto it = std::find_if(changes.begin(), changes.end(), [&name](const SettingChange & change) { return change.name == name; });
        if (it == changes.end())
            return nullptr;
        return &*it;
    }
}

void SettingChange::serialize(WriteBuffer & buf) const
{
    writeBinary(name, buf);
    writeFieldBinary(value, buf);
}

void SettingChange::deserialize(ReadBuffer & buf)
{
    readBinary(name, buf);
    readFieldBinary(value, buf);
}

void SettingChange::toProto(Protos::SettingChange & proto) const
{
    proto.set_name(name);
    value.toProto(*proto.mutable_value());
}

void SettingChange::fillFromProto(const Protos::SettingChange & proto)
{
    name = proto.name();
    value.fillFromProto(proto.value());
}

bool SettingsChanges::tryGet(const std::string_view & name, Field & out_value) const
{
    const auto * change = find(*this, name);
    if (!change)
        return false;
    out_value = change->value;
    return true;
}

const Field * SettingsChanges::tryGet(const std::string_view & name) const
{
    const auto * change = find(*this, name);
    if (!change)
        return nullptr;
    return &change->value;
}

Field * SettingsChanges::tryGet(const std::string_view & name)
{
    auto * change = find(*this, name);
    if (!change)
        return nullptr;
    return &change->value;
}

bool SettingsChanges::insertSetting(std::string_view name, const Field & value)
{
    auto it = std::find_if(begin(), end(), [&name](const SettingChange & change) { return change.name == name; });
    if (it != end())
        return false;
    emplace_back(name, value);
    return true;
}

void SettingsChanges::setSetting(std::string_view name, const Field & value)
{
    if (auto * setting_value = tryGet(name))
        *setting_value = value;
    else
        insertSetting(name, value);
}

bool SettingsChanges::removeSetting(std::string_view name)
{
    auto it = std::find_if(begin(), end(), [&name](const SettingChange & change) { return change.name == name; });
    if (it == end())
        return false;
    erase(it);
    return true;
}

void SettingsChanges::merge(const SettingsChanges & other)
{
    for (const auto & change : other)
        setSetting(change.name, change.value);
}

void SettingsChanges::serialize(WriteBuffer & buf) const
{
    writeBinary(size(), buf);
    for (auto & change : *this)
        change.serialize(buf);
}

void SettingsChanges::deserialize(ReadBuffer & buf)
{
    size_t size;
    readBinary(size, buf);
    for (size_t i = 0; i < size; ++i)
    {
        SettingChange change;
        change.deserialize(buf);
        this->push_back(change);
    }
}

void SettingsChanges::toProto(Protos::SettingsChanges & proto) const
{
    for (const auto & element : *this)
        element.toProto(*proto.add_settings_changes());
}

void SettingsChanges::fillFromProto(const Protos::SettingsChanges & proto)
{
    for (const auto & proto_element : proto.settings_changes())
    {
        SettingChange element;
        element.fillFromProto(proto_element);
        this->emplace_back(std::move(element));
    }
}
std::unordered_set<String> SettingsChanges::WHITELIST_SETTINGS =
    {
        "access_table_names",
        "accessible_table_names",
        "active_role",
        "add_http_cors_header",
        "additional_background_pool_size",
        "aggregation_memory_efficient_merge_threads",
        "aggressive_merge_in_optimize",
        "allow_ddl",
        "allow_distributed_ddl",
        "allow_experimental_cross_to_join_conversion",
        "allow_experimental_data_skipping_indices",
        "allow_experimental_geo_types",
        "allow_experimental_multiple_joins_emulation",
        "allow_hyperscan",
        "allow_map_access_without_key",
        "allow_simdjson",
        "allows_query_when_mysql_lost",
        "ansi_sql",
        "asterisk_left_columns_only",
        "background_consume_schedule_pool_size",
        "background_dump_thread_pool_size",
        "background_pool_size",
        "background_schedule_pool_size",
        "backup_virtual_warehouse",
        "batch_size_in_attaching_parts",
        "calculate_text_stack_trace",
        "cancel_http_readonly_queries_on_client_close",
        "cascading_refresh_materialized_view",
        "catalog_enable_streaming_rpc",
        "check_consistency",
        "check_query_single_value_result",
        "cnch_alter_task_timeout",
        "cnch_offloading_mode",
        "cnch_server_vw",
        "cnch_vw_default",
        "cnch_vw_write",
        "compile",
        "compile_expressions",
        "connect_timeout",
        "connect_timeout_with_failover_ms",
        "connection_check_pool_size",
        "connections_with_failover_max_tries",
        "conservative_merge_predicate",
        "constraint_skip_violate",
        "count_distinct_implementation",
        "cte_mode",
        "databases_load_pool_size",
        "date_time_input_format",
        "debug_cnch_force_commit_parts_rpc",
        "debug_cnch_remain_temp_part",
        "decimal_check_overflow",
        "decode_with_merged_dict",
        "decrease_error_period",
        "deduce_part_eliminate",
        "delay_dequeue_ms",
        "dialect_type",
        "direct_forward_query_to_cnch",
        "disable_perfect_shard_auto_merge",
        "disable_remote_stream_log",
        "disk_cache_mode",
        "distributed_aggregation_memory_efficient",
        "distributed_connections_pool_size",
        "distributed_ddl_task_timeout",
        "distributed_directory_monitor_batch_inserts",
        "distributed_directory_monitor_sleep_time_ms",
        "distributed_group_by_no_merge",
        "distributed_max_parallel_size",
        "distributed_perfect_shard",
        "distributed_query_has_limit_clause",
        "distributed_to_local",
        "drop_vw_disk_cache",
        "empty_result_for_aggregation_by_empty_set",
        "enable_ab_index_optimization",
        "enable_async_build_bitmap_in_attach",
        "enable_async_execution",
        "enable_bloom_filter",
        "enable_debug_queries",
        "enable_detail_event_log",
        "enable_deterministic_sample_by_range",
        "enable_dictionary_compression",
        "enable_direct_insert",
        "enable_distinct_remove",
        "enable_distributed_stages",
        "enable_dynamic_filter",
        "enable_final_for_delta",
        "enable_final_sample",
        "enable_gc_evict_disk_cache",
        "enable_http_compression",
        "enable_hybrid_allocation",
        "enable_left_join_to_right_join",
        "enable_materialized_view_rewrite",
        "enable_merge_scheduler",
        "enable_nullable_sorting_key",
        "enable_operator_level_profile",
        "enable_optimize_predicate_expression",
        "enable_optimizer",
        "enable_optimizer_fallback",
        "enable_optimizer_for_create_select",
        "enable_parts_sync_preload",
        "enable_predicate_pushdown",
        "enable_preload_parts",
        "enable_push_partial_agg",
        "enable_query_cache",
        "enable_query_level_profiling",
        "enable_query_metadata",
        "enable_query_queue",
        "enable_range_bloom_filter",
        "enable_rollup_by_time_opt",
        "enable_sample_by_range",
        "enable_sync_build_bitmap",
        "enable_sync_fetch",
        "enable_sync_from_ha",
        "enable_table_scan_build_pipeline_optimization",
        "enable_testlog_to_console",
        "enable_unaligned_array_join",
        "enable_variadic_arraySetCheck",
        "enable_view_based_query_rewrite",
        "enable_virtual_part",
        "enable_windows_parallel",
        "encrypt_key",
        "exchange_enable_metric",
        "exchange_multi_path_receiver_queue_size",
        "exchange_output_parallel_size",
        "exchange_remote_receiver_queue_size",
        "exchange_source_pipeline_threads",
        "exchange_timeout",
        "exchange_timeout_ms",
        "exchange_unordered_output_parallel_size",
        "exchange_wait_accept_max_timeout_ms",
        "expired_end_hour_to_merge",
        "expired_start_hour_to_merge",
        "external_table_functions_use_nulls",
        "extremes",
        "fallback_to_stale_replicas_for_distributed_queries",
        "force_index_by_date",
        "force_primary_key",
        "force_query_to_shard",
        "force_release_when_mmap_exceed",
        "format_csv_allow_double_quotes",
        "format_csv_allow_single_quotes",
        "format_csv_delimiter",
        "format_csv_write_utf8_with_bom",
        "format_protobuf_default_length_parser",
        "format_protobuf_enable_multiple_message",
        "format_schema",
        "free_resource_early_in_write",
        "fsync_metadata",
        "funnel_old_rule",
        "grace_hash_join_initial_buckets",
        "group_by_overflow_mode",
        "group_by_two_level_threshold",
        "group_by_two_level_threshold_bytes",
        "http_connection_timeout",
        "http_headers_progress_interval_ms",
        "http_max_multipart_form_data_size",
        "http_native_compression_disable_checksumming_on_decompress",
        "http_receive_timeout",
        "http_send_timeout",
        "http_zlib_compression_level",
        "idle_connection_timeout",
        "input_format_allow_errors_num",
        "input_format_allow_errors_ratio",
        "input_format_defaults_for_omitted_fields",
        "input_format_import_nested_json",
        "input_format_json_aggregate_function_type_base64_encode",
        "input_format_max_map_key_long",
        "input_format_parse_null_map_as_empty",
        "input_format_skip_null_map_value",
        "input_format_skip_unknown_fields",
        "input_format_values_interpret_expressions",
        "insert_allow_materialized_columns",
        "insert_deduplicate",
        "insert_distributed_sync",
        "insert_distributed_timeout",
        "insert_quorum",
        "insert_quorum_timeout",
        "insert_select_with_profiles",
        "insertion_label",
        "interactive_delay",
        "join_algorithm",
        "join_any_take_last_row",
        "join_use_nulls",
        "joined_subquery_requires_alias",
        "kafka_max_partition_fetch_bytes",
        "kafka_session_timeout_ms",
        "kms_token",
        "lasfs_access_key",
        "lasfs_endpoint",
        "lasfs_identity_id",
        "lasfs_identity_type",
        "lasfs_overwrite",
        "lasfs_region",
        "lasfs_secret_key",
        "lasfs_service_name",
        "lasfs_session_token",
        "load_balancing_offset",
        "local_disk_cache_thread_pool_size",
        "log_id",
        "log_profile_events",
        "log_queries",
        "log_queries_cut_to_length",
        "log_query_settings",
        "log_query_threads",
        "log_segment_profiles",
        "low_cardinality_allow_in_native_format",
        "low_cardinality_max_dictionary_size",
        "low_cardinality_use_single_dictionary_for_part",
        "mark_cache_min_lifetime",
        "materialized_mysql_tables_list",
        "max_ast_depth",
        "max_ast_elements",
        "max_block_size",
        "max_bytes_before_external_group_by",
        "max_bytes_before_external_sort",
        "max_bytes_before_remerge_sort",
        "max_bytes_in_buffer",
        "max_bytes_in_buffers",
        "max_bytes_in_distinct",
        "max_bytes_in_join",
        "max_bytes_in_set",
        "max_bytes_to_read",
        "max_bytes_to_sort",
        "max_bytes_to_transfer",
        "max_columns_to_read",
        "max_compress_block_size",
        "max_compressed_bytes_to_read",
        "max_concurrent_queries_for_user",
        "max_distributed_connections",
        "max_execution_speed",
        "max_execution_speed_bytes",
        "max_execution_time",
        "max_expanded_ast_elements",
        "max_fetch_partition_retries_count",
        "max_flush_data_time",
        "max_hdfs_read_network_bandwidth",
        "max_hdfs_write_buffer_size",
        "max_ingest_columns_size",
        "max_ingest_rows_size",
        "max_insert_block_size",
        "max_insert_block_size_bytes",
        "max_insert_threads",
        "max_memory_usage",
        "max_memory_usage_for_all_queries",
        "max_memory_usage_for_user",
        "max_network_bandwidth",
        "max_network_bandwidth_for_all_users",
        "max_network_bandwidth_for_fetch",
        "max_network_bandwidth_for_user",
        "max_network_bytes",
        "max_parallel_replicas",
        "max_parallel_threads_for_resharding",
        "max_partitions_for_resharding",
        "max_partitions_per_insert_block",
        "max_parts_to_optimize",
        "max_pipeline_depth",
        "max_query_cpu_seconds",
        "max_query_size",
        "max_read_buffer_size",
        "max_replica_delay_for_distributed_queries",
        "max_replica_delay_for_write_queries",
        "max_result_bytes",
        "max_result_rows",
        "max_rows_for_resharding",
        "max_rows_in_buffer",
        "max_rows_in_buffers",
        "max_rows_in_distinct",
        "max_rows_in_join",
        "max_rows_in_set",
        "max_rows_to_group_by",
        "max_rows_to_read",
        "max_rows_to_schedule_merge",
        "max_rows_to_sort",
        "max_rows_to_transfer",
        "max_rpc_read_timeout_ms",
        "max_sample_size_for_optimize",
        "max_streams_to_max_threads_ratio",
        "max_subquery_depth",
        "max_temporary_columns",
        "max_temporary_non_const_columns",
        "max_threads",
        "max_threads_for_cnch_dump",
        "max_uncompressed_bytes_to_read",
        "max_wait_time_when_mysql_unavailable",
        "memory_tracker_fault_probability",
        "merge_tree_coarse_index_granularity",
        "merge_tree_max_rows_to_use_cache",
        "merge_tree_min_rows_for_concurrent_read",
        "merge_tree_min_rows_for_seek",
        "merge_tree_uniform_read_distribution",
        "mimic_replica_name",
        "min_bytes_to_use_direct_io",
        "min_compress_block_size",
        "min_count_to_compile",
        "min_count_to_compile_expression",
        "min_execution_speed",
        "min_execution_speed_bytes",
        "min_insert_block_size_bytes",
        "min_insert_block_size_rows",
        "min_rows_per_vp",
        "multiple_joins_rewriter_version",
        "mysql_max_rows_to_insert",
        "network_compression_method",
        "network_zstd_compression_level",
        "ntimes_slower_to_alarm",
        "odbc_max_field_size",
        "optimize_map_column_serialization",
        "optimize_min_equality_disjunction_chain_length",
        "optimize_move_to_prewhere",
        "optimize_skip_unused_shards",
        "optimize_subpart_key",
        "optimize_subpart_number",
        "optimize_throw_if_noop",
        "outfile_in_server_with_tcp",
        "output_format_json_escape_forward_slashes",
        "output_format_json_quote_64bit_integers",
        "output_format_json_quote_denormals",
        "output_format_parquet_row_group_size",
        "output_format_pretty_color",
        "output_format_pretty_max_column_pad_width",
        "output_format_pretty_max_rows",
        "output_format_write_statistics",
        "parallel_fetch_part",
        "parallel_replica_offset",
        "parallel_replicas_count",
        "parallel_view_processing",
        "part_cache_min_lifetime",
        "parts_load_pool_size",
        "parts_preallocate_pool_size",
        "parts_preload_level",
        "pathgraph_threshold_x",
        "pathgraph_threshold_y",
        "poll_interval",
        "prefer_cnch_catalog",
        "prefer_localhost_replica",
        "preferred_block_size_bytes",
        "preferred_max_column_in_block_size_bytes",
        "preload_checksums_and_primary_index_cache",
        "priority",
        "process_list_block_time",
        "profile",
        "query_auto_retry",
        "query_auto_retry_millisecond",
        "query_cache_min_lifetime",
        "query_queue_timeout_ms",
        "query_worker_fault_tolerance",
        "queue_max_wait_ms",
        "read_backoff_max_throughput",
        "read_backoff_min_events",
        "read_backoff_min_interval_between_events_ms",
        "read_backoff_min_latency_ms",
        "read_only_replica_info",
        "read_overflow_mode",
        "readonly",
        "receive_timeout",
        "remote_query_memory_table",
        "replace_running_query",
        "replication_alter_columns_timeout",
        "replication_alter_partitions_sync",
        "restore_table_expression_in_distributed",
        "result_overflow_mode",
        "rm_zknodes_while_alter_engine",
        "s3_access_key_id",
        "s3_access_key_secret",
        "s3_ak_id",
        "s3_ak_secret",
        "s3_check_objects_after_upload",
        "s3_endpoint",
        "s3_gc_inter_partition_parallelism",
        "s3_gc_intra_partition_parallelism",
        "s3_max_connections",
        "s3_max_list_nums",
        "s3_max_redirects",
        "s3_max_request_ms",
        "s3_max_single_part_upload_size",
        "s3_max_single_read_retries",
        "s3_max_unexpected_write_error_retries",
        "s3_min_upload_part_size",
        "s3_region",
        "s3_skip_empty_files",
        "s3_upload_part_size_multiply_factor",
        "s3_upload_part_size_multiply_parts_count_threshold",
        "s3_use_virtual_hosted_style",
        "schedule_sync_thread_per_table",
        "select_sequential_consistency",
        "send_logs_level",
        "send_progress_in_http_headers",
        "send_timeout",
        "show_table_uuid_in_table_create_query_if_not_nil",
        "skip_error_count",
        "skip_history",
        "skip_nullinput_notnull_col",
        "skip_table_definition_hash_check",
        "skip_unavailable_shards",
        "slow_query_ms",
        "snappy_format_blocked",
        "statement_timeout_in_seconds",
        "stream_flush_interval_ms",
        "stream_poll_timeout_ms",
        "strict_rows_to_schedule_merge",
        "sync_thread_heartbeat_interval_ms",
        "sync_thread_max_heartbeat_interval_second",
        "sync_thread_schedule_interval_ms",
        "system_mutations_only_basic_info",
        "table_function_remote_max_addresses",
        "tables_load_pool_size",
        "tcp_keep_alive_timeout",
        "tealimit_order_keep",
        "timeout_before_checking_execution_speed",
        "tos_access_key",
        "tos_connection_timeout",
        "tos_endpoint",
        "tos_region",
        "tos_request_timeout",
        "tos_secret_key",
        "tos_security_token",
        "totals_auto_threshold",
        "totals_mode",
        "underlying_dictionary_tables",
        "use_ansi_sql",
        "use_client_time_zone",
        "use_encoded_bitmap",
        "use_index_for_in_with_subqueries",
        "use_uncompressed_cache",
        "virtual_part_size",
        "virtual_warehouse",
        "virtual_warehouse_write",
        "vw"
    };

}
