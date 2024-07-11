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

#include <Core/BaseSettings.h>
#include <Core/Defines.h>
#include <Core/SettingsEnums.h>
#include <Poco/JSON/Object.h>


namespace Poco::Util
{
class AbstractConfiguration;
}

namespace boost::program_options
{
class options_description;
}


namespace DB
{
class IColumn;

/** List of settings: type, name, default value, description, flags
  *
  * This looks rather inconvenient. It is done that way to avoid repeating settings in different places.
  * Note: as an alternative, we could implement settings to be completely dynamic in form of map: String -> Field,
  *  but we are not going to do it, because settings is used everywhere as static struct fields.
  *
  * `flags` can be either 0 or IMPORTANT.
  * A setting is "IMPORTANT" if it affects the results of queries and can't be ignored by older versions.
  */

// The variable need be same with `parts_preload_level`
enum PreloadLevelSettings : UInt64
{
    ClosePreload = 0,
    MetaPreload = 1,
    DataPreload = 2,
    AllPreload = 3
};

#define COMMON_SETTINGS(M) \
    M(UInt64, \
      TEST_KNOB, \
      0, \
      "A placeholder for experiment features. Each bit represents a feature, the feature is enabled if the bit is 1", \
      0) \
    M(UInt64, \
      min_compress_block_size, \
      65536, \
      "The actual size of the block to compress, if the uncompressed data less than max_compress_block_size is no less than this value " \
      "and no less than the volume of data for one mark.", \
      0) \
    M(UInt64, \
      max_compress_block_size, \
      1048576, \
      "The maximum size of blocks of uncompressed data before compressing for writing to a table.", \
      0) \
    M(UInt64, max_block_size, DEFAULT_BLOCK_SIZE, "Maximum block size for reading", 0) \
    M(UInt64, max_insert_block_size, DEFAULT_INSERT_BLOCK_SIZE, "The maximum block size for insertion, if we control the creation of blocks for insertion.", 0) \
    M(UInt64, max_insert_block_size_bytes, DEFAULT_BLOCK_SIZE_BYTES, "The maximum block bytes for insertion, if we control the creation of blocks for insertion.", 0) \
    M(UInt64, min_insert_block_size_rows, DEFAULT_INSERT_BLOCK_SIZE, "Squash blocks passed to INSERT query to specified size in rows, if blocks are not big enough.", 0) \
    M(UInt64, min_insert_block_size_bytes, (DEFAULT_INSERT_BLOCK_SIZE * 256), "Squash blocks passed to INSERT query to specified size in bytes, if blocks are not big enough.", 0) \
    M(UInt64, min_insert_block_size_rows_for_materialized_views, 0, "Like min_insert_block_size_rows, but applied only during pushing to MATERIALIZED VIEW (default: min_insert_block_size_rows)", 0) \
    M(UInt64, min_insert_block_size_bytes_for_materialized_views, 0, "Like min_insert_block_size_bytes, but applied only during pushing to MATERIALIZED VIEW (default: min_insert_block_size_bytes)", 0) \
    M(UInt64, max_joined_block_size_rows, DEFAULT_BLOCK_SIZE, "Maximum block size for JOIN result (if join algorithm supports it). 0 means unlimited.", 0) \
    M(UInt64, max_insert_threads, 0, "The maximum number of threads to execute the INSERT SELECT query. Values 0 or 1 means that INSERT SELECT is not run in parallel. Higher values will lead to higher memory usage. Parallel INSERT SELECT has effect only if the SELECT part is run on parallel, see 'max_threads' setting.", 0) \
    M(Bool, enable_insert_squashing, true, "Squashing when insert", 0) \
    M(UInt64, max_final_threads, 16, "The maximum number of threads to read from table with FINAL.", 0) \
    M(MaxThreads, max_threads, 0, "The maximum number of threads to execute the request. By default, it is determined automatically.", 0) \
    M(MaxThreads, \
      max_alter_threads, \
      0, \
      "The maximum number of threads to execute the ALTER requests. By default, it is determined automatically.", \
      0) \
    M(UInt64, max_read_buffer_size, DBMS_DEFAULT_BUFFER_SIZE, "The maximum size of the buffer to read from the filesystem.", 0) \
    M(UInt64, max_read_buffer_size_local_fs, 128*1024, "The maximum size of the buffer to read from local filesystem. If set to 0 then max_read_buffer_size will be used.", 0) \
    M(UInt64, max_read_buffer_size_remote_fs, 0, "The maximum size of the buffer to read from remote filesystem. If set to 0 then max_read_buffer_size will be used.", 0) \
    M(UInt64, max_distributed_connections, 1024, "The maximum number of connections for distributed processing of one query (should be greater than max_threads).", 0) \
    M(UInt64, max_query_size, DBMS_DEFAULT_MAX_QUERY_SIZE, "Which part of the query can be read into RAM for parsing (the remaining data for INSERT, if any, is read later)", 0) \
    M(UInt64, interactive_delay, 100000, "The interval in microseconds to check if the request is cancelled, and to send progress info.", 0) \
    M(UInt64, interactive_delay_optimizer_mode, 0, "The interval(in optimizer mode) in microseconds to check if the request is cancelled, and to send progress info.", 0) \
    M(Seconds, connect_timeout, DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, "Connection timeout if there are no replicas.", 0) \
    M(Milliseconds, \
      connect_timeout_with_failover_ms, \
      DBMS_DEFAULT_CONNECT_TIMEOUT_WITH_FAILOVER_MS, \
      "Connection timeout for selecting first healthy replica.", \
      0) \
    M(Milliseconds, \
      connect_timeout_with_failover_secure_ms, \
      DBMS_DEFAULT_CONNECT_TIMEOUT_WITH_FAILOVER_SECURE_MS, \
      "Connection timeout for selecting first healthy replica (for secure connections).", \
      0) \
    M(Seconds, receive_timeout, DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, "", 0) \
    M(Seconds, send_timeout, DBMS_DEFAULT_SEND_TIMEOUT_SEC, "", 0) \
    M(Seconds, \
      tcp_keep_alive_timeout, \
      0, \
      "The time in seconds the connection needs to remain idle before TCP starts sending keepalive probes", \
      0) \
    M(Milliseconds, \
      hedged_connection_timeout_ms, \
      DBMS_DEFAULT_HEDGED_CONNECTION_TIMEOUT_MS, \
      "Connection timeout for establishing connection with replica for Hedged requests", \
      0) \
    M(Milliseconds, \
      receive_data_timeout_ms, \
      DBMS_DEFAULT_RECEIVE_DATA_TIMEOUT_MS, \
      "Connection timeout for receiving first packet of data or packet with positive progress from replica", \
      0) \
    M(Bool, use_hedged_requests, false, "Use hedged requests for distributed queries", 0) \
    M(Bool, \
      allow_changing_replica_until_first_data_packet, \
      false, \
      "Allow HedgedConnections to change replica until receiving first data packet", \
      0) \
    M(Milliseconds, \
      queue_max_wait_ms, \
      0, \
      "The wait time in the request queue, if the number of concurrent requests exceeds the maximum.", \
      0) \
    M(Milliseconds, connection_pool_max_wait_ms, 0, "The wait time when the connection pool is full.", 0) \
    M(Milliseconds, \
      replace_running_query_max_wait_ms, \
      5000, \
      "The wait time for running query with the same query_id to finish when setting 'replace_running_query' is active.", \
      0) \
    M(Milliseconds, kafka_max_wait_ms, 5000, "The wait time for reading from Kafka before retry.", 0) \
    M(Milliseconds, kafka_session_timeout_ms, 180000, "Kafka client session.timeout.ms", 0) \
    M(UInt64, kafka_refresh_consul_time, 60 * 60, "Time to refresh consul", 0) \
    M(Bool, enable_debug_select_from_kafka_table, 0, "Enable read from StorageHaKafka for debug", 0) \
    M(Bool, constraint_skip_violate, 0, "Whether to skip constraint violated rows.", 0) \
    M(Milliseconds, rabbitmq_max_wait_ms, 5000, "The wait time for reading from RabbitMQ before retry.", 0) \
    M(UInt64, \
      poll_interval, \
      DBMS_DEFAULT_POLL_INTERVAL, \
      "Block at the query wait loop on the server for the specified number of seconds.", \
      0) \
    M(UInt64, idle_connection_timeout, 3600, "Close idle TCP connections after specified number of seconds.", 0) \
    M(UInt64, \
      distributed_connections_pool_size, \
      DBMS_DEFAULT_DISTRIBUTED_CONNECTIONS_POOL_SIZE, \
      "Maximum number of connections with one remote server in the pool.", \
      0) \
    M(UInt64, \
      connections_with_failover_max_tries, \
      DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES, \
      "The maximum number of attempts to connect to replicas.", \
      0) \
    M(UInt64, s3_min_upload_part_size, 512 * 1024 * 1024, "The minimum size of part to upload during multipart upload to S3.", 0) \
    M(UInt64, s3_max_single_part_upload_size, 64 * 1024 * 1024, "The maximum size of object to upload using singlepart upload to S3.", 0) \
    M(UInt64, s3_max_single_read_retries, 4, "The maximum number of retries during single S3 read.", 0) \
    M(UInt64, s3_max_redirects, 10, "Max number of S3 redirects hops allowed.", 0) \
    M(UInt64, s3_max_connections, 1024, "The maximum number of connections per server.", 0) \
    M(Bool, \
      s3_check_objects_after_upload, \
      false, \
      "Check each uploaded object to s3 with head request to be sure that upload was successful", \
      0) \
    M(UInt64, s3_max_unexpected_write_error_retries, 4, "The maximum number of retries in case of unexpected errors during S3 write.", 0) \
    M(UInt64, \
      s3_upload_part_size_multiply_factor, \
      2, \
      "Multiply s3_min_upload_part_size by this factor each time s3_multiply_parts_count_threshold parts were uploaded from a single " \
      "write to S3.", \
      0) \
    M(UInt64, \
      s3_upload_part_size_multiply_parts_count_threshold, \
      500, \
      "Each time this number of parts was uploaded to S3 s3_min_upload_part_size multiplied by s3_upload_part_size_multiply_factor.", \
      0) \
    M(String, s3_access_key_id, "", "S3 table access key id", 0) \
    M(String, s3_access_key_secret, "", "S3 table access key secret", 0) \
    M(UInt64, s3_max_list_nums, 1000, "Sets the maximum number of keys returned in the response, now it is just for CnchS3", 0) \
    M(UInt64, s3_max_request_ms, 30000, "Request max timeout ms , now it is just for CnchS3", 0) \
    M(Bool, overwrite_current_file, false, "Enable overwrite current file, now it is just for CnchS3/CnchHDFS", 0) \
    M(Bool, insert_new_file, true, "Create new file when write data into the file, now it is just for CnchS3/CnchHDFS", 0) \
    M(Bool, extremes, false, "Calculate minimums and maximums of the result columns. They can be output in JSON-formats.", IMPORTANT) \
    M(Bool, use_uncompressed_cache, false, "Whether to use the cache of uncompressed blocks.", 0) \
    M(Bool, replace_running_query, false, "Whether the running request should be canceled with the same id as the new one.", 0) \
    M(UInt64, background_buffer_flush_schedule_pool_size, 16, "Number of threads performing background flush for tables with Buffer engine. Only has meaning at server startup.", 0) \
    M(UInt64, background_pool_size, 16, "Number of threads performing background work for tables (for example, merging in merge tree). Only has meaning at server startup.", 0) \
    M(UInt64, distributed_data_service_pool_size, 16, "Number of threads response distributed file data.", 0) \
    M(UInt64, distributed_data_service_max_retry_count, 3, "Number of threads response distributed file data.", 0) \
    M(UInt64, unique_table_background_pool_size, 16, "Number of threads performing background work for tables (for example, merging in merge tree). Only has meaning at server startup.", 0) \
    M(UInt64, background_move_pool_size, 8, "Number of threads performing background moves for tables. Only has meaning at server startup.", 0) \
    M(UInt64, background_fetches_pool_size, 8, "Number of threads performing background fetches for replicated tables. Only has meaning at server startup.", 0) \
    M(UInt64, background_schedule_pool_size, 128, "Number of threads performing background tasks for replicated tables, dns cache updates. Only has meaning at server startup.", 0) \
    M(UInt64, background_message_broker_schedule_pool_size, 16, "Number of threads performing background tasks for message streaming. Only has meaning at server startup.", 0) \
    M(UInt64, background_distributed_schedule_pool_size, 16, "Number of threads performing background tasks for distributed sends. Only has meaning at server startup.", 0) \
    M(UInt64, background_local_schedule_pool_size, 16, "Number of threads performing background no-network operation tasks for replicated tables. Only has meaning at server startup.", 0) \
    M(UInt64, background_consume_schedule_pool_size, 16, "Number of threads performing background tasks for kafka tables. Only has meaning at server startup.", 0) \
    M(UInt64, background_unique_table_schedule_pool_size, 16, "Number of threads performing background tasks for unique tables. Only has meaning at server startup.", 0) \
    M(UInt64, background_memory_table_schedule_pool_size, 24, "Number of threads performing background tasks for memory table. Only has meaning at server startup.", 0) \
    M(UInt64, background_topology_thread_pool_size, 4, "Number of threads performing topology related background tasks.", 0) \
    M(UInt64, background_metrics_recalculation_schedule_pool_size, 16, "Number of threads performing metrics recalculation related background tasks.", 0) \
    M(UInt64, background_gc_schedule_pool_size, 16, "Number of threads performing data removing related background tasks.", 0) \
    M(UInt64, local_disk_cache_thread_pool_size, 16, "Number of threads perforrming background tasks from cache segments from cloud storage to local disk. Only has meaning at server startup.", 0) \
    M(UInt64, local_disk_cache_evict_thread_pool_size, 16, "Number of threads perforrming asynchronous remove disk cache file.", 0) \
    M(UInt64, \
      max_bandwidth_for_disk_cache, \
      0, \
      "The maximum speed for loading from disk cache. Zero means unlimited. Only has meaning at server startup.", \
      0) \
    M(UInt64, \
      max_replicated_fetches_network_bandwidth_for_server, \
      0, \
      "The maximum speed of data exchange over the network in bytes per second for replicated fetches. Zero means unlimited. Only has " \
      "meaning at server startup.", \
      0) \
    M(UInt64, \
      max_replicated_sends_network_bandwidth_for_server, \
      0, \
      "The maximum speed of data exchange over the network in bytes per second for replicated sends. Zero means unlimited. Only has " \
      "meaning at server startup.", \
      0) \
\
    M(Milliseconds, \
      distributed_directory_monitor_sleep_time_ms, \
      100, \
      "Sleep time for StorageDistributed DirectoryMonitors, in case of any errors delay grows exponentially.", \
      0) \
    M(Milliseconds, \
      distributed_directory_monitor_max_sleep_time_ms, \
      30000, \
      "Maximum sleep time for StorageDistributed DirectoryMonitors, it limits exponential growth too.", \
      0) \
\
    M(Bool, \
      distributed_directory_monitor_batch_inserts, \
      false, \
      "Should StorageDistributed DirectoryMonitors try to batch individual inserts into bigger ones.", \
      0) \
    M(Bool, \
      distributed_directory_monitor_split_batch_on_failure, \
      false, \
      "Should StorageDistributed DirectoryMonitors try to split batch into smaller in case of failures.", \
      0) \
\
    M(Bool, optimize_move_to_prewhere, true, "Allows disabling WHERE to PREWHERE optimization in SELECT queries from MergeTree.", 0) \
    M(Bool, \
      optimize_move_to_prewhere_if_final, \
      false, \
      "If query has `FINAL`, the optimization `move_to_prewhere` is not always correct and it is enabled only if both settings " \
      "`optimize_move_to_prewhere` and `optimize_move_to_prewhere_if_final` are turned on", \
      0) \
\
    M(UInt64, \
      replication_alter_partitions_sync, \
      1, \
      "Wait for actions to manipulate the partitions. 0 - do not wait, 1 - wait for execution only of itself, 2 - wait for everyone.", \
      0) \
    M(UInt64, \
      replication_alter_columns_timeout, \
      60, \
      "Wait for actions to change the table structure within the specified number of seconds. 0 - wait unlimited time.", \
      0) \
\
    M(LoadBalancing, \
      load_balancing, \
      LoadBalancing::RANDOM, \
      "Which replicas (among healthy replicas) to preferably send a query to (on the first attempt) for distributed processing.", \
      0) \
    M(UInt64, \
      load_balancing_first_offset, \
      0, \
      "Which replica to preferably send a query when FIRST_OR_RANDOM load balancing strategy is used.", \
      0) \
\
    M(TotalsMode, \
      totals_mode, \
      TotalsMode::AFTER_HAVING_EXCLUSIVE, \
      "How to calculate TOTALS when HAVING is present, as well as when max_rows_to_group_by and group_by_overflow_mode = ‘any’ are " \
      "present.", \
      IMPORTANT) \
    M(Float, totals_auto_threshold, 0.5, "The threshold for totals_mode = 'auto'.", 0) \
    M(Bool, allow_suspicious_low_cardinality_types, true, "In CREATE TABLE statement allows specifying LowCardinality modifier for types of small fixed size (8 or less). Enabling this may increase merge times and memory consumption.", 0) \
    M(Bool, allow_suspicious_fixed_string_types, false, "In CREATE TABLE statement allows creating columns of type FixedString(n) with n > 256. FixedString with length >= 256 is suspicious and most likely indicates misusage", 0) \
    M(Bool, compile_expressions, true, "Compile some scalar functions and operators to native code.", 0) \
    M(UInt64, min_count_to_compile_expression, 3, "The number of identical expressions before they are JIT-compiled", 0) \
    M(Bool, compile_aggregate_expressions, true, "Compile aggregate functions to native code.", 0) \
    M(UInt64, min_count_to_compile_aggregate_expression, 3, "The number of identical aggregate expressions before they are JIT-compiled", 0) \
    M(UInt64, group_by_two_level_threshold, 100000, "From what number of keys, a two-level aggregation starts. 0 - the threshold is not set.", 0) \
    M(UInt64, group_by_two_level_threshold_bytes, 50000000, "From what size of the aggregation state in bytes, a two-level aggregation begins to be used. 0 - the threshold is not set. Two-level aggregation is used when at least one of the thresholds is triggered.", 0) \
    M(Bool, group_by_two_level_for_grouping_set, true, "Adaptive two-level aggregation is not valid for grouping set queries. Setting 1 to enforce two-level aggregation, 0 to enforce single-level aggregation.", 0) \
    M(Bool, distributed_aggregation_memory_efficient, true, "Is the memory-saving mode of distributed aggregation enabled.", 0) \
    M(UInt64, \
      aggregation_memory_efficient_merge_threads, \
      0, \
      "Number of threads to use for merge intermediate aggregation results in memory efficient mode. When bigger, then more memory is " \
      "consumed. 0 means - same as 'max_threads'.", \
      0) \
    M(Bool, enable_positional_arguments, true, "Enable positional arguments in ORDER BY, GROUP BY and LIMIT BY", 0) \
\
    M(Bool, \
      enable_extended_results_for_datetime_functions, \
      false, \
      "Enable date functions like toLastDayOfMonth return Date32 results (instead of Date results) for Date32/DateTime64 arguments.", \
      0) \
\
    M(UInt64, \
      max_parallel_replicas, \
      1, \
      "The maximum number of replicas of each shard used when the query is executed. For consistency (to get different parts of the same " \
      "partition), this option only works for the specified sampling key. The lag of the replicas is not controlled.", \
      0) \
    M(UInt64, parallel_replicas_count, 0, "", 0) \
    M(UInt64, parallel_replica_offset, 0, "", 0) \
    \
    M(Bool, skip_unavailable_shards, false, "If 1, ClickHouse silently skips unavailable shards and nodes unresolvable through DNS. Shard is marked as unavailable when none of the replicas can be reached.", 0) \
    \
    M(UInt64, parallel_distributed_insert_select, 0, "Process distributed INSERT SELECT query in the same cluster on local tables on every shard, if 1 SELECT is executed on each shard, if 2 SELECT and INSERT is executed on each shard", 0) \
    M(UInt64, distributed_group_by_no_merge, 0, "If 1, Do not merge aggregation states from different servers for distributed queries (shards will process query up to the Complete stage, initiator just proxies the data from the shards). If 2 the initiator will apply ORDER BY and LIMIT stages (it is not in case when shard process query up to the Complete stage)", 0) \
    M(UInt64, distributed_push_down_limit, 0, "If 1, LIMIT will be applied on each shard separateoly. Usually you don't need to use it, since this will be done automatically if it is possible, i.e. for simple query SELECT FROM LIMIT.", 0) \
    M(Bool, optimize_distributed_group_by_sharding_key, false, "Optimize GROUP BY sharding_key queries (by avoiding costly aggregation on the initiator server).", 0) \
    M(UInt64, optimize_skip_unused_shards_limit, 1000, "Limit for number of sharding key values, turns off optimize_skip_unused_shards if the limit is reached", 0) \
    M(Bool, distributed_perfect_shard, false, "Whether to enable aggregation finished in worker side, to avoid merge aggregation states in coordinator", 0) \
    M(Bool, fallback_perfect_shard, true, "Whether to fallback if there is any exception", 0) \
    M(Bool, optimize_skip_unused_shards, false, "Assumes that data is distributed by sharding_key. Optimization to skip unused shards if SELECT query filters by sharding_key.", 0) \
    M(Bool, optimize_skip_unused_shards_rewrite_in, true, "Rewrite IN in query for remote shards to exclude values that does not belong to the shard (requires optimize_skip_unused_shards)", 0) \
    M(Bool, allow_nondeterministic_optimize_skip_unused_shards, false, "Allow non-deterministic functions (includes dictGet) in sharding_key for optimize_skip_unused_shards", 0) \
    M(UInt64, force_optimize_skip_unused_shards, 0, "Throw an exception if unused shards cannot be skipped (1 - throw only if the table has the sharding key, 2 - always throw.", 0) \
    M(UInt64, optimize_skip_unused_shards_nesting, 0, "Same as optimize_skip_unused_shards, but accept nesting level until which it will work.", 0) \
    M(UInt64, force_optimize_skip_unused_shards_nesting, 0, "Same as force_optimize_skip_unused_shards, but accept nesting level until which it will work.", 0) \
    \
    M(Bool, use_sync_pipeline_executor, false, "Whether to use sync pipeline executor", 0) \
    \
    M(Bool, input_format_parallel_parsing, true, "Enable parallel parsing for some data formats.", 0) \
    M(UInt64, \
      min_chunk_bytes_for_parallel_parsing, \
      (10 * 1024 * 1024), \
      "The minimum chunk size in bytes, which each thread will parse in parallel.", \
      0) \
    M(Bool, output_format_parallel_formatting, true, "Enable parallel formatting for some data formats.", 0) \
    \
    M(String, local_filesystem_read_method, "pread", "Method of reading data from local filesystem, one of: read, pread, mmap, io_uring, pread_threadpool. The 'io_uring' method is experimental and does not work for Log, TinyLog, StripeLog, File, Set and Join, and other tables with append-able files in presence of concurrent reads and writes.", 0) \
    M(String, remote_filesystem_read_method, "threadpool", "Method of reading data from remote filesystem, one of: read, threadpool.", 0) \
    M(Bool, local_filesystem_read_prefetch, false, "Should use prefetching when reading data from local filesystem.", 0) \
    M(Bool, remote_filesystem_read_prefetch, true, "Should use prefetching when reading data from remote filesystem.", 0) \
    M(Bool, enable_remote_read_log, false, "Should write detailed trace log when reading data from remote filesystem.", 0) \
    M(UInt64, merge_tree_min_rows_for_concurrent_read, (20 * 8192), "If at least as many lines are read from one file, the reading can be parallelized.", 0) \
    M(UInt64, merge_tree_min_bytes_for_concurrent_read, (24 * 10 * 1024 * 1024), "If at least as many bytes are read from one file, the reading can be parallelized.", 0) \
    M(UInt64, merge_tree_min_rows_for_seek, 0, "You can skip reading more than that number of rows at the price of one seek per file.", 0) \
    M(UInt64, merge_tree_min_bytes_for_seek, 0, "You can skip reading more than that number of bytes at the price of one seek per file.", 0) \
    M(UInt64, remote_read_min_bytes_for_seek, 3 * DBMS_DEFAULT_BUFFER_SIZE, "Min bytes required for remote read (url, s3) to do seek, instead of read with ignore.", 0) \
    M(UInt64, merge_tree_coarse_index_granularity, 8, "If the index segment can contain the required keys, divide it into as many parts and recursively check them.", 0) \
    M(UInt64, merge_tree_max_rows_to_use_cache, (128 * 8192), "The maximum number of rows per request, to use the cache of uncompressed data. If the request is large, the cache is not used. (For large queries not to flush out the cache.)", 0) \
    M(UInt64, merge_tree_max_bytes_to_use_cache, (192 * 10 * 1024 * 1024), "The maximum number of bytes per request, to use the cache of uncompressed data. If the request is large, the cache is not used. (For large queries not to flush out the cache.)", 0) \
    M(UInt64, merge_tree_calculate_columns_size_sample, 1000, "The number of the sample parts to calculate columns size.", 0) \
    M(Bool, do_not_merge_across_partitions_select_final, false, "Merge parts only in one partition in select final", 0) \
    \
    M(UInt64, mysql_max_rows_to_insert, 65536, "The maximum number of rows in MySQL batch insertion of the MySQL storage engine", 0) \
    M(Bool, mysql_map_string_to_text_in_show_columns, false, "If enabled, String type will be mapped to TEXT in SHOW [FULL] COLUMNS, BLOB otherwise.", 0) \
    M(Bool, mysql_map_fixed_string_to_text_in_show_columns, false, "If enabled, FixedString type will be mapped to TEXT in SHOW [FULL] COLUMNS, BLOB otherwise.", 0) \
    \
    M(UInt64, optimize_min_equality_disjunction_chain_length, 3, "The minimum length of the expression `expr = x1 OR ... expr = xN` for optimization ", 0) \
    \
    M(UInt64, min_bytes_to_use_direct_io, 0, "The minimum number of bytes for reading the data with O_DIRECT option during SELECT queries execution. 0 - disabled.", 0) \
    M(UInt64, min_bytes_to_use_mmap_io, 0, "The minimum number of bytes for reading the data with mmap option during SELECT queries execution. 0 - disabled.", 0) \
    M(Bool, checksum_on_read, true, "Validate checksums on reading. It is enabled by default and should be always enabled in production. Please do not expect any benefits in disabling this setting. It may only be used for experiments and benchmarks. The setting only applicable for tables of MergeTree family. Checksums are always validated for other table engines and when receiving data over network.", 0) \
    \
    M(Bool, force_index_by_date, 0, "Throw an exception if there is a partition key in a table, and it is not used.", 0) \
    M(Bool, force_primary_key, 0, "Throw an exception if there is primary key in a table, and it is not used.", 0) \
    M(Bool, enable_skip_index, 1, "Whether enable to use skip index", 0) \
    M(String, force_data_skipping_indices, "", "Comma separated list of strings or literals with the name of the data skipping indices that should be used during query execution, otherwise an exception will be thrown.", 0) \
    \
    M(Float, max_streams_to_max_threads_ratio, 1, "Allows you to use more sources than the number of threads - to more evenly distribute work across threads. It is assumed that this is a temporary solution, since it will be possible in the future to make the number of sources equal to the number of threads, but for each source to dynamically select available work for itself.", 0) \
    M(Float, max_streams_multiplier_for_merge_tables, 5, "Ask more streams when reading from Merge table. Streams will be spread across tables that Merge table will use. This allows more even distribution of work across threads and especially helpful when merged tables differ in size.", 0) \
    \
    M(String, network_compression_method, "LZ4", "Allows you to select the method of data compression when writing.", 0) \
\
    M(Int64, network_zstd_compression_level, 1, "Allows you to select the level of ZSTD compression.", 0) \
\
    M(UInt64, priority, 0, "Priority of the query. 1 - the highest, higher value - lower priority; 0 - do not use priorities.", 0) \
    M(Int64, \
      os_thread_priority, \
      0, \
      "If non zero - set corresponding 'nice' value for query processing threads. Can be used to adjust query priority for OS scheduler.", \
      0) \
\
    M(Bool, log_queries, 1, "Log requests and write the log to the system table.", 0) \
    M(Bool, log_max_io_thread_queries, 1, "Log max io time thread requests and write the log to the system table", 0) \
    M(LogQueriesType, \
      log_queries_min_type, \
      QueryLogElementType::QUERY_START, \
      "Minimal type in query_log to log, possible values (from low to high): QUERY_START, QUERY_FINISH, EXCEPTION_BEFORE_START, " \
      "EXCEPTION_WHILE_PROCESSING.", \
      0) \
    M(Milliseconds, \
      log_queries_min_query_duration_ms, \
      0, \
      "Minimal time for the query to run, to get to the query_log/query_thread_log.", \
      0) \
    M(UInt64, \
      log_queries_cut_to_length, \
      100000, \
      "If query length is greater than specified threshold (in bytes), then cut query when writing to query log. Also limit length of " \
      "printed query in ordinary text log.", \
      0) \
    M(Bool, log_queries_with_partition_ids, 0, "Log requests partition ids and write the log to the system table.", 0) \
\
    M(Bool, log_processors_profiles, false, "Log Processors profile events.", 0) \
    M(Bool, log_segment_profiles, false, "Log profile of each segment info including runtime and planning information.", 0) \
    M(Bool, report_processors_profiles, false, "Report processors profile to coordinator.", 0) \
    M(UInt64, report_processors_profiles_timeout_millseconds, 10, "Report processors profile to coordinator timeout millseconds.", 0) \
    M(DistributedProductMode, \
      distributed_product_mode, \
      DistributedProductMode::DENY, \
      "How are distributed subqueries performed inside IN or JOIN sections?", \
      IMPORTANT) \
\
    M(UInt64, max_concurrent_queries_for_all_users, 0, "The maximum number of concurrent requests for all users.", 0) \
    M(UInt64, max_concurrent_queries_for_user, 0, "The maximum number of concurrent requests per user.", 0) \
\
    M(Bool, \
      insert_deduplicate, \
      true, \
      "For INSERT queries in the replicated table, specifies that deduplication of insertings blocks should be performed", \
      0) \
\
    M(UInt64, \
      insert_quorum, \
      0, \
      "For INSERT queries in the replicated table, wait writing for the specified number of replicas and linearize the addition of the " \
      "data. 0 - disabled.", \
      0) \
    M(Milliseconds, insert_quorum_timeout, 600000, "", 0) \
    M(Bool, insert_quorum_parallel, true, "For quorum INSERT queries - enable to make parallel inserts without linearizability", 0) \
    M(UInt64, select_sequential_consistency, 0, "For SELECT queries from the replicated table, throw an exception if the replica does not have a chunk written with the quorum; do not read the parts that have not yet been written with the quorum.", 0) \
    M(UInt64, table_function_remote_max_addresses, 1000, "The maximum number of different shards and the maximum number of replicas of one shard in the `remote` function.", 0) \
    M(Milliseconds, read_backoff_min_latency_ms, 1000, "Setting to reduce the number of threads in case of slow reads. Pay attention only to reads that took at least that much time.", 0) \
    M(UInt64, read_backoff_max_throughput, 1048576, "Settings to reduce the number of threads in case of slow reads. Count events when the read bandwidth is less than that many bytes per second.", 0) \
    M(Milliseconds, read_backoff_min_interval_between_events_ms, 1000, "Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time.", 0) \
    M(UInt64, read_backoff_min_events, 2, "Settings to reduce the number of threads in case of slow reads. The number of events after which the number of threads will be reduced.", 0) \
    \
    M(UInt64, read_backoff_min_concurrency, 1, "Settings to try keeping the minimal number of threads in case of slow reads.", 0) \
\
    M(Float, \
      memory_tracker_fault_probability, \
      0., \
      "For testing of `exception safety` - throw an exception every time you allocate memory with the specified probability.", \
      0) \
\
    M(Bool, \
      enable_http_compression, \
      0, \
      "Compress the result if the client over HTTP said that it understands data compressed by gzip or deflate.", \
      0) \
    M(Int64, \
      http_zlib_compression_level, \
      3, \
      "Compression level - used if the client on HTTP said that it understands data compressed by gzip or deflate.", \
      0) \
\
    M(Bool, \
      http_native_compression_disable_checksumming_on_decompress, \
      0, \
      "If you uncompress the POST data from the client compressed by the native format, do not check the checksum.", \
      0) \
\
    M(String, count_distinct_implementation, "uniqExact", "What aggregate function to use for implementation of count(DISTINCT ...)", 0) \
\
    M(Bool, add_http_cors_header, false, "Write add http CORS header.", 0) \
\
    M(UInt64, \
      max_http_get_redirects, \
      0, \
      "Max number of http GET redirects hops allowed. Make sure additional security measures are in place to prevent a malicious server " \
      "to redirect your requests to unexpected services.", \
      0) \
\
    M(Bool, \
      use_client_time_zone, \
      false, \
      "Use client timezone for interpreting DateTime string values, instead of adopting server timezone.", \
      0) \
\
    M(Bool, \
      send_progress_in_http_headers, \
      false, \
      "Send progress notifications using X-ClickHouse-Progress headers. Some clients do not support high amount of HTTP headers (Python " \
      "requests in particular), so it is disabled by default.", \
      0) \
\
    M(UInt64, \
      http_headers_progress_interval_ms, \
      100, \
      "Do not send HTTP headers X-ClickHouse-Progress more frequently than at each specified interval.", \
      0) \
\
    M(Bool, \
      fsync_metadata, \
      1, \
      "Do fsync after changing metadata for tables and databases (.sql files). Could be disabled in case of poor latency on server with " \
      "high load of DDL queries and high load of disk subsystem.", \
      0) \
\
    M(Bool, \
      join_use_nulls, \
      1, \
      "Use NULLs for non-joined rows of outer JOINs for types that can be inside Nullable. If false, use default value of corresponding " \
      "columns data type.", \
      IMPORTANT) \
    M(Bool, join_using_null_safe, 0, "Force null safe equal comparison for USING keys except the last key of ASOF join", 0) \
    \
    M(JoinStrictness, join_default_strictness, JoinStrictness::ALL, "Set default strictness in JOIN query. Possible values: empty string, 'ANY', 'ALL'. If empty, query without strictness will throw exception.", 0) \
    M(Bool, any_join_distinct_right_table_keys, false, "Enable old ANY JOIN logic with many-to-one left-to-right table keys mapping for all ANY JOINs. It leads to confusing not equal results for 't1 ANY LEFT JOIN t2' and 't2 ANY RIGHT JOIN t1'. ANY RIGHT JOIN needs one-to-many keys mapping to be consistent with LEFT one.", IMPORTANT) \
    M(Bool, enable_join_on_1_equals_1, false, "Enable join on 1=1.", 0) \
    \
    M(UInt64, preferred_block_size_bytes, 1000000, "", 0) \
\
    M(UInt64, \
      max_replica_delay_for_distributed_queries, \
      300, \
      "If set, distributed queries of Replicated tables will choose servers with replication delay in seconds less than the specified " \
      "value (not inclusive). Zero means do not take delay into account.", \
      0) \
    M(Bool, \
      fallback_to_stale_replicas_for_distributed_queries, \
      1, \
      "Suppose max_replica_delay_for_distributed_queries is set and all replicas for the queried table are stale. If this setting is " \
      "enabled, the query will be performed anyway, otherwise the error will be reported.", \
      0) \
    M(UInt64, \
      preferred_max_column_in_block_size_bytes, \
      0, \
      "Limit on max column size in block while reading. Helps to decrease cache misses count. Should be close to L2 cache size.", \
      0) \
\
    M(Bool, \
      insert_distributed_sync, \
      false, \
      "If setting is enabled, insert query into distributed waits until data will be sent to all nodes in cluster.", \
      0) \
    M(UInt64, \
      insert_distributed_timeout, \
      0, \
      "Timeout for insert query into distributed. Setting is used only with insert_distributed_sync enabled. Zero value means no " \
      "timeout.", \
      0) \
    M(Int64, \
      distributed_ddl_task_timeout, \
      180, \
      "Timeout for DDL query responses from all hosts in cluster. If a ddl request has not been performed on all hosts, a response will " \
      "contain a timeout error and a request will be executed in an async mode. Negative value means infinite. Zero means async mode.", \
      0) \
    M(Milliseconds, stream_flush_interval_ms, 7500, "Timeout for flushing data from streaming storages.", 0) \
    M(Milliseconds, stream_poll_timeout_ms, 500, "Timeout for polling data from/to streaming storages.", 0) \
\
    /** Settings for testing hedged requests */ \
    M(Milliseconds, sleep_in_send_tables_status_ms, 0, "Time to sleep in sending tables status response in TCPHandler", 0) \
    M(Milliseconds, sleep_in_send_data_ms, 0, "Time to sleep in sending data in TCPHandler", 0) \
    M(UInt64, unknown_packet_in_send_data, 0, "Send unknown packet instead of data Nth data packet", 0) \
\
    M(Bool, insert_allow_materialized_columns, 0, "If setting is enabled, Allow materialized columns in INSERT.", 0) \
    M(Seconds, http_connection_timeout, DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT, "HTTP connection timeout.", 0) \
    M(Seconds, http_send_timeout, DEFAULT_HTTP_READ_BUFFER_TIMEOUT, "HTTP send timeout", 0) \
    M(Seconds, http_receive_timeout, DEFAULT_HTTP_READ_BUFFER_TIMEOUT, "HTTP receive timeout", 0) \
    M(UInt64, http_max_uri_size, 1048576, "Maximum URI length of HTTP request", 0) \
    M(UInt64, http_max_fields, 1000000, "Maximum number of fields in HTTP header", 0) \
    M(UInt64, http_max_field_name_size, 1048576, "Maximum length of field name in HTTP header", 0) \
    M(UInt64, http_max_field_value_size, 1048576, "Maximum length of field value in HTTP header", 0) \
    M(Bool, \
      optimize_throw_if_noop, \
      false, \
      "If setting is enabled and OPTIMIZE query didn't actually assign a merge then an explanatory exception is thrown", \
      0) \
    M(Bool, \
      use_index_for_in_with_subqueries, \
      true, \
      "Try using an index if there is a subquery or a table expression on the right side of the IN operator.", \
      0) \
    M(Bool, \
      joined_subquery_requires_alias, \
      false, \
      "Force joined subqueries and table functions to have aliases for correct name qualification.", \
      0) \
    M(Bool, empty_result_for_aggregation_by_empty_set, false, "Return empty result when aggregating without keys on empty set.", 0) \
    M(Bool, allow_distributed_ddl, true, "If it is set to true, then a user is allowed to executed distributed DDL queries.", 0) \
    M(Bool, allow_suspicious_codecs, false, "If it is set to true, allow to specify meaningless compression codecs.", 0) \
    M(Bool, \
      allow_experimental_codecs, \
      false, \
      "If it is set to true, allow to specify experimental compression codecs (but we don't have those yet and this option does " \
      "nothing).", \
      0) \
    M(UInt64, odbc_max_field_size, 1024, "Max size of filed can be read from ODBC dictionary. Long strings are truncated.", 0) \
    M(UInt64, \
      query_profiler_real_time_period_ns, \
      1000000000, \
      "Period for real clock timer of query profiler (in nanoseconds). Set 0 value to turn off the real clock query profiler. " \
      "Recommended value is at least 10000000 (100 times a second) for single queries or 1000000000 (once a second) for cluster-wide " \
      "profiling.", \
      0) \
    M(UInt64, \
      query_profiler_cpu_time_period_ns, \
      1000000000, \
      "Period for CPU clock timer of query profiler (in nanoseconds). Set 0 value to turn off the CPU clock query profiler. Recommended " \
      "value is at least 10000000 (100 times a second) for single queries or 1000000000 (once a second) for cluster-wide profiling.", \
      0) \
    M(Bool, metrics_perf_events_enabled, false, "If enabled, some of the perf events will be measured throughout queries' execution.", 0) \
    M(Bool, enable_os_task_stats, true, "If enabled, collect additional per-thread OS metrics (like OSCPUWaitMicroseconds/OSIOWaitMicroseconds) throughout queries' execution.", 0) \
    M(String, metrics_perf_events_list, "", "Comma separated list of perf metrics that will be measured throughout queries' execution. Empty means all events. See PerfEventInfo in sources for the available events.", 0) \
    M(Float, opentelemetry_start_trace_probability, 0., "Probability to start an OpenTelemetry trace for an incoming query.", 0) \
    M(Bool, prefer_column_name_to_alias, false, "Prefer using column names instead of aliases if possible.", 0) \
    M(Bool, \
      prefer_global_in_and_join, \
      false, \
      "If enabled, all IN/JOIN operators will be rewritten as GLOBAL IN/JOIN. It's useful when the to-be-joined tables are only " \
      "available on the initiator and we need to always scatter their data on-the-fly during distributed processing with the GLOBAL " \
      "keyword. It's also useful to reduce the need to access the external sources joining external tables.", \
      0) \
    M(UInt64, connection_check_pool_size, 16, "Number of thread for connection check", 0) \
    M(Bool, \
      query_worker_fault_tolerance, \
      false, \
      "Whether to retry when worker failures are detected when allocating metadata during query execution.", \
      0) \
    M(Bool, enable_partition_prune, true, "prune partition based on where expression analysis.", 0) \
    M(Bool, restore_table_expression_in_distributed, 1, "restore table expressions in distributed query to pass current database to remote query.", 0) \
    \
    /**  settings about bitmap index */\
    M(Bool, enable_ab_index_optimization, true, "Optimize ab version by reading Bitmap", 0)\
    M(Bool, enable_sync_build_bitmap, false, "Build bitmap index in sync mode", 0)\
    M(Bool, enable_async_build_bitmap_in_attach, false, "Async build bitmap index in attach, it is a user config", 0)\
    M(Bool, throw_exception_when_has_null, true, "Whether throw an exception when there has null value in arraySetCheck", 0)\
    \
    /**  settings about segment bitmap index */\
    M(Bool, enable_sync_build_segment_bitmap, false, "Build  segment bitmap index in sync mode", 0)\
    M(Bool, enable_async_build_segment_bitmap_in_attach, false, "Async build segment bitmap index in attach, it is a user config", 0)\
    \
    /** Limits during query execution are part of the settings. \
      * Used to provide a more safe execution of queries from the user interface. \
      * Basically, limits are checked for each block (not every row). That is, the limits can be slightly violated. \
      * Almost all limits apply only to SELECTs. \
      * Almost all limits apply to each stream individually. \
      */ \
\
    M(UInt64, \
      max_rows_to_read, \
      0, \
      "Limit on read rows from the most 'deep' sources. That is, only in the deepest subquery. When reading from a remote server, it is " \
      "only checked on a remote server.", \
      0) \
    M(UInt64, \
      max_bytes_to_read, \
      0, \
      "Limit on read bytes (after decompression) from the most 'deep' sources. That is, only in the deepest subquery. When reading from " \
      "a remote server, it is only checked on a remote server.", \
      0) \
    M(OverflowMode, read_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
\
    M(UInt64, \
      max_rows_to_read_leaf, \
      0, \
      "Limit on read rows on the leaf nodes for distributed queries. Limit is applied for local reads only excluding the final merge " \
      "stage on the root node.", \
      0) \
    M(UInt64, \
      max_bytes_to_read_leaf, \
      0, \
      "Limit on read bytes (after decompression) on the leaf nodes for distributed queries. Limit is applied for local reads only " \
      "excluding the final merge stage on the root node.", \
      0) \
    M(OverflowMode, read_overflow_mode_leaf, OverflowMode::THROW, "What to do when the leaf limit is exceeded.", 0) \
    /** Just for compatible, totally the same with settings above */ \
    M(UInt64, max_rows_to_read_local, 0, "Limit max reading rows for each local shard. The same ", 0) \
    M(UInt64, max_bytes_to_read_local, 0, "Limit max reading bytes for each local shard.", 0) \
    M(OverflowMode, read_overflow_mode_local, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    \
    /** Just for compatible, maybe removed or implemented later */ \
    M(Bool, group_array_can_return_nullable, true, "Force return type of groupArray is nullable.", 0) \
    M(Bool, allow_experimental_multiple_joins_emulation, true, "Emulate multiple joins using subselects", 0) \
    M(Bool, allow_experimental_data_skipping_indices, true, "Emulate data skipping indices", 0) \
    M(Bool, enable_predicate_pushdown, false, "Where to push down predicate", 0) \
    M(Bool, dict_table_full_mode, false, "If encode / decode table is not bucket table, try to dispatch dict to all workers, if false, throw exception instead", 0) \
    M(UInt64, pathgraph_threshold_y, 0, "maximum point number in each level", 0) \
    M(Bool, to_string_extra_arguments, true, "Whether to allow an extra argument in toString Function", 0) \
    \
    M(UInt64, max_query_cpu_seconds, 0, "Limit the maximum amount of CPU resources such a query segment can consume.", 0) \
    M(UInt64, max_distributed_query_cpu_seconds, 0, "Limit the maximum amount of CPU resources such a distribute query can consume.", 0) \
\
    M(UInt64, max_rows_to_group_by, 0, "", 0) \
    M(OverflowModeGroupBy, group_by_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    M(UInt64, max_bytes_before_external_group_by, 0, "", 0) \
    M(Bool, enable_lc_group_by_opt, false, "Whether enable single lowcardinality column group by optimize", 0) \
    M(UInt64, spill_buffer_bytes_before_external_group_by, 10485760, "Agg memory buffer threshold when the spill trigger condition is reached, default 10Mb", 0) \
    \
    M(UInt64, max_rows_to_sort, 0, "", 0) \
    M(UInt64, max_bytes_to_sort, 0, "", 0) \
    M(OverflowMode, sort_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    M(UInt64, max_bytes_before_external_sort, 0, "Max bytes before external sort", 0) \
    M(UInt64, external_sort_max_block_size, DEFAULT_BLOCK_SIZE, "Maximum block size for external sort", 0) \
    M(UInt64, max_bytes_before_remerge_sort, 1000000000, "In case of ORDER BY with LIMIT, when memory usage is higher than specified threshold, perform additional steps of merging blocks before final merge to keep just top LIMIT rows.", 0) \
    M(Float, remerge_sort_lowered_memory_bytes_ratio, 2., "If memory usage after remerge does not reduced by this ratio, remerge will be disabled.", 0) \
    \
    M(UInt64, max_result_rows, 0, "Limit on result size in rows. Also checked for intermediate data sent from remote servers.", 0) \
    M(UInt64, \
      max_result_bytes, \
      0, \
      "Limit on result size in bytes (uncompressed). Also checked for intermediate data sent from remote servers.", \
      0) \
    M(OverflowMode, result_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
\
    /* TODO: Check also when merging and finalizing aggregate functions. */ \
    M(Seconds, max_execution_time, 0, "", 0) \
    M(OverflowMode, timeout_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
\
    M(UInt64, min_execution_speed, 0, "Minimum number of execution rows per second.", 0) \
    M(UInt64, max_execution_speed, 0, "Maximum number of execution rows per second.", 0) \
    M(UInt64, min_execution_speed_bytes, 0, "Minimum number of execution bytes per second.", 0) \
    M(UInt64, max_execution_speed_bytes, 0, "Maximum number of execution bytes per second.", 0) \
    M(Seconds, \
      timeout_before_checking_execution_speed, \
      10, \
      "Check that the speed is not too low after the specified time has elapsed.", \
      0) \
\
    M(UInt64, max_columns_to_read, 0, "", 0) \
    M(UInt64, max_temporary_columns, 0, "", 0) \
    M(UInt64, max_temporary_non_const_columns, 0, "", 0) \
\
    M(UInt64, max_subquery_depth, 100, "", 0) \
    M(UInt64, max_pipeline_depth, 1000, "", 0) \
    M(UInt64, max_ast_depth, 1000, "Maximum depth of query syntax tree. Checked after parsing.", 0) \
    M(UInt64, max_ast_elements, 50000, "Maximum size of query syntax tree in number of nodes. Checked after parsing.", 0) \
    M(UInt64, \
      max_expanded_ast_elements, \
      500000, \
      "Maximum size of query syntax tree in number of nodes after expansion of aliases and the asterisk.", \
      0) \
\
    M(UInt64, \
      readonly, \
      0, \
      "0 - everything is allowed. 1 - only read requests. 2 - only read requests, as well as changing settings, except for the " \
      "'readonly' setting.", \
      0) \
\
    M(UInt64, max_rows_in_set, 0, "Maximum size of the set (in number of elements) resulting from the execution of the IN section.", 0) \
    M(UInt64, max_bytes_in_set, 0, "Maximum size of the set (in bytes in memory) resulting from the execution of the IN section.", 0) \
    M(OverflowMode, set_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
\
    M(UInt64, max_rows_in_join, 0, "Maximum size of the hash table for JOIN (in number of rows).", 0) \
    M(UInt64, max_bytes_in_join, 0, "Maximum size of the hash table for JOIN (in number of bytes in memory).", 0) \
    M(OverflowMode, join_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    M(Bool, \
      join_any_take_last_row, \
      false, \
      "When disabled (default) ANY JOIN will take the first found row for a key. When enabled, it will take the last row seen if there " \
      "are multiple rows for the same key.", \
      IMPORTANT) \
    M(JoinAlgorithm, \
      join_algorithm, \
      JoinAlgorithm::HASH, \
      "Specify join algorithm: 'auto', 'hash', 'partial_merge', 'prefer_partial_merge', 'parallel_hash'. 'auto' tries to change HashJoin " \
      "to MergeJoin on the fly to avoid out of memory.", \
      0) \
    M(Bool, join_parallel_left_right, true, "Enable joinTransform parallel for left input and right input", 0) \
    M(Bool, partial_merge_join_optimizations, true, "Enable optimizations in partial merge join", 0) \
    M(UInt64, \
      default_max_bytes_in_join, \
      1000000000, \
      "Maximum size of right-side table if limit is required but max_bytes_in_join is not set.", \
      0) \
    M(UInt64, \
      partial_merge_join_left_table_buffer_bytes, \
      32000000, \
      "If not 0 group left table blocks in bigger ones for left-side table in partial merge join. It uses up to 2x of specified memory " \
      "per joining thread. In current version work only with 'partial_merge_join_optimizations = 1'.", \
      0) \
    M(UInt64, \
      partial_merge_join_rows_in_right_blocks, \
      65536, \
      "Split right-hand joining data in blocks of specified size. It's a portion of data indexed by min-max values and possibly unloaded " \
      "on disk.", \
      0) \
    M(UInt64, \
      join_on_disk_max_files_to_merge, \
      64, \
      "For MergeJoin on disk set how much files it's allowed to sort simultaneously. Then this value bigger then more memory used and " \
      "then less disk I/O needed. Minimum is 2.", \
      0) \
    M(String, temporary_files_codec, "LZ4", "Set compression codec for temporary files (sort and join on disk). I.e. LZ4, NONE.", 0) \
\
    M(UInt64, \
      max_rows_to_transfer, \
      0, \
      "Maximum size (in rows) of the transmitted external table obtained when the GLOBAL IN/JOIN section is executed.", \
      0) \
    M(UInt64, \
      max_bytes_to_transfer, \
      0, \
      "Maximum size (in uncompressed bytes) of the transmitted external table obtained when the GLOBAL IN/JOIN section is executed.", \
      0) \
    M(OverflowMode, transfer_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
\
    M(UInt64, max_rows_in_distinct, 0, "Maximum number of elements during execution of DISTINCT.", 0) \
    M(UInt64, max_bytes_in_distinct, 0, "Maximum total size of state (in uncompressed bytes) in memory for the execution of DISTINCT.", 0) \
    M(OverflowMode, distinct_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
\
    M(UInt64, max_memory_usage, 0, "Maximum memory usage for processing of single query. Zero means unlimited.", 0) \
    M(UInt64, \
      max_memory_usage_for_user, \
      0, \
      "Maximum memory usage for processing all concurrently running queries for the user. Zero means unlimited.", \
      0) \
    M(UInt64, \
      max_untracked_memory, \
      (4 * 1024 * 1024), \
      "Small allocations and deallocations are grouped in thread local variable and tracked or profiled only when amount (in absolute " \
      "value) becomes larger than specified value. If the value is higher than 'memory_profiler_step' it will be effectively lowered to " \
      "'memory_profiler_step'.", \
      0) \
    M(UInt64, \
      memory_profiler_step, \
      (4 * 1024 * 1024), \
      "Whenever query memory usage becomes larger than every next step in number of bytes the memory profiler will collect the " \
      "allocating stack trace. Zero means disabled memory profiler. Values lower than a few megabytes will slow down query processing.", \
      0) \
    M(Float, \
      memory_profiler_sample_probability, \
      0., \
      "Collect random allocations and deallocations and write them into system.trace_log with 'MemorySample' trace_type. The probability " \
      "is for every alloc/free regardless to the size of the allocation. Note that sampling happens only when the amount of untracked " \
      "memory exceeds 'max_untracked_memory'. You may want to set 'max_untracked_memory' to 0 for extra fine grained sampling.", \
      0) \
\
    M(UInt64, \
      max_network_bandwidth, \
      0, \
      "The maximum speed of data exchange over the network in bytes per second for a query. Zero means unlimited.", \
      0) \
    M(UInt64, \
      max_network_bytes, \
      0, \
      "The maximum number of bytes (compressed) to receive or transmit over the network for execution of the query.", \
      0) \
    M(UInt64, \
      max_network_bandwidth_for_user, \
      0, \
      "The maximum speed of data exchange over the network in bytes per second for all concurrently running user queries. Zero means " \
      "unlimited.", \
      0) \
    M(UInt64, \
      max_network_bandwidth_for_all_users, \
      0, \
      "The maximum speed of data exchange over the network in bytes per second for all concurrently running queries. Zero means " \
      "unlimited.", \
      0) \
\
    M(Bool, log_profile_events, true, "Log query performance statistics into the query_log and query_thread_log.", 0) \
    M(Bool, log_query_settings, true, "Log query settings into the query_log.", 0) \
    M(Bool, log_query_threads, true, "Log query threads into system.query_thread_log table. This setting have effect only when 'log_queries' is true.", 0) \
    M(Bool, log_query_exchange, false, "Log query exchange metric.", 0) \
    M(String, log_comment, "", "Log comment into system.query_log table and server log. It can be set to arbitrary string no longer than max_query_size.", 0) \
    M(LogsLevel, send_logs_level, LogsLevel::fatal, "Send server text logs with specified minimum level to client. Valid values: 'trace', 'debug', 'information', 'warning', 'error', 'fatal', 'none'", 0) \
    M(Bool, enable_optimize_predicate_expression, 1, "If it is set to true, optimize predicates to subqueries.", 0) \
    M(Bool, enable_optimize_predicate_expression_to_final_subquery, 1, "Allow push predicate to final subquery.", 0) \
    M(Bool, allow_push_predicate_when_subquery_contains_with, 1, "Allows push predicate when subquery contains WITH clause", 0) \
\
    M(UInt64, low_cardinality_max_dictionary_size, 8192, "Maximum size (in rows) of shared global dictionary for LowCardinality type.", 0) \
    M(Bool, \
      low_cardinality_use_single_dictionary_for_part, \
      false, \
      "LowCardinality type serialization setting. If is true, than will use additional keys when global dictionary overflows. Otherwise, " \
      "will create several shared dictionaries.", \
      0) \
    M(Bool, decimal_check_overflow, true, "Check overflow of decimal arithmetic/comparison operations", 0) \
\
    M(Bool, \
      prefer_localhost_replica, \
      1, \
      "1 - always send query to local replica, if it exists. 0 - choose replica to send query between local and remote ones according to " \
      "load_balancing", \
      0) \
    M(UInt64, max_fetch_partition_retries_count, 5, "Amount of retries while fetching partition from another host.", 0) \
    M(UInt64, \
      http_max_multipart_form_data_size, \
      1024 * 1024 * 1024, \
      "Limit on size of multipart/form-data content. This setting cannot be parsed from URL parameters and should be set in user " \
      "profile. Note that content is parsed and external tables are created in memory before start of query execution. And this is the " \
      "only limit that has effect on that stage (limits on max memory usage and max execution time have no effect while reading HTTP " \
      "form data).", \
      0) \
    M(Bool, \
      calculate_text_stack_trace, \
      1, \
      "Calculate text stack trace in case of exceptions during query execution. This is the default. It requires symbol lookups that may " \
      "slow down fuzzing tests when huge amount of wrong queries are executed. In normal cases you should not disable this option.", \
      0) \
    M(Bool, allow_ddl, true, "If it is set to true, then a user is allowed to executed DDL queries.", 0) \
    M(Bool, parallel_view_processing, false, "Enables pushing to attached views concurrently instead of sequentially.", 0) \
    M(Bool, \
      enable_unaligned_array_join, \
      false, \
      "Allow ARRAY JOIN with multiple arrays that have different sizes. When this settings is enabled, arrays will be resized to the " \
      "longest one.", \
      0) \
    M(Bool, optimize_read_in_order, true, "Enable ORDER BY optimization for reading data in corresponding order in MergeTree tables.", 0) \
    M(Bool, optimize_aggregation_in_order, false, "Enable GROUP BY optimization for aggregating data in corresponding order in MergeTree tables.", 0) \
    M(UInt64, read_in_order_two_level_merge_threshold, 100, "Minimal number of parts to read to run preliminary merge step during multithread reading in order of primary key.", 0) \
    M(Bool, low_cardinality_allow_in_native_format, true, "Use LowCardinality type in Native format. Otherwise, convert LowCardinality columns to ordinary for select query, and convert ordinary columns to required LowCardinality for insert query.", 0) \
    M(Bool, cancel_http_readonly_queries_on_client_close, false, "Cancel HTTP readonly queries when a client closes the connection without waiting for response.", 0) \
    M(Bool, external_table_functions_use_nulls, true, "If it is set to true, external table functions will implicitly use Nullable type if needed. Otherwise NULLs will be substituted with default values. Currently supported only by 'mysql', 'postgresql' and 'odbc' table functions.", 0) \
    \
    M(Bool, allow_hyperscan, true, "Allow functions that use Hyperscan library. Disable to avoid potentially long compilation times and excessive resource usage.", 0) \
    M(UInt64, max_hyperscan_regexp_length, 0, "Max length of regexp than can be used in hyperscan multi-match functions. Zero means unlimited.", 0) \
    M(UInt64, max_hyperscan_regexp_total_length, 0, "Max total length of all regexps than can be used in hyperscan multi-match functions (per every function). Zero means unlimited.", 0) \
    M(Bool, reject_expensive_hyperscan_regexps, true, "Reject patterns which will likely be expensive to evaluate with hyperscan (due to NFA state explosion)", 0) \
    M(Bool, allow_simdjson, true, "Allow using simdjson library in 'JSON*' functions if AVX2 instructions are available. If disabled rapidjson will be used.", 0) \
    M(Bool, allow_introspection_functions, false, "Allow functions for introspection of ELF and DWARF for query profiling. These functions are slow and may impose security considerations.", 0) \
    \
    M(UInt64, max_partitions_per_insert_block, 100, "Limit maximum number of partitions in single INSERTed block. Zero means unlimited. Throw exception if the block contains too many partitions. This setting is a safety threshold, because using large number of partitions is a common misconception.", 0) \
    M(Int64, max_partitions_to_read, -1, "Limit the max number of partitions that can be accessed in one query. <= 0 means unlimited.", 0) \
    M(Bool, check_query_single_value_result, false, "Return check query result as single 1/0 value", 0) \
    M(Bool, allow_drop_detached, false, "Allow ALTER TABLE ... DROP DETACHED PART[ITION] ... queries", 0) \
\
    M(UInt64, postgresql_connection_pool_size, 16, "Connection pool size for PostgreSQL table engine and database engine.", 0) \
    M(UInt64, \
      postgresql_connection_pool_wait_timeout, \
      5000, \
      "Connection pool push/pop timeout on empty pool for PostgreSQL table engine and database engine. By default it will block on empty " \
      "pool.", \
      0) \
    M(UInt64, glob_expansion_max_elements, 1000, "Maximum number of allowed addresses (For external storages, table functions, etc).", 0) \
    M(UInt64, odbc_bridge_connection_pool_size, 16, "Connection pool size for each connection settings string in ODBC bridge.", 0) \
\
    M(Seconds, \
      distributed_replica_error_half_life, \
      DBMS_CONNECTION_POOL_WITH_FAILOVER_DEFAULT_DECREASE_ERROR_PERIOD, \
      "Time period reduces replica error counter by 2 times.", \
      0) \
    M(UInt64, \
      distributed_replica_error_cap, \
      DBMS_CONNECTION_POOL_WITH_FAILOVER_MAX_ERROR_COUNT, \
      "Max number of errors per replica, prevents piling up an incredible amount of errors if replica was offline for some time and " \
      "allows it to be reconsidered in a shorter amount of time.", \
      0) \
    M(UInt64, distributed_replica_max_ignored_errors, 0, "Number of errors that will be ignored while choosing replicas", 0) \
\
    M(Bool, allow_experimental_live_view, false, "Enable LIVE VIEW. Not mature enough.", 0) \
    M(Seconds, \
      live_view_heartbeat_interval, \
      DEFAULT_LIVE_VIEW_HEARTBEAT_INTERVAL_SEC, \
      "The heartbeat interval in seconds to indicate live query is alive.", \
      0) \
    M(UInt64, \
      max_live_view_insert_blocks_before_refresh, \
      64, \
      "Limit maximum number of inserted blocks after which mergeable blocks are dropped and query is re-executed.", \
      0) \
    M(UInt64, \
      min_free_disk_space_for_temporary_data, \
      0, \
      "The minimum disk space to keep while writing temporary data used in external sorting and aggregation.", \
      0) \
\
    M(DefaultDatabaseEngine, default_database_engine, DefaultDatabaseEngine::Cnch, "Default database engine.", 0) \
    M(Bool, debug_cnch_remain_temp_part, false, "Debug mode: remain temp (inserted, merged, altered) part", 0) \
    M(Bool, debug_cnch_force_commit_parts_rpc, false, "Debug mode: force to commit parts by RPC", 0) \
    M(Bool, \
      show_table_uuid_in_table_create_query_if_not_nil, \
      false, \
      "For tables in databases with Engine=Atomic show UUID of the table in its CREATE query.", \
      0) \
    M(UInt64, max_threads_for_cnch_dump, 1, "The maximum number of threads for dumping data in cnch.", 0) \
    M(Bool, database_atomic_wait_for_drop_and_detach_synchronously, false, "When executing DROP or DETACH TABLE in Atomic database, wait for table data to be finally dropped or detached.", 0) \
    M(Bool, enable_scalar_subquery_optimization, true, "If it is set to true, prevent scalar subqueries from (de)serializing large scalar values and possibly avoid running the same subquery more than once.", 0) \
    M(Bool, optimize_trivial_count_query, false, "Process trivial 'SELECT count() FROM table' query from metadata.", 0) \
    M(Bool, optimize_respect_aliases, true, "If it is set to true, it will respect aliases in WHERE/GROUP BY/ORDER BY, that will help with partition pruning/secondary indexes/optimize_aggregation_in_order/optimize_read_in_order/optimize_trivial_count", 0) \
    M(UInt64, mutations_sync, 0, "Wait for synchronous execution of ALTER TABLE UPDATE/DELETE queries (mutations). 0 - execute asynchronously. 1 - wait current server. 2 - wait all replicas if they exist.", 0) \
    M(UInt64, mutations_wait_timeout, 0, "Maximum seconds to wait for synchronous mutations. 0 - wait unlimited time", 0) \
    M(String, mutation_query_id, "", "Used to overwrite mutation's query id in tests", 0) \
    M(Bool, system_mutations_only_basic_info, false, "Only return basic information that stored in KV. It avoid acquiring merge thread of tables", 0) \
    M(Bool, enable_lightweight_delete, true, "Enable lightweight DELETE for mergetree tables.", 0) \
    M(Bool, optimize_move_functions_out_of_any, false, "Move functions out of aggregate functions 'any', 'anyLast'.", 0) \
    M(Bool, optimize_normalize_count_variants, true, "Rewrite aggregate functions that semantically equals to count() as count().", 0) \
    M(Bool, optimize_injective_functions_inside_uniq, true, "Delete injective functions of one argument inside uniq*() functions.", 0) \
    M(Bool, optimize_arithmetic_operations_in_aggregate_functions, true, "Move arithmetic operations out of aggregation functions", 0) \
    M(Bool, optimize_duplicate_order_by_and_distinct, true, "Remove duplicate ORDER BY and DISTINCT if it's possible", 0) \
    M(Bool, optimize_redundant_functions_in_order_by, true, "Remove functions from ORDER BY if its argument is also in ORDER BY", 0) \
    M(Bool, \
      optimize_if_chain_to_multiif, \
      false, \
      "Replace if(cond1, then1, if(cond2, ...)) chains to multiIf. Currently it's not beneficial for numeric types.", \
      0) \
    M(Bool, \
      optimize_if_transform_strings_to_enum, \
      false, \
      "Replaces string-type arguments in If and Transform to enum. Disabled by default cause it could make inconsistent change in " \
      "distributed query that would lead to its fail.", \
      0) \
    M(Bool, optimize_monotonous_functions_in_order_by, true, "Replace monotonous function with its argument in ORDER BY", 0) \
    M(Bool, \
      optimize_functions_to_subcolumns, \
      false, \
      "Transform functions to subcolumns, if possible, to reduce amount of read data. E.g. 'length(arr)' -> 'arr.size0', 'col IS NULL' " \
      "-> 'col.null' ", \
      0) \
    M(Bool, normalize_function_names, true, "Normalize function names to their canonical names", 0) \
    M(Bool, allow_experimental_alter_materialized_view_structure, false, "Allow atomic alter on Materialized views. Work in progress.", 0) \
    M(Bool, \
      enable_early_constant_folding, \
      true, \
      "Enable query optimization where we analyze function and subqueries results and rewrite query if there're constants there", \
      0) \
    M(Bool, \
      deduplicate_blocks_in_dependent_materialized_views, \
      false, \
      "Should deduplicate blocks for materialized views if the block is not a duplicate for the table. Use true to always deduplicate in " \
      "dependent tables.", \
      0) \
    M(Bool, \
      use_compact_format_in_distributed_parts_names, \
      true, \
      "Changes format of directories names for distributed table insert parts.", \
      0) \
    M(Bool, \
      validate_polygons, \
      true, \
      "Throw exception if polygon is invalid in function pointInPolygon (e.g. self-tangent, self-intersecting). If the setting is false, " \
      "the function will accept invalid polygons but may silently return wrong result.", \
      0) \
    M(UInt64, max_parser_depth, DBMS_DEFAULT_MAX_PARSER_DEPTH, "Maximum parser depth (recursion depth of recursive descend parser).", 0) \
    M(Seconds, \
      temporary_live_view_timeout, \
      DEFAULT_TEMPORARY_LIVE_VIEW_TIMEOUT_SEC, \
      "Timeout after which temporary live view is deleted.", \
      0) \
    M(Seconds, \
      periodic_live_view_refresh, \
      DEFAULT_PERIODIC_LIVE_VIEW_REFRESH_SEC, \
      "Interval after which periodically refreshed live view is forced to refresh.", \
      0) \
    M(Bool, transform_null_in, false, "If enabled, NULL values will be matched with 'IN' operator as if they are considered equal.", 0) \
    M(Bool, allow_nondeterministic_mutations, false, "Allow non-deterministic functions in ALTER UPDATE/ALTER DELETE statements", 0) \
    M(Seconds, lock_acquire_timeout, DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC, "How long locking request should wait before failing", 0) \
    M(Bool, materialize_ttl_after_modify, true, "Apply TTL for old data, after ALTER MODIFY TTL query", 0) \
    M(String, \
      function_implementation, \
      "", \
      "Choose function implementation for specific target or variant (experimental). If empty enable all of them.", \
      0) \
    M(Bool, allow_experimental_geo_types, false, "Allow geo data types such as Point, Ring, Polygon, MultiPolygon", 0) \
    M(Bool, data_type_default_nullable, false, "Data types without NULL or NOT NULL will make Nullable", 0) \
    M(Bool, cast_keep_nullable, false, "CAST operator keep Nullable for result data type", 0) \
    M(Bool, cast_ipv4_ipv6_default_on_conversion_error, false, "CAST operator into IPv4, CAST operator into IPV6 type, toIPv4, toIPv6 functions will return default value instead of throwing exception on conversion error.", 0) \
    M(Bool, alter_partition_verbose_result, false, "Output information about affected parts. Currently works only for FREEZE and ATTACH commands.", 0) \
    M(Bool, allow_experimental_database_materialize_mysql, true, "Allow to create database with Engine=CnchMaterializedMySQL(...).", 0) \
    M(Bool, allow_experimental_database_materialized_postgresql, false, "Allow to create database with Engine=MaterializedPostgreSQL(...).", 0) \
    M(Bool, system_events_show_zero_values, false, "Include all metrics, even with zero values", 0) \
    M(MySQLDataTypesSupport, \
      mysql_datatypes_support_level, \
      0, \
      "Which MySQL types should be converted to corresponding ClickHouse types (rather than being represented as String). Can be empty " \
      "or any combination of 'decimal' or 'datetime64'. When empty MySQL's DECIMAL and DATETIME/TIMESTAMP with non-zero precision are " \
      "seen as String on ClickHouse's side.", \
      0) \
    M(Bool, optimize_trivial_insert_select, true, "Optimize trivial 'INSERT INTO table SELECT ... FROM TABLES' query", 0) \
    M(Bool, allow_non_metadata_alters, true, "Allow to execute alters which affects not only tables metadata, but also data on disk", 0) \
    M(Bool, enable_global_with_statement, true, "Propagate WITH statements to UNION queries and all subqueries", 0) \
    M(Bool, aggregate_functions_null_for_empty, false, "Rewrite all aggregate functions in a query, adding -OrNull suffix to them", 0) \
    M(Bool, \
      optimize_fuse_sum_count_avg, \
      false, \
      "Fuse aggregate functions sum(), avg(), count() with identical arguments into one sumCount() call, if the query has at least two " \
      "different functions", \
      0) \
    M(Bool, \
      flatten_nested, \
      true, \
      "If true, columns of type Nested will be flatten to separate array columns instead of one array of tuples", \
      0) \
    M(Bool, asterisk_include_materialized_columns, false, "Include MATERIALIZED columns for wildcard query", 0) \
    M(Bool, asterisk_include_alias_columns, false, "Include ALIAS columns for wildcard query", 0) \
    M(Bool, optimize_skip_merged_partitions, false, "Skip partitions with one part with level > 0 in optimize final", 0) \
    M(Bool, optimize_on_insert, true, "Do the same transformation for inserted block of data as if merge was done on this block.", 0) \
    M(Bool, allow_experimental_object_type, false, "Allow Object and JSON data types", 0) \
    M(Bool, describe_extend_object_types, false, "Deduce concrete type of columns of type Object in DESCRIBE query", 0) \
    M(Bool, describe_include_subcolumns, false, "If true, subcolumns of all table columns will be included into result of DESCRIBE query", 0) \
    M(Bool, allow_experimental_map_type, true, "Obsolete setting, does nothing.", 0) \
    M(Bool, allow_experimental_window_functions, true, "Allow experimental window functions", 0) \
    M(Bool, allow_experimental_projection_optimization, false, "Enable projection optimization when processing SELECT queries", 0) \
    M(Bool, force_optimize_projection, false, "If projection optimization is enabled, SELECT queries need to use projection", 0) \
    M(Bool, async_socket_for_remote, false, "Asynchronously read from socket executing remote query", 0) \
    M(UInt64, resize_number_after_remote_source, 1, "Resize number after remote source, will be useful if shard is small", 0) \
    M(Bool, insert_null_as_default, true, "Insert DEFAULT values instead of NULL in INSERT SELECT (UNION ALL)", 0) \
\
    M(Bool, collect_hash_table_stats_during_aggregation, true, "Enable collecting hash table statistics to optimize memory allocation", 0) \
    M(UInt64, \
      max_entries_for_hash_table_stats, \
      10'000, \
      "How many entries hash table statistics collected during aggregation is allowed to have", \
      0) \
    M(UInt64, \
      max_size_to_preallocate_for_aggregation, \
      10'000'000, \
      "For how many elements it is allowed to preallocate space in all hash tables in total before aggregation", \
      0) \
\
    M(Bool, handle_division_by_zero, false, "If set true, return null for division by zero (MySQL Behavior)", 0) \
    \
    M(Bool, optimize_rewrite_sum_if_to_count_if, true, "Rewrite sumIf() and sum(if()) function countIf() function when logically equivalent", 0) \
    M(UInt64, insert_shard_id, 0, "If non zero, when insert into a distributed table, the data will be inserted into the shard `insert_shard_id` synchronously. Possible values range from 1 to `shards_number` of corresponding distributed table", 0) \
    M(Bool, ignore_array_join_check_in_join_on_condition, false, "Ignore array-join function check in join on condition", 0) \
    M(Bool, check_identifier_begin_valid, true, "Whether to check identifier", 0) \
\
    /** Experimental feature for moving data between shards. */ \
    \
    M(Bool, allow_experimental_nlp_functions, false, "Enable experimental functions for natural language processing.", 0) \
    M(Bool, allow_experimental_query_deduplication, false, "Experimental data deduplication for SELECT queries based on part UUIDs", 0) \
    M(Bool, \
      experimental_query_deduplication_send_all_part_uuids, \
      false, \
      "If false only part UUIDs for currently moving parts are sent. If true all read part UUIDs are sent (useful only for testing).", \
      0) \
\
    M(Bool, engine_file_empty_if_not_exists, false, "Allows to select data from a file engine table without file", 0) \
    M(Bool, engine_file_truncate_on_insert, false, "Enables or disables truncate before insert in file engine tables", 0) \
    M(Bool, allow_experimental_database_replicated, false, "Allow to create databases with Replicated engine", 0) \
    M(UInt64, \
      database_replicated_initial_query_timeout_sec, \
      300, \
      "How long initial DDL query should wait for Replicated database to precess previous DDL queue entries", \
      0) \
    M(UInt64, max_distributed_depth, 5, "Maximum distributed query depth", 0) \
    M(Bool, \
      database_replicated_always_detach_permanently, \
      false, \
      "Execute DETACH TABLE as DETACH TABLE PERMANENTLY if database engine is Replicated", \
      0) \
    M(DistributedDDLOutputMode, distributed_ddl_output_mode, DistributedDDLOutputMode::THROW, "Format of distributed DDL query result", 0) \
    M(UInt64, distributed_ddl_entry_format_version, 2, "Version of DDL entry to write into ZooKeeper", 0) \
    M(UInt64, external_storage_max_read_rows, 0, "Limit maximum number of rows when table with external engine should flush history data. Now supported only for MySQL table engine, database engine, dictionary and MaterializeMySQL. If equal to 0, this setting is disabled", 0) \
    M(UInt64, external_storage_max_read_bytes, 0, "Limit maximum number of bytes when table with external engine should flush history data. Now supported only for MySQL table engine, database engine, dictionary and MaterializeMySQL. If equal to 0, this setting is disabled", 0)  \
    M(SetOperationMode, union_default_mode, SetOperationMode::ALL, "Set default mode in UNION query. Possible values: empty string, 'ALL', 'DISTINCT'. If empty, query without mode will throw exception.", 0) \
    M(SetOperationMode, intersect_default_mode, SetOperationMode::ALL, "Set default mode in INTERSECT query. Possible values: empty string, 'ALL', 'DISTINCT'. If empty, query without mode will throw exception.", 0) \
    M(SetOperationMode, except_default_mode, SetOperationMode::ALL, "Set default mode in EXCEPT query. Possible values: empty string, 'ALL', 'DISTINCT'. If empty, query without mode will throw exception.", 0) \
    M(Bool, optimize_aggregators_of_group_by_keys, false, "Eliminates min/max/any/anyLast aggregators of GROUP BY keys in SELECT section", 0) \
    M(Bool, optimize_group_by_function_keys, true, "Eliminates functions of other keys in GROUP BY section", 0) \
    M(Bool, \
      legacy_column_name_of_tuple_literal, \
      false, \
      "List all names of element of large tuple literals in their column names instead of hash. This settings exists only for " \
      "compatibility reasons. It makes sense to set to 'true', while doing rolling update of cluster from version lower than 21.7 to " \
      "higher.", \
      0) \
\
    M(Bool, query_plan_enable_optimizations, true, "Apply optimizations to query plan", 0) \
    M(UInt64, \
      query_plan_max_optimizations_to_apply, \
      10000, \
      "Limit the total number of optimizations applied to query plan. If zero, ignored. If limit reached, throw exception", \
      0) \
    M(Bool, query_plan_filter_push_down, true, "Allow to push down filter by predicate query plan step", 0) \
    M(Bool, enable_partition_filter_push_down, false, "Allow to push down partition filter to query info", 0) \
    M(Bool, remove_partition_filter_on_worker, true, "Remove partition filter before worker execution, since partition pruning has been done on the server. This temp fix is used for not selecting partition key as prewhere", 0) \
    M(Bool, enable_optimizer_early_prewhere_push_down, false, "Allow to push down prewhere in the optimizer phase", 0) \
    M(UInt64, regexp_max_matches_per_row, 1000, "Max matches of any single regexp per row, used to safeguard 'extractAllGroupsHorizontal' against consuming too much memory with greedy RE.", 0) \
    \
    M(UInt64, limit, 0, "Limit on read rows from the most 'end' result for select query, default 0 means no limit length", 0) \
    M(UInt64, offset, 0, "Offset on read rows from the most 'end' result for select query", 0) \
    \
    M(UInt64, function_range_max_elements_in_block, 500000000, "Maximum number of values generated by function 'range' per block of data (sum of array sizes for every row in a block, see also 'max_block_size' and 'min_insert_block_size_rows'). It is a safety threshold.", 0) \
    M(ShortCircuitFunctionEvaluation, short_circuit_function_evaluation, ShortCircuitFunctionEvaluation::ENABLE, "Setting for short-circuit function evaluation configuration. Possible values: 'enable' - use short-circuit function evaluation for functions that are suitable for it, 'disable' - disable short-circuit function evaluation, 'force_enable' - use short-circuit function evaluation for all functions.", 0) \
    \
    /** Bytedance */ \
    M(UInt64, force_alter_conflict_check, 1, "force alter conflict check", 0) \
    M(UInt64, alter_skip_check, 0, "Skip check while alter table. 1 for skipping columns and metadate strings check; 2 for skipping metadata_version check", 0) \
    M(UInt64, ha_alter_metadata_sync, 1, "Wait for actions to alter metadata. 0 - do not wait, 1 - wait for execution only of itself, 2 - wait for everyone.", 0) \
    M(UInt64, ha_alter_data_sync, 0, "Wait for actions to alter data. 0 - do not wait, 1 - wait for execution only of itself, 2 - wait for everyone.", 0) \
    M(String, blacklist_for_merge_task_regex, "CHTMP$", "A blacklist for merge task, to prevent the generation of MergeTasks for some tables.", 0) \
    M(Bool, ignore_leader_check, 0, "Ignore leader check while executing some ALTER queries", 0) \
    M(Bool, cascading_refresh_materialized_view, true, "Whether cascading refresh the materialized view", 0) \
    M(Bool, enable_element_mv_rows, false, "Whether enable element query calculate base rows and view rows", 0) \
    M(UInt64, max_rows_to_refresh_by_partition, 100000000, "The maximum rows to refresh a materialized view by partition. If exceed, we'll refresh the materialized view part by part.", 0) \
    M(UInt64, max_threads_to_refresh_by_partition, 1, "The maximum threads to refresh a materialized view by partition.", 0) \
    M(UInt64, slow_query_ms, 0, "Slow query criterial in ms. 0 means all related function will not be executed", 0)\
    M(UInt64, max_rows_to_schedule_merge, 500000000, "Max rows of merged part for merge scheduler", 0)\
    M(UInt64, total_rows_to_schedule_merge, 0, "Max total rows of merged parts for merge scheduler, 0 means unlimit", 0)\
    M(UInt64, expired_start_hour_to_merge, 12, "The hour of UTC time, if current time is greater than it, merge scheduler can lower the merge frequency", 0)\
    M(UInt64, expired_end_hour_to_merge, 12, "The hour of UTC time, if current time is smaller than it, merge scheduler can lower the merge frequency", 0)\
    M(UInt64, strict_rows_to_schedule_merge, 50000000, "Max rows of merged part for merge scheduler when the current time is expired according to expired_hour_to_merge", 0)\
    M(UInt64, max_parts_to_optimize, 1000, "Max number of parts to optimize", 0)\
    M(Bool, enable_merge_scheduler, false, "Whether to enable MergeScheduler to excute merge", 0)\
    M(Bool, conservative_merge_predicate, true, "Judge merge tree parts whether can be merged conservatively", 0)\
    M(Bool, snappy_format_blocked, false, "Using blocked decompress flow for Snappy input", 0)\
    M(String, vw, "", "The vw name set by user on which the query run without tenant information", 0) \
    M(String, virtual_warehouse, "", "The vw name set by user on which the query run", 0) \
    M(String, backup_virtual_warehouse, "", "The backup vw to run query when default vw is not avaiable", 0) \
    M(BackupVWMode, backup_vw_mode, BackupVWMode::BACKUP, "backup vw mode. backup round_robin backup_only", 0) \
    M(String, virtual_warehouse_write, "", "Deprecated, use virtual_warehouse instead", 0) \
    M(String, vw_schedule_algo, "Unknown", "algorithm for picking a worker group from vw. {Random(1),LocalRoundRobin(2),LocalLowCpu(3),LocalLowMem(4),LocalLowDisk(5),GlobalRoundRobin(102),GlobalLowCpu(103),GlobalLowMem(104),GlobalLowDisk(105)}", 0) \
    M(DialectType, dialect_type, DialectType::CLICKHOUSE, "Dialect type, e.g. CLICKHOUSE, ANSI, MYSQL", 0) \
    M(TextCaseOption, text_case_option, TextCaseOption::MIXED, "Convert identifiers to lower case/upper case just like MySQL", 0) \
    M(Bool, enable_implicit_arg_type_convert, false, "Eable implicit type conversion for functions", 0) \
    M(Bool, exception_on_unsupported_mysql_syntax, true, "Whether throws exceptions on currently unsupported mysql syntax such as auto_increment", 0) \
    M(Bool, adaptive_type_cast, true, "Performs type cast operations adaptively, according to the value", 0) \
    M(Bool, parse_literal_as_decimal, false, "Parse numeric literal as decimal instead of float", 0) \
    M(Bool, formatdatetime_f_prints_single_zero, false, "Formatter '%f' in function 'formatDateTime()' produces a single zero instead of six zeros if the formatted value has no fractional seconds.", 0) \
    M(Bool, formatdatetime_parsedatetime_m_is_month_name, false, "Formatter '%M' in functions 'formatDateTime()' and 'parseDateTime()' produces the month name instead of minutes.", 0) \
    M(Bool, date_format_clickhouse, false, "use date_format as a clickhouse function instead of hive", 0) \
    M(Bool, tealimit_order_keep, false, "Whether tealimit output keep order by clause", 0)\
    M(UInt64, early_limit_for_map_virtual_columns, 0, "Enable early limit while quering _map_column_keys column", 0)\
    M(Bool, skip_nullinput_notnull_col, false, "Skip null value in JSON for not null column", 0)\
    M(Milliseconds, meta_sync_task_interval_ms, 1*60*60*1000, "Interval of background schedule task for metasore synchronization", 0)\
    M(Bool, enable_fetch_part_incrementally, true, "Whether to enable fetching part incrementally", 0) \
    M(String, \
      blocklist_for_merge_thread_regex, \
      "", \
      "A blacklist for merge thread, to prevent the generation of MergeTasks for some tables.", \
      0) \
    M(Bool, \
      decimal_division_use_extended_scale, \
      false, \
      "If enabled, the result scale of decimal division is determined by: max(6, S1)", \
      0) \
    M(Bool, \
      decimal_arithmetic_promote_storage, \
      false, \
      "Promote storage for some cases of decimal arithmetic operation(e.g. Decimal32 * Decimal32 -> Decimal64)", \
      0) \
    M(Bool, \
      allow_extended_type_conversion, \
      false, \
      "When enabled, implicit type conversion is allowed for more input types(e.g. UInt64 & Ints, Decimal & Float, Float & Int64)", \
      0) \
    M(Bool, allow_multi_if_const_optimize, true, "Whether to optimize multiIf function for const case", 0) \
\
    M(Bool, use_query_cache, false, "Enable the query cache", 0) \
    M(Bool, enable_transactional_query_cache, true, "Enable transactional query cache for CNCH engine table", IMPORTANT) \
    M(Bool, enable_writes_to_query_cache, true, "Enable storing results of SELECT queries in the query cache", 0) \
    M(Bool, enable_reads_from_query_cache, true, "Enable reading results of SELECT queries from the query cache", 0) \
    M(Bool, \
      query_cache_store_results_of_queries_with_nondeterministic_functions, \
      false, \
      "Store results of queries with non-deterministic functions (e.g. rand(), now()) in the query cache", \
      0) \
    M(UInt64, \
      query_cache_max_size_in_bytes, \
      0, \
      "The maximum amount of memory (in bytes) the current user may allocate in the query cache. 0 means unlimited. ", \
      0) \
    M(UInt64, \
      query_cache_max_entries, \
      0, \
      "The maximum number of query results the current user may store in the query cache. 0 means unlimited.", \
      0) \
    M(UInt64, query_cache_min_query_runs, 0, "Minimum number a SELECT query must run before its result is stored in the query cache", 0) \
    M(Milliseconds, \
      query_cache_min_query_duration, \
      0, \
      "Minimum time in milliseconds for a query to run for its result to be stored in the query cache.", \
      0) \
    M(Bool, query_cache_compress_entries, true, "Compress cache entries.", 0) \
    M(Bool, \
      query_cache_squash_partial_results, \
      true, \
      "Squash partial result blocks to blocks of size 'max_block_size'. Reduces performance of inserts into the query cache but improves " \
      "the compressability of cache entries.", \
      0) \
    M(Seconds, query_cache_ttl, 60, "After this time in seconds entries in the query cache become stale", 0) \
    M(Bool, query_cache_share_between_users, false, "Allow other users to read entry in the query cache", 0) \
    M(Bool, create_view_check_column_names, true, "When executing CREATE VIEW queries, whether check column names are consistent with select query", 0) \
    M(Bool, rewrite_unknown_left_join_identifier, true, "Whether to rewrite unknown left join identifier, this is a deprecated feature but Aeolus SQL depends on it", 0) \
    \
    /** settings in cnch **/ \
    M(Seconds, drop_range_memory_lock_timeout, 5, "The time that spend on wait for memory lock when doing drop range", 0) \
    M(UInt64, cnch_data_retention_time_in_sec, 3*24*60*60, "Waiting time when dropped table or database is actually removed.", 0) \
    M(Milliseconds, topology_lease_renew_interval_ms, 500, "Interval of background task to renew topology lease.", 0) \
    M(Milliseconds, topology_refresh_interval_ms, 500, "Interval of background task to sync topology from consul.", 0) \
    M(Milliseconds, topology_retry_interval_ms, 100, "Interval of topology background task to retry.", 0) \
    M(Milliseconds, topology_lease_life_ms, 12000, "Expiration time of topology lease.", 0) \
    M(Milliseconds, topology_session_restart_check_ms, 120, "Check and try to restart leader election for server master", 0) \
    M(UInt64, catalog_max_commit_size, 500, "Max record number to be committed in one batch.", 0) \
    M(Bool, catalog_enable_multiple_threads, false, "Whether leverage multiple threads to handle metadata.", 0) \
    M(UInt64, catalog_multiple_threads_min_parts, 10000, "Minimum parts number to enable multi-thread in calc visible parts.", 0) \
    M(Bool, server_write_ha, false, "Whether to enable write on non-host server if host server is not available. Directly commit from non-host server.", 0) \
    M(Bool, enable_write_non_host_server, true, "Whether to eable write on non-host server. Will root write request to host server.", 0) \
    M(Bool, force_execute_alter, false, "Force the Alter Query to be executed ignore the host server.", 0) \
    M(UInt64, \
      cnch_clear_parts_timeout, \
      10, \
      "Wait for actions to clear the parts in workers within the specified number of seconds. 0 - wait unlimited time.", \
      0) \
    M(Seconds, cnch_fetch_parts_timeout, 60, "The timeout for gettting parts from metastore. 0 - wait unlimited time.", 0) \
    M(UInt64, \
      cnch_sync_parts_timeout, \
      10, \
      "Wait for actions to sync the parts in workers within the specified number of seconds. 0 - wait unlimited time.", \
      0) \
    M(UInt64, \
      part_cache_manager_thread_pool_size, \
      16, \
      "Number of thread performing background parts info collection in PartCacheManager.", \
      0) \
    M(String, username_for_internal_communication, "server", "Username to be used by server for authentication on worker side.", 0) \
    M(UInt64, \
      cnch_part_allocation_algorithm, \
      2, \
      "Part allocation algorithm, 0: jump consistent hashing, 1: bounded hash ring consistent hashing, 2: strict ring consistent " \
      "hashing.", \
      0) \
    M(UInt64, cnch_max_cached_storage, 2048, "Cnch storage cache size.", 0) \
    M(Bool, enable_multiple_tables_for_cnch_parts, 0, "Allow to query multiple tables for system.cnch_parts", 0) \
    M(Bool, enable_skip_non_cnch_tables_for_cnch_parts, true, "Allow to skip non cnch tables for system.cnch_parts", 0) \
    M(Bool, enable_skip_non_cnch_tables_for_cnch_trash_items, true, "Allow to skip non cnch tables for system.cnch_trash_items", 0) \
    M(Bool, enable_query_level_profiling, false, "Enable profiling at query and operator level", 0) \
    M(Bool, enable_kafka_log_profiling, false, "Enable query profiling for cnch_kafka_log table", 0) \
    M(Bool, enable_materialized_mysql_log_profiling, false, "Enable query profiling for cnch_materialized_mysql_log table", 0) \
    M(Bool, enable_query_metrics_tables_profiling, false, "Enable query profiling for query_metrics and query worker_metrics tables", 0) \
    M(UInt64, cloud_task_auto_stop_timeout, 60, "We will remove this task when heartbeat can't find this task more than retries_count times.", 0)\
    M(Bool, enable_local_disk_cache, 1, "enable global local disk cache", 0) \
    M(UInt64, parts_preload_level, 1, "used for global preload(manual alter&table auto), 0=close preload;1=preload meta;2=preload data;3=preload meta&data, Note: for table auto preload, 0 will disable all table preload, > 0 will use table preload setting", 0) \
    M(UInt64, parts_preload_throttler, 0, "used for max preload rpc concurrent count", 0) \
    M(DiskCacheMode, disk_cache_mode, DiskCacheMode::AUTO, "Whether to use local disk cache", 0) \
    M(Bool, enable_vw_customized_setting, false, "Allow vw customized overwrite profile settings", 0) \
    M(Bool, enable_async_execution, false, "Whether to enable async execution", 0) \
    M(Bool, drop_vw_disk_cache, false, "if drop the all disk cache of vw even pass one table.", 0) \
    M(Bool, force_grouping_standard_compatibility, true, "Make GROUPING function to return 1 when argument is not used as an aggregation key", 0) \
    M(Bool, disable_optimize_final, true, "Disable optimize final command", 0) \
    M(Milliseconds, brpc_data_parts_timeout_ms, 30000, "Timeout for transmitting data parts in brpc", 0) \
    M(UInt64, scan_all_table_threshold, 20, "The upper limit to avoid scan all tables in some system tables, like tables and cnch_tables.", 0) \
    M(Seconds, cnch_txn_lock_expire_duration_seconds, 30, "Transaction lock expire duration.", 0) \
    M(Seconds, cnch_lock_manager_txn_checker_schedule_seconds, 30, "LockManager txn checker schedule seconds.", 0) \
    M(UInt64, parts_preallocate_pool_size, 16, "Number of threads for part preallocate", 0) \
    /** Settings for hive */ \
    M(Bool, use_hive_metastore_filter, true, "", 0) \
    M(Bool, use_hive_cluster_key_filter, true, "", 0) \
    M(Bool, use_hive_partition_filter, true, "", 0) \
    M(Bool, use_hive_file_level_filter, false, "", 0) \
    M(Bool, use_hive_split_level_filter, false, "", 0) \
    /** Settings for Unique Table */ \
    M(Bool, enable_unique_partial_update, true, "Whether to use partial column update for INSERT", 0) \
    M(Milliseconds, dedup_worker_heartbeat_ms, 3000, "Dedup worker heartbeat interval time", 0) \
    M(Bool, enable_staging_area_for_write, false, "Whether INSERTs on unique tables should commit to the staging area or not.", 0) \
    M(UInt64, max_string_size_for_unique_key, 1048576, "Max string size limit for unique key.", 0) \
    M(Bool, enable_wait_attached_staged_parts_to_visible, true, "Enable wait for all staged parts become visible in attach process", 0) \
    M(Seconds, unique_key_attach_partition_timeout, 3600, "Default timeout (seconds) for attaching partition for unique key", 0) \
    M(Bool, enable_unique_table_attach_without_dedup, false, "Enable directly make attached parts visible without dedup for unique table, for example: override mode of offline loading", 0) \
    M(Bool, enable_unique_table_detach_ignore_delete_bitmap, false, "Enable ignore delete bitmap info when handling detach commands for unique table, for example: delete bitmap has been broken, we can just ignore it via this parameter.", 0) \
    M(DedupKeyMode, dedup_key_mode, DedupKeyMode::REPLACE, "Handle different deduplication modes, current valid values: REPLACE, THROW, APPEND. THROW mode can only be used in non-staging area scenarios. APPEND mode will not execute dedup process, which is suitable for historical non-duplicated data import scenarios", 0) \
    M(Seconds, unique_sleep_seconds_after_acquire_lock, 0, "Only for test", 0) \
    \
    /** Settings for Map */ \
    M(Bool, optimize_map_column_serialization, false, "Construct map value columns in advance during serialization", 0) \
    M(Bool, allow_map_access_without_key, true, "Allow access map column without providing key", 0) \
    \
    M(UInt64, resource_group_unmatched_behavior, 0, "The behavior when there is no resource group matched: 0 for let go, 1 for exception, 2 for the first root group.", 0) \
    /** Experimental functions */ \
    M(Bool, allow_experimental_funnel_functions, false, "Enable experimental functions for funnel analysis.", 0) \
    M(UInt64, grace_hash_join_initial_buckets, 1, "Initial number of grace hash join buckets", 0) \
    M(UInt64, grace_hash_join_max_buckets, 1024, "Limit on the number of grace hash join buckets", 0) \
    M(UInt64, grace_hash_join_left_side_parallel, 1, "Initial number of grace hash join left side parallel", 0) \
    M(UInt64, grace_hash_join_read_result_block_size, 65536, "Rows for reading spilled block in grace hash join.", 0) \
    M(UInt64, grace_hash_join_read_result_block_bytes, 10000000, "Bytes for reading spilled block in grace hash join.", 0) \
    M(Bool, use_grace_hash_only_repartition, false, "Only use grace hash join when exchange type is repartition", 0) \
    M(UInt64, filesystem_cache_max_download_size, (128UL * 1024 * 1024 * 1024), "Max remote filesystem cache size that can be downloaded by a single query", 0) \
    M(Bool, skip_download_if_exceeds_query_cache, true, "Skip download from remote filesystem if exceeds query cache size", 0) \
    M(UInt64, expected_table_definition_hash, 0, "If not zero, filter parts by table_definition hash, such as ALTER DETACH PARTITION BUCKET xxx commands.", 0) \
    \
    /** Complex query settings **/\
    M(Bool, enable_distributed_stages, false, "Enable complex query mode to split plan to distributed stages", 0)\
    M(Bool, fallback_to_simple_query, false, "Enable fallback if there is any syntax error", 0)\
    M(Bool, debug_plan_generation, false, "Enable complex query mode to split plan to distributed stages", 0)\
    M(Milliseconds, send_plan_segment_timeout_ms, 10000, "Default timeout for send plan segment by rpc", 0) \
    M(Bool, send_plan_segment_by_brpc_join_per_stage, false, "Whether to send plan segment by BRPC and join async rpc request per stage", 0)\
    M(Bool, send_plan_segment_by_brpc_join_at_last, true, "Whether to send plan segment by BRPC and join async rpc request at last", 0)\
    \
    /** Brpc config **/\
    M(Bool, enable_brpc_builtin_services, true, "Whether to enable brpc builtin services", 0)\
    \
    /** Obsolete settings that do nothing but left for compatibility reasons. Remove each one after half a year of obsolescence. */ \
    M(Bool, enable_memory_efficient_ingest_partition, true, "Obsolete setting, does nothing.", 0) \
    M(UInt64, max_memory_usage_for_all_queries, 0, "Obsolete setting, does nothing.", 0) \
    M(UInt64, multiple_joins_rewriter_version, 0, "Obsolete setting, does nothing.", 0) \
    M(Bool, enable_debug_queries, false, "Obsolete setting, does nothing.", 0) \
    M(Bool, allow_experimental_database_atomic, true, "Obsolete setting, does nothing.", 0) \
    M(Bool, allow_experimental_bigint_types, true, "Obsolete setting, does nothing.", 0) \
    M(HandleKafkaErrorMode, handle_kafka_error_mode, HandleKafkaErrorMode::DEFAULT, "Obsolete setting, does nothing.", 0) \
    M(Bool, database_replicated_ddl_output, true, "Obsolete setting, does nothing.", 0) \
    M(Bool, \
      enable_dictionary_compression, \
      false, \
      "Enable the dictioanry compression and decompression when performing a query (deprecated setting).", \
      0) \
    M(Bool, enable_rewrite_alias_in_select, true, "Whether rewrite alias in select (Obsolete setting).", 0) \
    M(Bool, enable_replace_group_by_literal_to_symbol, false, "Obsolete setting, does nothing.", 0) \
    M(Bool, enable_replace_order_by_literal_to_symbol, false, "Obsolete setting, does nothing.", 0) \
    M(Bool, enable_topn_filtering_optimization, false, "Obsolete setting, Whether enable TopNFilterting optimization", 0) \
    /** Ingestion */ \
    M(Seconds, ingest_column_memory_lock_timeout, 5, "The time that spend on wait for memory lock", 0) \
    M(UInt64, max_ingest_columns_size, 10, "The maximum number of columns that can be ingested.", 0) \
    M(UInt64, \
      memory_efficient_ingest_partition_max_key_count_in_memory, \
      50000000, \
      "The maximum number of key for ingestion to keep in memory during join.", \
      0) \
    M(UInt64, ingest_partition_timeout, 3600, "The ingestion timeout in seconds.", 0) \
    M(UInt64, max_ingest_rows_size, 50000000, "The maximum rows for ingestion.", 0) \
    M(UInt64, parallel_ingest_threads, 8, "The maximum threads for ingestion.", 0) \
    M(Bool, enable_replicas_create_ingest_node_in_zk, 0, "Whether to enable replicas to create ingest node in zk", 0) \
    M(Bool, allow_ingest_empty_partition, false, "Allow empty partition replace target table", 0) \
    M(Bool, enable_async_ingest, false, "Allow ingest in aync mode", 0) \
    M(Bool, optimize_ingest_with_bucket, true, "Using bucket table to optimize ingest", 0) \
    M(UInt64, max_ingest_task_on_workers, 50, "Max ingest task on wokers, now It is not make a distinction with query num", 0) \
    /** Early Stop **/ \
    M(Milliseconds, query_shard_timeout_time, 0, "Timeout for query shard", 0) \
    M(Milliseconds, late_shard_relax_time, 1000, "Relaxition time for late shard", 0) \
    M(Float, \
      exception_threshold_for_timeout_query, \
      0.1, \
      "Timeout for query shard, if timeout shards beyond this threshold, then throw exception", \
      0) \
    M(Bool, enable_early_stop_metric, 0, "Whether output metrics of early stop", 0) \
    M(UInt64, query_queue_size, 100, "Max query queue size", 0) \
    M(Bool, enable_query_queue, false, "Whether enable query queue", 0) \
    M(VWQueueMode, vw_queue_mode, VWQueueMode::Skip, "Whether enqueue virtual warehouse queue: Skip/Match/Force", 0) \
    M(QueueName, queue_name, QueueName::Auto, "the name of vw queue: highest/high/normal/low/lowest/auto", 0) \
    M(UInt64, vw_query_queue_timeout_ms, 100000, "Max queue pending time in ms", 0) \
    M(UInt64, query_queue_timeout_ms, 100000, "Max queue pending time in ms", 0) \
    M(Bool, enable_concurrency_control, false, "Whether enable concurrency control", 0) \
    M(UInt64, operator_profile_receive_timeout, 3000, "Max waiting time for operator profile in ms", 0) \
    /** Optimizer relative settings */ \
    M(Bool, enable_optimizer, true, "Whether enable query optimizer", 0) \
    M(Bool, enable_optimizer_fallback, false, "Whether enable query optimizer fallback to clickhouse origin when failed", 0) \
    M(Bool, enable_optimizer_for_create_select, false, "Whether enable query optimizer for CREATE TABLE SELECT queries", 0) \
    M(Bool, log_optimizer_run_time, false, "Whether Log optimizer runtime", 0) \
    M(UInt64, plan_optimizer_timeout, 600000, "Max running time of a plan rewriter optimizer in ms", 0) \
    M(UInt64, plan_optimizer_rule_warning_time, 1000, "Send warning if a optimize rule optimize time exceed timeout", 0) \
    M(Bool, enable_plan_cache, false, "Whether enable plan cache", 0) \
    M(Bool, force_plan_cache, false, "Force to use plan cache", 0) \
    M(UInt64, max_plannode_count, 200, "The max plannode count", 0) \
    M(Bool, enable_memory_catalog, false, "Enable memory catalog for unittest", 0) \
    M(UInt64, memory_catalog_worker_size, 8, "Memory catalog work size for unittest", 0) \
    M(Bool, print_graphviz, false, "Whether print graphviz", 0) \
    M(String, graphviz_path, "/tmp/plan/", "The path of graphviz plan", 0) \
    M(Bool, print_graphviz_ast, false, "Whether print graphviz", 0) \
    M(Bool, print_graphviz_planner, false, "Whether print graphviz", 0) \
    M(Bool, use_sql_binding, false, "Whether use SQL binding", 0) \
    M(Bool, enable_active_prewhere, false, "Whether to actively generate prewhere by statistics", 0) \
    M(Float, max_active_prewhere_selectivity, 0.3, "Max Selectivity of actively generated prewheres", 0) \
    M(UInt64, max_active_prewhere_size, 3, "Max Size of to actively generated prewheres", 0) \
    M(UInt64, global_bindings_update_time, 60*60, "Interval to update global binding cache from catalog, in seconds.", 0) \
    /** */ \
    M(Bool, late_materialize_aggressive_push_down, false, "When table use early materialize strategy, this setting enable aggressively moving predicates to read chain w/o considering other factor like columns size or number of columns in the query", 0) \
    /** Optimizer relative settings, Plan build and RBO */ \
    M(Bool, enable_auto_prepared_statement, false, "Whether to enable automatic prepared statement", 0) \
    M(Bool, enable_nested_loop_join, true, "Whether enable nest loop join for outer join with filter", 0)\
    M(Bool, enforce_all_join_to_any_join, false, "Whether enforce all join to any join", 0) \
    M(Bool, enable_implicit_type_conversion, true, "Whether enable implicit type conversion for JOIN, Set operation, IN subquery", 0) \
    M(Bool, rewrite_like_function, true, "Rewrite simple pattern like function", 0) \
    M(UInt64, iterative_optimizer_timeout, 10000, "Max running time of a single iterative optimizer in ms", 0) \
    M(Bool, debug_iterative_optimizer, false, "If enabled, iterative optimizer will print plan after each rule application", 0) \
    M(Bool, enable_remove_uncorrelated_in_subquery, true, "Whether enable remove uncorrelated in subquery", 0) \
    M(Bool, enable_remove_correlated_in_subquery, true, "Whether enable remove correlated in subquery", 0) \
    M(Bool, enable_remove_uncorrelated_exists_subquery, true, "Whether enable remove uncorrelated exists subquery", 0) \
    M(Bool, enable_remove_correlated_exists_subquery, true, "Whether enable remove correlated exists subquery", 0) \
    M(Bool, enable_remove_uncorrelated_scalar_subquery, true, "Whether enable remove uncorrelated scalar subquery", 0) \
    M(Bool, enable_remove_correlated_scalar_subquery, true, "Whether enable remove correlated scalar subquery", 0) \
    M(Bool, enable_remove_uncorrelated_quantified_comparison_subquery, true, "Whether enable remove correlated quantified comparison subquery", 0) \
    M(Bool, enable_remove_correlated_quantified_comparison_subquery, true, "Whether enable remove correlated quantified comparison subquery", 0) \
    M(Bool, enable_unnesting_subquery_with_window, true, "Whether enable unnesting subquery with window", 0) \
    M(Bool, enable_unnesting_subquery_with_semi_anti_join, true, "Whether enable unnesting subquery with semi anti join", 0) \
    M(Bool, eliminate_cross_joins, true, "Whether eliminate cross joins", 0) \
    M(Bool, enable_unify_join_outputs, true, "Whether enable unify join output ", 0) \
    M(Bool, enable_unify_nullable_type, true, "Whether enable unify nullable type", 0) \
    M(Bool, enable_sorting_property, true, "Whether enable sorting property rule", 0) \
    M(Bool, enable_streaming_property, true, "Whether enable streaming property rule", 0) \
    M(Bool, enable_distinct_to_aggregate, true, "Whether enable convert distinct to group by", 0) \
    M(Bool, enable_cross_join_to_union, false, "Whether enable convert cross join to union", 0) \
    M(Bool, enable_distinct_remove, true, "Whether to eliminate redundancy during execution", 0) \
    M(Bool, enable_single_distinct_to_group_by, true, "Whether enable convert single count distinct to group by", 0) \
    M(Bool, enable_mark_distinct_optimzation, false, "Whether enable Mark distinct optimization", 0)                                            \
    M(Bool, enable_expand_distinct_optimization, true, "Whether enable rewrite distinct optimization", 0)                                            \
    M(ExpandMode, expand_mode, ExpandMode::EXPAND, "Rewrite distinct optimization, Expand Mode : EXPAND|UNION|CTE", 0)                                            \
    M(Bool, enable_common_predicate_rewrite, true, "Whether enable common predicate rewrite", 0) \
    M(Bool, enable_common_join_predicate_rewrite, true, "Whether enable common predicate rewrite", 0) \
    M(Bool, enable_swap_predicate_rewrite, true, "Whether enable swap predicate rewrite", 0) \
    M(Bool, rewrite_predicate_by_domain, true, "When enabled, merge predicates belonging to the same domain", 0) \
    M(Bool, rewrite_complex_predicate_by_domain, false, "Whether enabled, extract merged predicate belonging to the same domain for complex predicate(which normally are DNFs)", 0) \
    M(Bool, enable_unwrap_cast_in, true, "Whether enable unwrap cast function", 0) \
    M(Bool, enable_windows_reorder, true, "Reorder adjacent windows to decrease exchange", 0) \
    M(Bool, enable_push_partial_agg, true, "Whether enable push partial agg", 0) \
    M(Bool, enable_shuffle_before_state_func, true, "Whether shuffle when agg func is state func.", 0) \
    M(Bool, enable_share_common_plan_node, true, "Whether enable share common plan node using cte", 0) \
    M(Bool, enable_redundant_sort_removal, true, "Whether enable ignore redundant sort in subquery", 0) \
    M(Bool, enable_remove_unused_cte, true, "Whether enable remove unused cte", 0) \
    M(Bool, enable_filter_window_to_partition_topn, true, "Filter window to partition topn", 0) \
    M(Bool, enable_optimizer_support_window, true, "Optimizer support window", 0) \
    M(Bool, enable_filter_window_to_sorting_limit, true, "Filter window to sorting limit", 0) \
    M(Bool, optimizer_projection_support, false, "Use projection in optimizer mode", 0) \
    M(Bool, optimizer_index_projection_support, true, "Use indexprojection in optimizer mode", 0) \
    M(Bool, enable_setoperation_to_agg, true, "Whether enable rewrite set operation to aggregation", 0)                                            \
    M(Bool, enable_execute_uncorrelated_subquery, false, "Whether enable execute uncorrelated subquery", 0) \
    M(UInt64, execute_uncorrelated_in_subquery_size, 10000, "Size of execute uncorrelated in subquery", 0) \
    M(Bool, enable_subcolumn_optimization_through_union, true, "Whether enable sub column optimization through set operation.", 0) \
    M(Bool, enable_buffer_for_deadlock_cte, true, "Whether to buffer data for deadlock cte", 0) \
    M(UInt64, statistics_collect_debug_level, 0, "Debug level for statistics collector", 0) \
    M(Bool, enable_add_exchange, true, "Whether to enable AddExchange rule", 0) \
    M(Bool, enable_bitmap_index_splitter, true, "Whether to enable BitMapIndexSplitter", 0) \
    M(Bool, enable_column_pruning, true, "Whether to enable ColumnPruning", 0) \
    M(Bool, enable_add_projection_to_pruning, true, "Whether add projection when column pruning", 0) \
    M(Bool, enable_predicate_pushdown_rewrite, true, "Whether to enable PredicatePushdown", 0) \
    M(Bool, enable_hints_propagator, true, "Whether to enable HintsPropagator", 0) \
    M(Bool, enable_join_algorithm_hints, true, "Whether to enable ImplementJoinAlgorithmHints", 0) \
    M(Bool, enable_join_operation_hints, true, "Whether to enable ImplementJoinOperationHints", 0) \
    M(Bool, enable_join_order_hint, true, "Whether to enable ImplementJoinOrderHints", 0) \
    M(Bool, enable_set_join_distribution, true, "Whether to enable SetJoinDistribution rule", 0) \
    M(Bool, enable_explain_analyze, true, "Whether to enable ExplainAnalyze rule", 0) \
    M(Bool, enable_implement_except, true, "Whether to enable ImplementExceptRule rule", 0) \
    M(Bool, enable_implement_intersect, true, "Whether to enable ImplementIntersectRule rule", 0) \
    M(Bool, enable_inline_projection, true, "Whether to enable InlineProjections rule", 0) \
    M(Bool, enable_merge_aggregate, true, "Whether to enable MergeAggregatings rule", 0) \
    M(Bool, enable_merge_union, true, "Whether to enable MergeUnionRule rule", 0) \
    M(Bool, enable_merge_except, true, "Whether to enable MergeExceptRule rule", 0) \
    M(Bool, enable_merge_intersect, true, "Whether to enable MergeIntersectRule rule", 0) \
    M(Bool, enable_pull_projection_on_join_through_join, true, "Whether to enable PullProjectionOnJoinThroughJoin rule", 0) \
    M(Bool, enable_push_agg_through_outer_join, true, "Whether to enable PushAggThroughOuterJoin rule", 0) \
    M(Bool, enable_push_agg_through_inner_join, true, "Whether to enable PushAggThroughInnerJoin rule", 0) \
    M(Bool, enable_push_limit_into_distinct, true, "Whether to enable PushLimitIntoDistinct rule", 0) \
    M(Bool, enable_push_limit_through_projetion, true, "Whether to enable PushLimitThroughProjection rule", 0) \
    M(Bool, enable_push_limit_through_extremes, true, "Whether to enable PushLimitThroughExtremes rule", 0) \
    M(Bool, enable_push_limit_through_union, true, "Whether to enable PushLimitThroughUnion rule", 0) \
    M(Bool, enable_push_limit_through_outer_join, true, "Whether to enable PushLimitThroughOuterJoin rule", 0) \
    M(Bool, enable_limit_zero_to_read_nothing, true, "Whether to enable LimitZeroToReadNothing rule", 0) \
    M(Bool, enable_push_down_limit_into_window, true, "Whether to enable PushdownLimitIntoWindow rule", 0) \
    M(Bool, enable_push_limit_into_sorting_rule, true, "Whether to enable PushLimitIntoSorting rule", 0) \
    M(Bool, enable_push_down_apply_through_join, true, "Whether to enable PushDownApplyThroughJoin rule", 0) \
    M(Bool, enable_push_storage_filter, true, "Whether to enable PushStorageFilter rule", 0) \
    M(Bool, enable_push_limit_into_table_scan, true, "Whether to enable PushLimitIntoTableScan rule", 0) \
    M(Bool, enable_push_aggregation_into_table_scan, true, "Whether to enable PushAggregationIntoTableScan rule", 0) \
    M(Bool, enable_push_projection_into_table_scan, true, "Whether to enable PushProjectionIntoTableScan rule", 0) \
    M(Bool, enable_push_index_projection_into_table_scan, true, "Whether to enable PushIndexProjectionIntoTableScan rule", 0) \
    M(Bool, enable_push_filter_into_table_scan, true, "Whether to enable PushFilterIntoTableScan rule", 0) \
    M(Bool, enable_inner_join_associate, true, "Whether to enable InnerJoinAssociate rule", 0) \
    M(Bool, enable_inner_join_commutation, true, "Whether to enable InnerJoinCommutation rule", 0) \
    M(Bool, enable_join_enum_on_graph, true, "Whether to enable JoinEnumOnGraph rule", 0) \
    M(Bool, enable_join_to_multi_join, true, "Whether to enable JoinToMultiJoin rule", 0) \
    M(Bool, enable_cardinality_based_join_reorder, true, "Whether to enable CardinalityBasedJoinReorder rule", 0) \
    M(Bool, enable_selectivity_based_join_reorder, true, "Whether to enable SelectivityBasedJoinReorder rule", 0) \
    M(Bool, enable_left_join_to_right_join, true, "Whether to enable LeftJoinToRightJoin rule", 0) \
    M(Bool, enable_magic_set_push_through_projection, true, "Whether to enable MagicSetPushThroughProject rule", 0) \
    M(Bool, enable_magic_set_push_through_join, true, "Whether to enable MagicSetPushThroughJoin rule", 0) \
    M(Bool, enable_magic_set_push_through_filter, true, "Whether to enable MagicSetPushThroughFilter rule", 0) \
    M(Bool, enable_magic_set_push_through_aggregating, true, "Whether to enable MagicSetPushThroughAggregating rule", 0) \
    M(Bool, enable_pull_outer_join, true, "Whether to enable PullOuterJoin rule", 0) \
    M(Bool, enable_push_join_through_union, true, "Whether to enable PushJoinThroughUnion rule", 0) \
    M(Bool, enable_semi_join_push_down, true, "Whether to enable SemiJoinPushDown rule", 0) \
    M(Bool, enable_simplify_predicate_rewrite, true, "Whether to enable SimplifyPredicateRewrite rule", 0) \
    M(Bool, enable_simplify_prewhere_rewrite, true, "Whether to enable SimplifyPrewhereRewrite rule", 0) \
    M(Bool, enable_simplify_join_filter_rewrite, true, "Whether to enable SimplifyJoinFilterRewrite rule", 0) \
    M(Bool, enable_simplify_expression_rewrite, true, "Whether to enable SimplifyExpressionRewrite rule", 0) \
    M(Bool, enable_simplify_predicate_in_projection, false, "Whether to rewrite predicate in projection", 0) \
    M(Bool, enable_simplify_assume_not_null, true, "Whether to remove redundant assumeNotNull --temporary settings", 0) \
    M(Bool, enable_evaluate_constant_for_nondeterministic, true, "Enable evaluate the constant result of non-deterministic functions while using optimizer", 0) \
    M(Bool, enable_remove_redundant, true, "Whether to enable RemoveRedundant rules", 0) \
    M(Bool, enable_push_projection, true, "Whether to enable PushProjection rules", 0) \
    M(Bool, enable_push_partial_agg_through_exchange, true, "Whether to enable PushPartialAggThroughExchange rules", 0) \
    M(Bool, enable_push_partial_agg_through_union, true, "Whether to enable PushPartialAggThroughUnion rules", 0) \
    M(Bool, enable_push_partial_sorting_through_exchange, true, "Whether to enable PushPartialSortingThroughExchange rules", 0) \
    M(Bool, enable_push_partial_limit_through_exchange, true, "Whether to enable PushPartialLimitThroughExchange rules", 0) \
    M(Bool, enable_push_partial_distinct_through_exchange, true, "Whether to enable PushPartialDistinctThroughExchange rules", 0) \
    M(UInt64, max_rows_to_use_topn_filtering, 0, "The maximum N of TopN to use topn filtering optimization. Set 0 to choose this value adaptively.", 0) \
    M(String, topn_filtering_algorithm_for_unsorted_stream, "SortAndLimit", "The default topn filtering algorithm for unsorted stream, can be one of: 'SortAndLimit', 'Heap'", 0) \
    M(Bool, enable_create_topn_filtering_for_aggregating, false, "Whether to enable CreateTopNFilteringForAggregating rules", 0) \
    M(Bool, enable_push_topn_through_projection, true, "Whether to enable PushTopNThroughProjection rules", 0) \
    M(Bool, enable_push_topn_filtering_through_projection, true, "Whether to enable PushTopNFilteringThroughProjection rules", 0) \
    M(Bool, enable_push_topn_filtering_through_union, true, "Whether to enable PushTopNFilteringThroughUnion rules", 0) \
    M(Bool, enable_optimize_aggregate_memory_efficient, false, "Whether to enable OptimizeMemoryEfficientAggregation rules", 0) \
    M(Bool, enable_cascades_optimizer, true, "Whether to enable CascadesOptimizer", 0) \
    M(Bool, enable_iterative_rewriter, true, "Whether to enable InterativeRewriter", 0) \
    M(Float, multi_join_keys_correlated_coefficient, 0.8, "Coefficient about multi join keys, the smaller the value, the smaller the estimated join cardnlity, do nothing when equals 1.0", 0) \
    M(Float, multi_agg_keys_correlated_coefficient, 0.9, "Coefficient about multi agg keys, the smaller the value, the smaller the estimated agg cardnlity, do nothing when equals 1.0", 0) \
    M(Bool, enable_common_expression_sharing, true, "Whether to share common expression between steps", 0) \
    M(Bool, enable_common_expression_sharing_for_prewhere, true, "Whether to share common expression between steps and PREWHERE", 0) \
    M(Bool, enable_unalias_symbol_references, true, "Whether to enable unalias symbol references", 0) \
    M(UInt64, common_expression_sharing_threshold, 3, "The minimal cost to share a common expression, the cost is defined by (complexity * (occurrence - 1))", 0) \
    M(Bool, enable_add_local_exchange, false, "Whether to add local exchange", 0) \
    /** Optimizer relative settings, statistics */ \
    M(Bool, create_stats_time_output, true, "Enable time output in create stats, should be disabled at regression test", 0) \
    M(Bool, statistics_collect_histogram, true, "Enable histogram collection", 0) \
    M(Bool, statistics_collect_floating_histogram, true, "Collect histogram for float/double/Decimal columns", 0) \
    M(Bool, statistics_collect_floating_histogram_ndv, true, "Collect histogram ndv for float/double/Decimal columns", 0) \
    M(UInt64, statistics_collect_string_size_limit_for_histogram, 64, "Collect string histogram only for avg_size <= string_size_limit, since it's unnecessary to collect stats for text", 0) \
    M(UInt64, statistics_histogram_bucket_size, 250, "Default bucket size of histogram", 0) \
    M(UInt64, statistics_kll_sketch_log_k, 1600, "Default logK parameter of kll_sketch in statistics", 0) \
    M(Bool, statistics_enable_async, false, "Collect stats use async mode", 0) \
    M(Bool, statistics_enable_sample, true, "Use sampling for statistics", 0) \
    M(UInt64, statistics_sample_row_count, 40'000'000, "Minimal row count for sampling", 0) \
    M(Float, statistics_sample_ratio, 0.01, "Ratio for sampling", 0) \
    M(StatisticsAccurateSampleNdvMode, statistics_accurate_sample_ndv, StatisticsAccurateSampleNdvMode::AUTO, "Mode of accurate sample ndv to estimate full ndv", 0) \
    M(UInt64, statistics_batch_max_columns, 30, "Max column size in a batch when collecting stats", 0) \
    M(String, statistics_exclude_tables_regex, "", "regex to exclude tables for statistics operations", 0) \
    M(Bool, statistics_simplify_histogram, false, "Reduce buckets of histogram with simplifying", 0) \
    M(Float, statistics_simplify_histogram_ndv_density_threshold, 0.2, "Histogram simplifying threshold for ndv", 0) \
    M(Float, statistics_simplify_histogram_range_density_threshold, 0.2, "Histogram simplifying threshold for range", 0) \
    M(StatisticsCachePolicy, statistics_cache_policy, StatisticsCachePolicy::Default, "Cache policy for stats command and SQLs: (default|cache|catalog)", 0) \
    M(Bool, statistics_query_cnch_parts_for_row_count, true, "Use cnch parts instead of count(*) for row count to speed up test", 0) \
    /** Optimizer relative settings, cost model and estimation */ \
    M(Float, cost_calculator_table_scan_weight, 1, "Table scan cost weight for cost calculator", 0) \
    M(Float, cost_calculator_aggregating_weight, 7, "Aggregate output weight for cost calculator", 0) \
    M(Float, cost_calculator_join_probe_weight, 0.5, "Join probe side weight for cost calculator", 0) \
    M(Float, cost_calculator_join_build_weight, 1.5, "Join build side weight for cost calculator", 0) \
    M(Float, cost_calculator_join_output_weight, 0.5, "Join output weight for cost calculator", 0) \
    M(Float, cost_calculator_cte_weight, 1, "CTE output weight for cost calculator", 0) \
    M(Float, cost_calculator_cte_weight_for_join_build_side, 1.3, "Join build side weight for cost calculator", 0) \
    M(Float, cost_calculator_projection_weight, 0.1, "CTE output weight for cost calculator", 0) \
    M(Float, stats_estimator_join_filter_selectivity, 0.5, "Join filter selectivity", 0) \
    M(Float, stats_estimator_anti_join_filter_coefficient, 0.6, "Anti Join filter coefficient", 0) \
    M(Float, stats_estimator_first_agg_key_filter_coefficient, 0.3, "First agg key coefficient", 0) \
    M(Float, stats_estimator_remaining_agg_keys_filter_coefficient, 1.5, "Remaining agg key coefficient", 0) \
    M(Float, stats_estimator_unknown_filter_selectivity, 0.25, "Join filter selectivity", 0) \
    M(Float, stats_estimator_unknown_in_filter_selectivity, 0.5, "In filter selectivity", 0) \
    M(Float, stats_estimator_like_selectivity, 0.15, "Like filter selectivity", 0) \
    M(Bool, enable_estimate_without_symbol_statistics, false, "Try to estimiate cardinality even if no symbol statistics", 0) \
    M(Bool, enable_left_deep_join_reorder, false, "Try to do join reorder without accurate statistics", 0) \
    M(Bool, enable_pk_fk, true, "Whether enable PK-FK join estimation", 0) \
    M(Bool, enable_real_pk_fk, true, "Whether enable Real PK-FK join estimation", 0) \
    M(Float, pk_selectivity, 1.0, "PK selectivity for join estimation", 0) \
    /** Optimizer relative settings, CBO, CTE, MagicSet, MV */ \
    M(Bool, enable_join_reorder, true, "Whether enable join reorder", 0) \
    M(UInt64, cascades_optimizer_timeout, 10000, "Max running time of a single cascades optimizer in ms", 0) \
    M(UInt64 , max_graph_reorder_size, 6, "Max tables join order enum on graph", 0) \
    M(UInt64 , heuristic_join_reorder_enumeration_times, 3, "Heuristic times in CardinalityBased Join Reorder algorithm", 0) \
    M(Bool, enable_cbo, true, "Whether enable CBO", 0) \
    M(Bool, enable_cascades_pruning, true, "Whether enable cascades pruning", 0) \
    M(Bool, enum_replicate, true, "Enum replicate join", 0) \
    M(Bool, enum_repartition, true, "Enum repartition join", 0) \
    M(Bool, enum_replicate_no_stats, true, "Enum replicate join when statistics not exists", 0) \
    M(UInt64, max_replicate_build_size, 200000, "Max join build size, when enum replicate", 0) \
    M(UInt64, max_replicate_shuffle_size, 50000000, "Max join build size, when enum replicate", 0) \
    M(UInt64, parallel_join_threshold, 2000000, "Parallel join right source rows threshold", 0) \
    M(Bool, enable_adaptive_scheduler, false, "Whether enable adaptive scheduler", 0) \
    M(Bool, enable_wait_cancel_rpc, false, "Whether wait rpcs of cancel worker to finish", 0) \
    M(UInt64, parallel_join_rows_batch_threshold, 4096, "Rows that concurrent hash join wait data reach, then to build hashtable or join block", 0) \
    M(Bool, add_parallel_after_join, false, "Add parallel after join", 0) \
    M(Bool, enforce_round_robin, false, "Whether add round robin exchange node", 0) \
    M(Bool, enable_shuffle_with_order, false, "Whether enable keep data order when shuffle", 0) \
    M(Bool, enable_merge_require_property, false, "Whether enable merge required property in aggregation", 0) \
    M(Bool, enable_join_graph_support_filter, true, "Whether enable join graph support filter", 0) \
    M(Bool, enable_equivalences, true, "Whether enable using equivalences when property match", 0) \
    M(Bool, enable_injective_in_property, false, "Whether enable using injective function when property match", 0) \
    M(UInt64, max_expand_join_key_size, 3, "Whether enable using equivalences when property match", 0) \
    M(UInt64, max_expand_agg_key_size, 3, "Max allowed agg/window keys number when expand powerset when property match", 0) \
    M(Bool, enable_sharding_optimize, false, "Whether enable sharding optimization, eg. local join", 0) \
    M(Bool, enable_magic_set, true, "Whether enable magic set rewriting for join aggregation", 0) \
    M(Float, magic_set_filter_factor, 0.5, "The minimum filter factor of magic set, used for early pruning", 0) \
    M(UInt64, magic_set_max_search_tree, 2, "The maximum table scans in magic set, used for early pruning", 0) \
    M(UInt64, magic_set_source_min_rows, 10000, "The minimum rows of source node in magic set, used for early pruning", 0) \
    M(Float, magic_set_rows_factor, 0.6, "The minimum rows of source node in magic set, used for early pruning", 0) \
    M(Bool, enable_magic_set_cte, true, "Whether enable magic set rewriting build as cte", 0) \
    M(CTEMode, cte_mode, CTEMode::AUTO, "CTE mode: SHARED|INLINED|AUTO|ENFORCED", 0) \
    M(SpillMode, spill_mode, SpillMode::MANUAL, "SpillMode: MANUAL(default)|AUTO", 0) \
    M(UInt64, max_allowed_mem_size_in_join_spill, 512000000, "Max allowed memory-size(estimated) in join spill", 0) \
    M(Float, spill_triger_threshold, 0.7, "Threshold to triger spill then memory usage reach a certain ratio of memory quota", 0) \
    M(Bool, enable_cte_property_enum, false, "Whether enumerate all possible properties for cte", 0) \
    M(Bool, enable_cte_common_property, true, "Whether search common property for cte", 0) \
    M(Bool, enable_windows_parallel, false, "Whether run windows in parallel", 0) \
    M(Bool, enable_view_based_query_rewrite, false, "Whether enable materialized view based rewriter for query, compatible for  enable_materialized_view_rewrite", 0) \
    M(Bool, enable_non_equijoin_reorder, true, "Whether enable no equi join reorder", 0) \
    M(Bool, enable_materialized_view_rewrite, true, "Whether enable materialized view based rewriter for query", 0) \
    M(Bool, enable_sync_materialized_view_rewrite, true, "Whether enable materialized view based rewriter for sync materialized view", 0) \
    M(Bool, enforce_materialized_view_rewrite, false, "Whether throw exception if materialized view is not applied", 0) \
    M(String, enable_push_partial_block_list, "", "Aggregate names who can push partial agg, split by ',' => axxx,bxxx,cxxx", 0) \
    M(Bool, enable_materialized_view_ast_rewrite, false, "Whether enable materialized view based rewriter for query", 0) \
    M(Bool, enable_materialized_view_rewrite_verbose_log, false, "Whether enable materialized view based rewriter for query", 0) \
    M(Bool, enable_materialized_view_empty_grouping_rewriting, true, "Whether enable materialized view based rewriter for query", 0) \
    M(Bool, enable_materialized_view_join_rewriting, true, "Whether enable materialized view based rewriter for query using join materialized views", 0) \
    M(Bool, enable_materialized_view_union_rewriting, true, "Whether enable materialized view based rewriter for query using union", 0) \
    M(MaterializedViewConsistencyCheckMethod, materialized_view_consistency_check_method, MaterializedViewConsistencyCheckMethod::PARTITION, "The method to check whether a materialized view is consistent with the base table for a query", 0) \
    M(Bool, enable_execute_query, true, "Whether to execute this query", 0) \
    M(UInt64, max_plan_segment_num, 500, "maximum plan segments allowed, 0 means no restriction", 0)\
    M(Bool, force_create_foreign_key, false, "Whether to create inexistent foreign key when creating a table", 0) \
    M(Bool, enable_group_by_keys_pruning, false, "Whether to enable RBO -- group by keys pruning optimization", 0) \
    M(Bool, enable_eager_aggregation, false, "Whether to enable RBO -- eager aggregation optimization", 0) \
    M(Bool, only_push_agg_with_functions, false, "Only use eager aggregation with functions", 0) \
    M(Float, agg_push_down_threshold, 40.0, "Which ratio is greater than threshold can be push down", 0) \
    M(String, eager_agg_join_id_blocklist, "", "Which join in blocklist can't be push down through", 0) \
    M(Bool, enable_eliminate_join_by_fk, false, "Whether to enable RBO -- eliminate join by fk optimization", 0) \
    M(Bool, enable_eliminate_complicated_pk_fk_join, false, "Whether to eliminate complicated join by fk optimization", 0) \
    M(Bool, enable_eliminate_complicated_pk_fk_join_without_top_join, false, "Whether to allow eliminate complicated join by fk pull through pass the multi-child node even if no top join", 0) \
    M(Bool, enable_filtered_pk_selectivity, 1, "Enable the selectivity of filtered pk table", 0) \
    \
    /** remote disk cache*/ \
    M(Bool, use_local_cache_for_remote_storage, true, "Use local cache for remote storage like HDFS or S3, it's used for remote table engine only", 0) \
    M(Bool, enable_parquert_orc_split, false, "Use local cache for remote storage like HDFS or S3, it's used for remote table engine only", 0) \
    \
    /** Exchange settings */ \
    M(UInt64, min_compatible_brpc_minor_version, 2, "Min compatble version of inter server BRPC protocol", 0) \
    M(Bool, exchange_enable_multipath_reciever, true, "Whether enable exchange new mode ", 0) \
    M(UInt64, exchange_parallel_size, 1, "Exchange parallel size", 0) \
    M(UInt64, exchange_source_pipeline_threads, 16, "Recommend number of threads for pipeline which reading data from exchange, ingoned if exchange need keep data order", 0) \
    M(UInt64, exchange_timeout_ms, 100000, "Exchange request timeout ms",0) \
    M(UInt64, exchange_wait_accept_max_timeout_ms, 10000, "Exchange receiver wait accept max timeout ms",0) \
    M(UInt64, exchange_queue_bytes, 209715200, "Queue size(bytes) for exchange queue, 0 means disable", 0) \
    M(UInt64, exchange_local_receiver_queue_size, 300, "Queue size for local exchange receiver",0) \
    M(UInt64, exchange_remote_receiver_queue_size, 100, "Queue size for remote exchange receiver",0) \
    M(UInt64, exchange_multi_path_receiver_queue_size, 200, "Queue size for multi path exchange receiver", 0) \
    M(Bool, exchange_enable_block_compress, false, "Whether enable exchange block compress ", 0) \
    M(UInt64, exchange_stream_max_buf_size, 209715200, "Default 200M, -1 means no limit", 0) \
    M(UInt64, exchange_buffer_send_threshold_in_bytes, 1000000, "The minimum bytes when exchange will flush send buffer ", 0) \
    M(UInt64, exchange_buffer_send_threshold_in_row, 65505, "The minimum row num when exchange will flush send buffer", 0) \
    M(UInt64, \
      exchange_unordered_output_parallel_size, \
      8, \
      "The num of exchange sink for unorder exchange, ingoned if exchange need keep data order ", \
      0) \
    M(Bool, exchange_enable_keep_order_parallel_shuffle, false, "Whether enable parallel shuffle when exchange need keep order", 0) \
    M(Bool, exchange_enable_force_remote_mode, false, "Force exchange data transfer through network", 0) \
    M(Bool, enable_wait_for_post_processing, false, "Whether a query needs to wait for post processing rpcs done before end", 0) \
    M(Bool, exchange_enable_force_keep_order, false, "Force exchange keep data order", 0) \
    M(Bool, exchange_force_use_buffer, false, "Force exchange use buffer as possible", 0) \
    M(Bool, exchange_enable_node_stable_hash, false, "Force exchange use buffer as possible", 0) \
    M(Bool, exchange_use_query_memory_tracker, true, "Use query-level memory tracker", 0) \
    M(UInt64, wait_for_post_processing_timeout_ms, 1000, "Timeout for waiting post processing rpc from workers.", 0) \
    M(UInt64, distributed_query_wait_exception_ms, 1000,"Wait final planSegment exception from segmentScheduler.", 0) \
    M(UInt64, distributed_max_parallel_size, false, "Max distributed execution parallel size", 0) \
    \
    /** Runtime Filter settings */ \
    M(UInt64, wait_runtime_filter_timeout, 1000, "Execute filter wait for runtime filter timeout ms", 0) \
    M(Bool, enable_runtime_filter, true, "Whether enable runtime filter for join", 0) \
    M(Bool, enable_runtime_filter_cost, false, "Whether enable runtime filter cost", 0) \
    M(Bool, enable_local_runtime_filter, true, "Whether enable runtime filter in local mode", 0) \
    M(UInt64, runtime_filter_min_filter_rows, 10000, "Set minimum row to enable runtime filter", 0) \
    M(Float, runtime_filter_min_filter_factor, 0.4, "Set minimum filter factor to enable runtime filter", 0) \
    M(Float, runtime_filter_min_filter_factor_for_non_table_scan, 0.9, "Set minimum filter factor to enable runtime filter if runtime filter can not pushdown", 0) \
    M(Bool, enable_range_cover, true, "Whether use range rather than bloom or values set for runtime filter", 0) \
    M(Bool, enable_rewrite_bf_into_prewhere, true, "Whether enable pushdown runtime filter to prewhere for join", 0) \
    M(UInt64, runtime_filter_bloom_build_threshold, 2048000, "The threshold of right table to build bloom filter", 0) \
    M(UInt64, runtime_filter_in_build_threshold, 1024, "The threshold of right table to build value set filter", 0) \
    M(Bool, enable_runtime_filter_pipeline_poll, true, "No additional segment needed for the left side during broadcast join, polling time bounded", 0) \
    M(Float, adjust_range_set_filter_rate, 0.1, "If the prewhere is not range or set, adjust use this value as priority to bloom filter ", 0) \
    M(UInt64, shuffle_aware_ndv_threshold, 0, "Threshold to to use shuffle-aware grf or to use pre enlarge ndv, 0 disable shuffle-aware grf", 0) \
    M(String, runtime_filter_black_list, "", "Runtime filter ids need be blocked", 0) \
    \
    /** ip2geo settings */ \
    M(String, ip2geo_local_path, "/data01/clickhouse/data/geo_db/", "Local path for IP Database files", 0)\
    M(String, ip2geo_local_path_oversea, "/data01/clickhouse/data/geo_db/oversea/", "Local path for IP Database files for oversea", 0)\
    M(Bool, ip2geo_update_from_hdfs, 0, "Whether to update db file from hdfs", 0)\
    M(String, ipv4_file, "ipv4_pro", "IPDB file for ipv4", 0)\
    M(String, ipv6_file, "ipv6_pro", "IPDB file for ipv6", 0)\
    M(String, geoip_city_file, "GeoIP2-City", "GeoIP DB file for city", 0)\
    M(String, geoip_isp_file, "GeoIP2-ISP", "GeoIP DB file for ISP", 0)\
    M(String, geoip_asn_file, "GeoLite2-ASN", "GeoIP DB file for ASN", 0)\
    \
    /** Sample setttings */ \
    M(Bool, enable_sample_by_range, false, "Sample by range if it is true", 0) \
    M(Bool, enable_deterministic_sample_by_range, false, "Deterministic sample by range if it is true", 0) \
    M(Bool, uniform_sample_by_range, false, "Sample by range with uniform mode", 0) \
    M(Bool, ensure_one_mark_in_part_when_sample_by_range, true, "Sample by range will ensure at least a mark is sampled in each part, otherwise will do sample on parts when necessary", 0) \
    M(Bool, enable_final_sample, false, "Sample from result rows if it is true", 0) \
    M(Bool, uniform_final_sample, false, "Final sample with uniform mode", 0)\
    \
    /** clone strategy **/ \
    M(Bool, stop_clone_in_utc_time, false, "Enable stop executing clone log in utc time", 0) \
    M(UInt64, utc_time_to_stop_clone, 2, "The hour of UTC time, if current time is greater than it, clone is stopepd", 0) \
    M(UInt64, utc_time_to_start_clone, 12, "The hour of UTC time, if current time is greater than it, clone is started", 0) \
    M(String, utc_time_interval_allow_clone, "", "A list of UTC time, every two elements consist an interval which can execute clone", 0) \
    M(String, utc_time_interval_stop_clone, "", "A list of UTC time, every two elements consist an interval which stop clone", 0) \
    M(Bool, remote_query_memory_table, false, "Query remote memory table", 0) \
\
    /** S3 Storage settings*/ \
    M(UInt64, s3_gc_inter_partition_parallelism, 4, "Partition level concurrency when gc s3 table", 0) \
    M(UInt64, s3_gc_intra_partition_parallelism, 16, "Part level concurrency when gc s3 table", 0) \
    \
    M(UInt64, \
      table_partition_metrics_recalculate_recently_used_threshold, \
      1, \
      "Recalculate partitions/tables that `last_update_time > last_snapshot_time`, in hours.", 0) \
    M(UInt64, \
      table_partition_metrics_recalculate_not_recently_used_threshold, \
      1, \
      "Recalculate partitions/tables that not recently updated, in days.", 0) \
    M(UInt64, table_partition_metrics_snapshot_threshold, 1, "Snapshot partition/tables that get recently updated, in hours", 0) \
    /* Transaction and catalog */ \
    M(Bool, ignore_duplicate_insertion_label, true, "Throw an exception if false", 0) \
    M(Bool, bypass_ddl_db_lock, true, "Bypass locking database while creating tables", 0) \
    M(Bool, prefer_cnch_catalog, false, "Force using cnch catalog to get table first when resolving database and table", 0) \
    M(Bool, enable_interactive_transaction, true, "Enable interactive transaction", 0) \
    M(Bool, force_clean_transaction_by_dm, false, "Force clean transaction by dm, can be used for testing purpose", 0) \
    M(Bool, cnch_atomic_attach_part, true, "Whether to ATTACH PARTITION/PARTS in atomic way", 0) \
    M(Bool, cnch_atomic_attach_part_preemtive_lock_acquire, false, "Whether to acquire lock preemptively during atomic attach part", 0) \
    M(UInt64, cnch_notify_table_created_rpc_timeout_ms, 1000, "Timeout for create table meta when creating table", 0) \
    M(Bool, allow_full_scan_txn_records, false, "Whether to allow full scan of all transaction records on catalog", 0) \
    \
    /* Snapshot */ \
    M(String, use_snapshot, "", "If not empty, specify the name of the snapshot to use for query", 0) \
    M(Seconds, snapshot_clean_interval, 300, "How often to remove ttl expired snapshots", 0) \
    /* Outfile related Settings */ \
    M(UInt64, split_file_size_in_mb, 0, "Threshold to split the out data in 'INTO OUTFILE' clause", 0) \
    M(Bool, outfile_in_server_with_tcp, false, "Out file in sever with tcp and return client empty block", 0) \
    M(Bool, enable_distributed_output, false, "Each worker is allowed to output query results to a file separately", 0) \
    M(UInt64, outfile_buffer_size_in_mb, 1, "Out file buffer size in 'OUT FILE'", 0) \
    M(UInt64, fuzzy_max_files, 100, "The max number of files when insert with fuzzy names.", 0) \
    /** OSS related settings */ \
    M(String, oss_access_key, "", "The access_key set by user when accessing oss.", 0) \
    M(String, oss_secret_key, "", "The secret_key set by user when accessing oss.", 0) \
    M(String, oss_region, "", "The region set by user when accessing oss.", 0) \
    M(String, oss_security_token, "", "The security_key set by user when accessing oss with assume role.", 0) \
    M(String, oss_endpoint, "", "The endpoint set by user when accessing oss.", 0) \
    /* VE-TOS related settings */ \
    M(String, tos_access_key, "", "The access_key set by user when accessing ve tos.", 0) \
    M(String, tos_secret_key, "", "The secret_key set by user when accessing ve tos.", 0) \
    M(String, tos_region, "", "The region set by user when accessing ve tos.", 0) \
    M(String, tos_security_token, "", "The security_key set by user when accessing ve tos with assume role.", 0) \
    M(String, tos_endpoint, "", "The endpoint set by user when accessing ve tos, which should be compatible with S3.", 0) \
    M(UInt64, tos_connection_timeout, 10000, "The connection timeout set by user when accessing ve tos.", 0) \
    M(UInt64, tos_request_timeout, 120000, "The request timeout set by user when accessing ve tos.", 0) \
    M(String, lasfs_session_token, "", "the session_token set by user when accessing lasfs", 0) \
    /* LASFS related settings */ \
    M(String, lasfs_identity_id, "", "the identity_id set by user when accessing lasfs", 0) \
    M(String, lasfs_identity_type, "", "the identity_type set by user when accessing lasfs", 0) \
    M(String, lasfs_access_key, "", "the access_key set by user when accessing lasfs", 0) \
    M(String, lasfs_secret_key, "", "the secret_key set by user when accessing lasfs", 0) \
    M(String, lasfs_service_name, "", "the service_name set by user when accessing lasfs", 0) \
    M(String, lasfs_endpoint, "", "the endpoint set by user when accessing lasfs", 0) \
    M(String, lasfs_region, "", "the region set by user when accessing lasfs", 0) \
    /** The section above is for obsolete settings. Do not add anything there. */ \
    M(Bool, count_distinct_optimization, false, "Rewrite count distinct to subquery of group by", 0) \
    M(MaxThreads, max_download_threads, 4, "The maximum number of threads to download data (e.g. for URL engine).", 0) \
    /*start of bulk synchronous parallel section*/ \
    M(Bool, bsp_mode, false, "If enabled, query will execute in bsp mode", 0) \
    M(String, disk_shuffle_files_codec, "LZ4", "Set compression codec for disk shuffle files. I.e. LZ4, NONE.", 0) \
    M(Bool, bsp_shuffle_reduce_locality_enabled, true, "Whether to compute locality preferences for reduce tasks", 0) \
    M(Float, bsp_shuffle_reduce_locality_fraction, 0.2, "Fraction of total map output that must be at a location for it to considered as a preferred location for a reduce task", 0) \
    M(UInt64, bsp_max_retry_num, 3, "max retry number for a task(plan segment instance) in bsp mode, does not include first execution(i.e. normal execution without retry)",0) \
    /*end of bulk synchronous parallel section*/ \
    M(Bool, enable_io_scheduler, false, "Enable io scheduler", 0) \
    M(Bool, enable_io_pfra, false, "Enable prefetch and read ahead for remote read", 0) \
    M(Bool, enable_implicit_column_prewhere_push, false, "Enable push implicit column(map column) to prewhere", 0) \
    \
    M(Bool, force_manipulate_materialized_mysql_table, false, "For tables of materialized mysql engine, force to manipulate it.", 0) \
    M(Bool, throw_exception_when_mysql_connection_failed, false, "For mysql database engine, whether throw exception when mysql connection failed. If it is set to true, clickhouse may shutdown during restarting due to mysql connection failure", 0) \
    /** for inverted index*/ \
    M(UInt64, skip_inverted_index_term_size, 512, "If term size bigger than size, do not filter with inverted index", 0) \
    M(Bool, disable_str_to_array_cast, false, "disable String to Array(XXX) CAST", 0) \
    /** materialized view async refresh related settings */ \
    M(Bool, enable_mv_async_insert_overwrite, true, "whether async refresh use insert overwrite instead of drop partition and insert select mode", 0) \
    M(Bool, enable_non_partitioned_base_refresh_throw_exception, false, "Whether when async refresh non-partitioned base table, throw exception", 0) \
    M(Bool, async_mv_refresh_offload_mode, false, "offload async mv refresh task to worker.", 0) \
    M(Bool, async_mv_refresh_task_submit_to_bg_thread, true, "submit async mv refresh task to bg thread.", 0) \
    M(Bool, async_mv_refresh_task_bsp_mode, true, "whether to execute async mv refresh task in bsp mode.", 0) \
    M(UInt64, max_server_refresh_materialized_view_task_num, 10, "refresh materialized async max thread num in server.", 0) \
    M(Bool, enable_async_mv_debug, false, "whether show async debug information", 0) \
    M(Bool, async_mv_enable_mv_meta_cache, true, "whether enable read from mv meta cache.", 0) \
    \

// End of COMMON_SETTINGS
// Please add settings related to formats into the FORMAT_FACTORY_SETTINGS and move obsolete settings to OBSOLETE_SETTINGS.

#define MAKE_OBSOLETE(M, TYPE, NAME, DEFAULT) \
    M(TYPE, NAME, DEFAULT, "Obsolete setting, does nothing.", BaseSettingsHelpers::Flags::OBSOLETE)

#define OBSOLETE_SETTINGS(M) \
    /** Obsolete settings that do nothing now but left for compatibility reasons. Remove them or implement them when you have free time. */ \
    MAKE_OBSOLETE(M, Bool, enable_hybrid_allocation, false) \
    MAKE_OBSOLETE(M, Bool, make_partition_by_todate_monotonic, false) \
    MAKE_OBSOLETE(M, Bool, enable_query_cache, false) \
    MAKE_OBSOLETE(M, Bool, enable_parallel_input_generator, false) \
    MAKE_OBSOLETE(M, Bool, enable_prune_source_plan_segment, true) \
    MAKE_OBSOLETE(M, Bool, exchange_enable_metric, true) \
    MAKE_OBSOLETE(M, UInt64, cnch_offloading_mode, 0) \
    MAKE_OBSOLETE(M, UInt64, distributed_query_max_threads, 0) \
    MAKE_OBSOLETE(M, UInt64, exchange_local_no_repartition_extra_threads, 32) \
    MAKE_OBSOLETE(M, UInt64, filtered_ratio_to_use_skip_read, 0) \
    MAKE_OBSOLETE(M, Bool, enable_two_stages_prewhere, false) \
    /** End of OBSOLETE_SETTINGS */ \

#define FORMAT_FACTORY_SETTINGS(M) \
    M(Char, \
      format_csv_delimiter, \
      ',', \
      "The character to be considered as a delimiter in CSV data. If setting with a string, a string has to have a length of 1.", \
      0) \
    M(Bool, format_csv_allow_single_quotes, 1, "If it is set to true, allow strings in single quotes.", 0) \
    M(Bool, format_csv_allow_double_quotes, 1, "If it is set to true, allow strings in double quotes.", 0) \
    M(Bool, format_csv_write_utf8_with_bom, false, "If it is set to true, we'll write the BOM at the beginning of the output.", 0) \
    M(Bool, output_format_csv_crlf_end_of_line, false, "If it is set true, end of line in CSV format will be \\r\\n instead of \\n.", 0) \
    M(Bool, input_format_csv_unquoted_null_literal_as_null, false, "Consider unquoted NULL literal as \\N", 0) \
    M(Bool, input_format_csv_enum_as_number, false, "Treat inserted enum values in CSV formats as enum indices \\N", 0) \
    M(Bool, input_format_csv_arrays_as_nested_csv, false, R"(When reading Array from CSV, expect that its elements were serialized in nested CSV and then put into string. Example: "[""Hello"", ""world"", ""42"""" TV""]". Braces around array can be omitted.)", 0) \
    M(Bool, input_format_json_read_objects_as_strings, false, "Allow to parse JSON objects as strings in JSON input formats; Do NOT set it true by default as it breaks the logic of parsing JSON", 0) \
    M(Bool, input_format_json_named_tuples_as_objects, true, "Deserialize named tuple columns as JSON objects", 0) \
    M(Bool, input_format_json_defaults_for_missing_elements_in_named_tuple, true, "Insert default value in named tuple element if it's missing in json object", 0) \
    M(Bool, input_format_skip_unknown_fields, false, "Skip columns with unknown names from input data (it works for JSONEachRow, CSVWithNames, TSVWithNames and TSKV formats).", 0) \
    M(Bool, input_format_with_names_use_header, true, "For TSVWithNames and CSVWithNames input formats this controls whether format parser is to assume that column data appear in the input exactly as they are specified in the header.", 0) \
    M(Bool, input_format_import_nested_json, false, "Map nested JSON data to nested tables (it works for JSONEachRow format).", 0) \
    M(Bool, \
      input_format_defaults_for_omitted_fields, \
      true, \
      "For input data calculate default expressions for omitted fields (it works for JSONEachRow, CSV and TSV formats).", \
      IMPORTANT) \
    M(Bool, input_format_tsv_empty_as_default, false, "Treat empty fields in TSV input as default values.", 0) \
    M(Bool, input_format_tsv_enum_as_number, false, "Treat inserted enum values in TSV formats as enum indices \\N", 0) \
    M(Bool, input_format_null_as_default, true, "For text input formats initialize null fields with default values if data type of this field is not nullable", 0) \
    M(Bool, input_format_protobuf_enable_multiple_message, true, "If it is set to true, allows read protobuf messages which separated by a length header consecutively.", 0) \
    M(Bool, input_format_protobuf_default_length_parser, false, "If it is set to true, use variable length header, otherwise a 8 byte fixed length header is used.", 0) \
    M(Bool, check_data_overflow, false, "If true, throw exception when integer/decimal/date overflows; convert float/double to NULL if the column is nullable or +/- inf if the column is not nullable. Turned on automatically when dialect is mysql.", 0) \
    \
    M(DateTimeInputFormat, date_time_input_format, FormatSettings::DateTimeInputFormat::Basic, "Method to read DateTime from text input formats. Possible values: 'basic' and 'best_effort'.", 0) \
    M(DateTimeOutputFormat, date_time_output_format, FormatSettings::DateTimeOutputFormat::Simple, "Method to write DateTime to text output. Possible values: 'simple', 'iso', 'unix_timestamp'.", 0) \
    \
    M(UInt64, max_hdfs_write_buffer_size, DBMS_DEFAULT_BUFFER_SIZE, "The maximum size of the buffer to write data to hdfs.",0) \
    M(String, bool_true_representation, "1", "Text to represent bool value in TSV/CSV formats.", 0) \
    M(String, bool_false_representation, "0", "Text to represent bool value in TSV/CSV formats.", 0) \
    M(Bool, input_format_ipv4_default_on_conversion_error, false, "Deserialization of IPv4 will use default values instead of throwing exception on conversion error.", 0) \
    M(Bool, input_format_ipv6_default_on_conversion_error, false, "Deserialization of IPV6 will use default values instead of throwing exception on conversion error.", 0) \
    \
    M(Bool, input_format_values_interpret_expressions, true, "For Values format: if the field could not be parsed by streaming parser, run SQL parser and try to interpret it as SQL expression.", 0) \
    M(Bool, input_format_values_deduce_templates_of_expressions, true, "For Values format: if the field could not be parsed by streaming parser, run SQL parser, deduce template of the SQL expression, try to parse all rows using template and then interpret expression for all rows.", 0) \
    M(Bool, input_format_values_accurate_types_of_literals, true, "For Values format: when parsing and interpreting expressions using template, check actual type of literal to avoid possible overflow and precision issues.", 0) \
    M(Bool, input_format_avro_allow_missing_fields, false, "For Avro/AvroConfluent format: when field is not found in schema use default value instead of error", 0) \
    M(URI, format_avro_schema_registry_url, "", "For AvroConfluent format: Confluent Schema Registry URL.", 0) \
\
    M(Bool, output_format_json_quote_64bit_integers, false, "Controls quoting of 64-bit integers in JSON output format.", 0) \
\
    M(Bool, output_format_json_quote_denormals, false, "Enables '+nan', '-nan', '+inf', '-inf' outputs in JSON output format.", 0) \
\
    M(Bool, \
      output_format_json_escape_forward_slashes, \
      true, \
      "Controls escaping forward slashes for string outputs in JSON output format. This is intended for compatibility with JavaScript. " \
      "Don't confuse with backslashes that are always escaped.", \
      0) \
    M(Bool, output_format_json_named_tuples_as_objects, false, "Serialize named tuple columns as JSON objects.", 0) \
    M(Bool, output_format_json_array_of_rows, false, "Output a JSON array of all rows in JSONEachRow(Compact) format.", 0) \
\
    M(UInt64, output_format_pretty_max_rows, 10000, "Rows limit for Pretty formats.", 0) \
    M(UInt64, output_format_pretty_max_column_pad_width, 250, "Maximum width to pad all values in a column in Pretty formats.", 0) \
    M(UInt64, \
      output_format_pretty_max_value_width, \
      10000, \
      "Maximum width of value to display in Pretty formats. If greater - it will be cut.", \
      0) \
    M(Bool, output_format_pretty_color, true, "Use ANSI escape sequences to paint colors in Pretty formats", 0) \
    M(String, \
      output_format_pretty_grid_charset, \
      "UTF-8", \
      "Charset for printing grid borders. Available charsets: ASCII, UTF-8 (default one).", \
      0) \
    M(UInt64, output_format_parquet_row_group_size, 1000000, "Row group size in rows.", 0) \
    M(Bool, output_format_parquet_string_as_string, false, "Use Parquet String type instead of Binary for String columns.", 0) \
    M(Bool, output_format_parquet_fixed_string_as_fixed_byte_array, true, "Use Parquet FIXED_LENGTH_BYTE_ARRAY type instead of Binary for FixedString columns.", 0) \
    M(Bool, input_format_parquet_case_insensitive_column_matching, false, "Ignore case when matching Parquet columns with CH columns.", 0) \
    M(Bool, input_format_parquet_preserve_order, false, "Avoid reordering rows when reading from Parquet files. Usually makes it much slower.", 0) \
    M(Bool, input_format_parquet_filter_push_down, true, "When reading Parquet files, skip whole row groups based on the WHERE/PREWHERE expressions and min/max statistics in the Parquet metadata.", 0) \
    M(UInt64, input_format_parquet_max_block_size, 8192, "Max block size for parquet reader.", 0) \
    M(Bool, input_format_parquet_allow_missing_columns, false, "Allow missing columns while reading Parquet input formats", 0) \
    M(Bool, input_format_parquet_import_nested, false, "Allow to insert array of structs into Nested table in Parquet input format.", 0) \
    M(Bool, input_format_allow_seeks, true, "Allow seeks while reading in ORC/Parquet/Arrow input formats", 0) \
    M(Bool, input_format_arrow_avoid_buffering, true, "If ReadBuffer supports random read then avoid using buffer in arrow stream", 0) \
    M(Bool, input_format_orc_allow_missing_columns, false, "Allow missing columns while reading ORC input formats", 0) \
    M(Bool, input_format_arrow_import_nested, false, "Allow to insert array of structs into Nested table in Arrow input format.", 0) \
    M(Bool, input_format_orc_import_nested, false, "Allow to insert array of structs into Nested table in ORC input format.", 0) \
    M(Bool, input_format_arrow_allow_missing_columns, false, "Allow missing columns while reading Arrow input formats", 0) \
    M(String, output_format_avro_codec, "", "Compression codec used for output. Possible values: 'null', 'deflate', 'snappy'.", 0) \
    M(UInt64, output_format_avro_sync_interval, 16 * 1024, "Sync interval in bytes.", 0) \
    M(Bool, output_format_tsv_crlf_end_of_line, false, "If it is set true, end of line in TSV format will be \\r\\n instead of \\n.", 0) \
    M(String, output_format_tsv_null_representation, "\\N", "Custom NULL representation in TSV format", 0) \
    M(Bool, output_format_decimal_trailing_zeros, false, "Output trailing zeros when printing Decimal values. E.g. 1.230000 instead of 1.23.", 0) \
    \
    /** Settings for Map */ \
    M(Bool, input_format_parse_null_map_as_empty, true, "Parse null map as empty map. Throw exception if set false.", 0) \
    M(Bool, input_format_skip_null_map_value, true, "Skip null map value. Throw exception if set false.", 0) \
    M(UInt64, input_format_max_map_key_long, 80, "The maximum length of map key for input data.", 0) \
    \
    M(UInt64, input_format_allow_errors_num, 0, "Maximum absolute amount of errors while reading text formats (like CSV, TSV). In case of error, if at least absolute or relative amount of errors is lower than corresponding value, will skip until next line and continue.", 0) \
    M(Float, input_format_allow_errors_ratio, 0, "Maximum relative amount of errors while reading text formats (like CSV, TSV). In case of error, if at least absolute or relative amount of errors is lower than corresponding value, will skip until next line and continue.", 0) \
    \
    M(String, format_schema, "", "Schema identifier (used by schema-based formats)", 0) \
    M(String, format_custom_schema_path, "", "Support table-level path to schema file for multiple users (used by schema-based formats); distinguish from `format_schema_path`", 0) \
    M(String, format_template_resultset, "", "Path to file which contains format string for result set (for Template format)", 0) \
    M(String, format_template_row, "", "Path to file which contains format string for rows (for Template format)", 0) \
    M(String, format_template_rows_between_delimiter, "\n", "Delimiter between rows (for Template format)", 0) \
\
    M(String, format_custom_escaping_rule, "Escaped", "Field escaping rule (for CustomSeparated format)", 0) \
    M(String, format_custom_field_delimiter, "\t", "Delimiter between fields (for CustomSeparated format)", 0) \
    M(String, format_custom_row_before_delimiter, "", "Delimiter before field of the first column (for CustomSeparated format)", 0) \
    M(String, format_custom_row_after_delimiter, "\n", "Delimiter after field of the last column (for CustomSeparated format)", 0) \
    M(String, format_custom_row_between_delimiter, "", "Delimiter between rows (for CustomSeparated format)", 0) \
    M(String, format_custom_result_before_delimiter, "", "Prefix before result set (for CustomSeparated format)", 0) \
    M(String, format_custom_result_after_delimiter, "", "Suffix after result set (for CustomSeparated format)", 0) \
\
    M(String, format_regexp, "", "Regular expression (for Regexp format)", 0) \
    M(String, format_regexp_escaping_rule, "Raw", "Field escaping rule (for Regexp format)", 0) \
    M(Bool, format_regexp_skip_unmatched, false, "Skip lines unmatched by regular expression (for Regexp format", 0) \
\
    M(Bool, output_format_enable_streaming, false, "Enable streaming in output formats that support it.", 0) \
    M(Bool, output_format_write_statistics, true, "Write statistics about read rows, bytes, time elapsed in suitable output formats.", 0) \
    M(Bool, output_format_pretty_row_numbers, false, "Add row numbers before each row for pretty output format", 0) \
    M(Bool, \
      insert_distributed_one_random_shard, \
      false, \
      "If setting is enabled, inserting into distributed table will choose a random shard to write when there is no sharding key", \
      0) \
    M(Bool, cross_to_inner_join_rewrite, true, "Use inner join instead of comma/cross join if possible", 0) \
\
    M(Bool, output_format_arrow_low_cardinality_as_dictionary, false, "Enable output LowCardinality type as Dictionary Arrow type", 0) \
    M(Bool, output_format_arrow_string_as_string, false, "Use Arrow String type instead of Binary for String columns", 0) \
    M(Bool, output_format_arrow_fixed_string_as_fixed_byte_array, true, "Use Arrow FIXED_SIZE_BINARY type instead of Binary for FixedString columns.", 0) \
    M(Bool, output_format_orc_string_as_string, false, "Use ORC String type instead of Binary for String columns", 0) \
    M(Bool, enable_low_cardinality_merge_new_algo, true, "Whether use the new merge algorithm during part merge for low cardinality column", 0) \
    M(UInt64, low_cardinality_distinct_threshold, 100000, "Threshold for fallback to native column from low cardinality column, 0 disable", 0) \
    M(Bool, enable_zero_copy_read, false, "Whether enable zero copy read from cache, it requires uncompressed-cache to be enabled.", 0) \
    M(String, skip_shard_list, "", "Set slow shards that query want to skip, shard num is split by comma", 0) \
    \
    M(UInt64, cnch_background_task_part_load_max_seconds, 600, "Maximum seconds of part load for background tasks", 0)\
    M(UInt64, cnch_part_attach_limit, 3000, "Maximum number of part for ATTACH PARTITION/PARTS command", 0)\
    M(UInt64, cnch_part_attach_drill_down, 1, "Maximum levels of path to find cnch data parts, 0 means no drill down", 0) \
    M(UInt64, cnch_part_attach_assert_parts_count, 0, "Assert total number of parts to attach.", 0) \
    M(UInt64, cnch_part_attach_assert_rows_count, 0, "Assert totol number of part rows to attach.", 0) \
    M(UInt64, cnch_part_attach_max_source_discover_level, 1, "Maximum levels of drill down to lookup for different sources", 0) \
    M(UInt64, cnch_part_attach_max_threads, 16, "Max threads to use when attach parts", 0) \
    M(UInt64, attach_failure_injection_knob, 0, "Attach failure injection knob, for test only", 0) \
    M(Bool, skip_table_definition_hash_check, false, "Whether skip table definition hash check when attach data parts.", 0)  \
    M(Bool, allow_attach_parts_with_different_table_definition_hash, true, "Whether to allow attaching of parts with different table definition hash to target table.", 0)  \
    M(Bool, async_post_commit, true, "Txn post commit asynchronously", 0) \
    M(String, tenant_id, "", "tenant_id of cnch user", 0) \
    M(Bool, cnch_enable_merge_prefetch, true, "Enable prefetching while merge", 0) \
    M(UInt64, cnch_merge_prefetch_segment_size, 256 * 1024 * 1024, "Min segment size of file when prefetching for merge", 0) \
    M(Bool, offloading_with_query_plan, false, "utilize query plan to offload the computation completely to worker", 0) \
    M(Seconds, access_entity_ttl, 60 * 60, "TTL for access entities stored in memory in seconds", 0) \
\
    M(Bool, debug_enable_early_partition_pruning, false, "Whether we can prune parts via query filter and partition value", 0) \
    \
    M(String, s3_ak_id, "", "The access_key set by user when accessing ve s3.", 0) \
    M(String, s3_ak_secret, "", "The secret_key set by user when accessing ve s3.", 0) \
    M(String, s3_region, "", "The region set by user when accessing ve s3.", 0) \
    M(String, s3_endpoint, "", "The endpoint set by user when accessing ve s3.", 0) \
    M(Bool, s3_use_virtual_hosted_style, false, "is virtual hosted style or not, default false", 0) \
    \
    M(Bool, enable_cache_reader_buffer_reuse, false, "Decpreated settings, only a place holder", 0) \
    M(Bool, enable_auto_query_forwarding, true, "Auto forward query to target server when having multiple servers", 0) \
    M(Bool, enable_select_query_forwarding, false, "Auto forward select query to target server when having multiple servers", 0) \
    \
    M(Bool, merge_partition_stats,false, "merge all partition stats", 0) \
    M(Bool, enable_three_part_identifier, true, "merge all partition stats", 0) \
    M(String, default_catalog, "", "current catalog", 0) \
    \
    /** BitEngine related settings */ \
    M(Bool, use_encoded_bitmap, true, "Whether to read the encoded bitmap column", 0) \
    M(Bool, enable_parallel_bitengine_recode, false, "Whether to encode bitengine in parallel.", 0)  \
    M(Bool, bitengine_encode_without_lock, false, "Whether to encode bitengine without lock.", 0) \
    M(Bool, bitengine_encode_fast_mode, false, "Whether to encode bitengine in parallel but with a ZooKeeper lock. It's used for BitEngineDictionary::encodeFast.", 0) \
    M(UInt64, max_parallel_threads_for_bitengine_recode, 10, "The maximum number of threads to recode bitengine parts", 0) \
    M(Bool, decode_with_merged_dict, true, "Decode column by merging all bucket dicts", 0) \
    M(Bool, encode_with_splitted_columns, true, "Decode column by merging all bucket dicts", 0) \
    M(Bool, enable_parallel_load_dict, true, "Read dict file in parallel", 0) \
    M(Bool, load_dict_from_cache, true, "Read dict from cache", 0) \
    M(Bool, throw_exception_if_bucket_unmatched, false, "Whether to throw exception if bucket is unmatched when send bitengine resource", 0) \
    M(Bool, enable_cnch_engine_conversion, false, "Whether to converse MergeTree engine to CnchMergeTree engine", 0) \
    /** End of BitEngine related settings */ \
    \

// End of FORMAT_FACTORY_SETTINGS

// Please add settings non-related to formats into the COMMON_SETTINGS above.
// Please add settings for compatible into OBSOLETE_SETTINGS above. And add FORMAT_FACTORY_OBSOLETE_SETTINGS if you needed.

#define LIST_OF_SETTINGS(M)    \
    COMMON_SETTINGS(M)         \
    OBSOLETE_SETTINGS(M)       \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS_ALLOW_CUSTOM_SETTINGS(SettingsTraits, LIST_OF_SETTINGS)


/** Settings of query execution.
  * These settings go to users.xml.
  */
struct Settings : public BaseSettings<SettingsTraits>
{
    /// For initialization from empty initializer-list to be "value initialization", not "aggregate initialization" in C++14.
    /// http://en.cppreference.com/w/cpp/language/aggregate_initialization
    Settings() = default;

    /** Set multiple settings from "profile" (in server configuration file (users.xml), profiles contain groups of multiple settings).
     * The profile can also be set using the `set` functions, like the profile setting.
     */
    void setProfile(const String & profile_name, const Poco::Util::AbstractConfiguration & config);

    /// Load settings from configuration file, at "path" prefix in configuration.
    void loadSettingsFromConfig(const String & path, const Poco::Util::AbstractConfiguration & config);

    /// Dumps profile events to column of type Map(String, String)
    void dumpToMapColumn(IColumn * column, bool changed_only = true);

    /// Adds program options to set the settings from a command line.
    /// (Don't forget to call notify() on the `variables_map` after parsing it!)
    void addProgramOptions(boost::program_options::options_description & options);

    /// Check that there is no user-level settings at the top level in config.
    /// This is a common source of mistake (user don't know where to write user-level setting).
    static void checkNoSettingNamesAtTopLevel(const Poco::Util::AbstractConfiguration & config, const String & config_path);

    /// Get all changed settings
    SettingsChanges getChangedSettings() const;
    /// save settings changed to json
    void dumpToJSON(Poco::JSON::Object & dumpJson) const;
};

/*
 * User-specified file format settings for File and URL engines.
 */
DECLARE_SETTINGS_TRAITS(FormatFactorySettingsTraits, FORMAT_FACTORY_SETTINGS)

struct FormatFactorySettings : public BaseSettings<FormatFactorySettingsTraits>
{
};

}
