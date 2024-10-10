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

#include <Core/Defines.h>
#include <Core/BaseSettings.h>
#include <Core/SettingsEnums.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}


namespace DB
{
class ASTStorage;
struct Settings;

enum StealingCacheMode : UInt64
{
  DISABLE = 0,
  READ_ONLY = 1,
  WRITE_ONLY = 2,
  READ_WRITE = 3
};

/** These settings represent fine tunes for internal details of MergeTree storages
  * and should not be changed by the user without a reason.
  */

#define LIST_OF_MERGE_TREE_SETTINGS(M) \
    M(UInt64, \
      min_compress_block_size, \
      0, \
      "When granule is written, compress the data in buffer if the size of pending uncompressed data is larger or equal than the " \
      "specified threshold. If this setting is not set, the corresponding global setting is used.", \
      0) \
    M(UInt64, \
      max_compress_block_size, \
      0, \
      "Compress the pending uncompressed data in buffer if its size is larger or equal than the specified threshold. Block of data will " \
      "be compressed even if the current granule is not finished. If this setting is not set, the corresponding global setting is used.", \
      0) \
    M(UInt64, index_granularity, 8192, "How many rows correspond to one primary key value.", 0) \
    M(UInt64, max_digestion_size_per_segment, 256 * 1024 * 1024, "Max number of bytes to digest per segment to build GIN index.", 0) \
    \
    /** Data storing format settings. */ \
    /* For backward compatibility, do not use compact part */ \
    M(UInt64, \
      min_bytes_for_wide_part, \
      /*10485760*/ 0, \
      "Minimal uncompressed size in bytes to create part in wide format instead of compact", \
      0) \
    M(UInt64, min_rows_for_wide_part, 0, "Minimal number of rows to create part in wide format instead of compact", 0) \
    M(UInt64, \
      min_bytes_for_compact_part, \
      0, \
      "Experimental. Minimal uncompressed size in bytes to create part in compact format instead of saving it in RAM", \
      0) \
    M(UInt64, \
      min_rows_for_compact_part, \
      0, \
      "Experimental. Minimal number of rows to create part in compact format instead of saving it in RAM", \
      0) \
    M(Bool, \
      in_memory_parts_enable_wal, \
      true, \
      "Whether to write blocks in Native format to write-ahead-log before creation in-memory part", \
      0) \
    M(UInt64, write_ahead_log_max_bytes, 1024 * 1024 * 1024, "Rotate WAL, if it exceeds that amount of bytes", 0) \
    M(Bool, reorganize_marks_data_layout, false, "Whether to use the data layout of concentrated marks for cnch part", 0) \
    \
    /** Merge settings. */ \
    M(UInt64, merge_max_block_size, DEFAULT_MERGE_BLOCK_SIZE, "How many rows in blocks should be formed for merge operations, By default has the same value as `index_granularity`.", 0) \
    M(UInt64, merge_max_block_size_bytes, 10 * 1024 * 1024, "How many bytes in blocks should be formed for merge operations. By default has the same value as `index_granularity_bytes`.", 0) \
    M(UInt64, max_bytes_to_merge_at_max_space_in_pool, 150ULL * 1024 * 1024 * 1024, "Maximum in total size of parts to merge, when there are maximum free threads in background pool (or entries in replication queue).", 0) \
    M(UInt64, max_bytes_to_merge_at_min_space_in_pool, 1024 * 1024, "Maximum in total size of parts to merge, when there are minimum free threads in background pool (or entries in replication queue).", 0) \
    M(UInt64, max_replicated_merges_in_queue, 16, "How many tasks of merging and mutating parts are allowed simultaneously in ReplicatedMergeTree queue.", 0) \
    M(UInt64, max_replicated_mutations_in_queue, 8, "How many tasks of mutating parts are allowed simultaneously in ReplicatedMergeTree queue.", 0) \
    M(UInt64, max_replicated_merges_with_ttl_in_queue, 1, "How many tasks of merging parts with TTL are allowed simultaneously in ReplicatedMergeTree queue.", 0) \
    M(UInt64, number_of_free_entries_in_pool_to_lower_max_size_of_merge, 8, "When there is less than specified number of free entries in pool (or replicated queue), start to lower maximum size of merge to process (or to put in queue). This is to allow small merges to process - not filling the pool with long running merges.", 0) \
    M(UInt64, number_of_free_entries_in_pool_to_execute_mutation, 10, "When there is less than specified number of free entries in pool, do not execute part mutations. This is to leave free threads for regular merges and avoid \"Too many parts\"", 0) \
    M(UInt64, max_number_of_merges_with_ttl_in_pool, 2, "When there is more than specified number of merges with TTL entries in pool, do not assign new merge with TTL. This is to leave free threads for regular merges and avoid \"Too many parts\"", 0) \
    /** We implement 2-phase part GC. `old_parts_lifetime` is the ttl for part meta in phase-1, and `ttl_for_trash_items` is the ttl for part file in phase-2. For old_parts_lifetime, a small value can speedup query by scanning less parts, and a bigger value means a safer strategy to recover data. */ \
    M(Seconds, old_parts_lifetime, 30 * 60, "How many seconds to keep obsolete parts before marking them as trash keys.", 0) \
    M(Seconds, ttl_for_trash_items, 3 * 3600, "How many additional seconds to wait before actual data removal for trash keys", 0) \
    M(Seconds, temporary_directories_lifetime, 86400, "How many seconds to keep tmp_-directories. You should not lower this value because merges and mutations may not be able to work with low value of this setting.", 0) \
    M(Seconds, lock_acquire_timeout_for_background_operations, DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC, "For background operations like merges, mutations etc. How many seconds before failing to acquire table locks.", 0) \
    M(UInt64, min_rows_to_fsync_after_merge, 0, "Minimal number of rows to do fsync for part after merge (0 - disabled)", 0) \
    M(UInt64, \
      min_compressed_bytes_to_fsync_after_merge, \
      0, \
      "Minimal number of compressed bytes to do fsync for part after merge (0 - disabled)", \
      0) \
    M(UInt64, \
      min_compressed_bytes_to_fsync_after_fetch, \
      0, \
      "Minimal number of compressed bytes to do fsync for part after fetch (0 - disabled)", \
      0) \
    M(Bool, \
      fsync_after_insert, \
      false, \
      "Do fsync for every inserted part. Significantly decreases performance of inserts, not recommended to use with wide parts.", \
      0) \
    M(Bool, fsync_part_directory, false, "Do fsync for part directory after all part operations (writes, renames, etc.).", 0) \
    M(UInt64, write_ahead_log_bytes_to_fsync, 100ULL * 1024 * 1024, "Amount of bytes, accumulated in WAL to do fsync.", 0) \
    M(UInt64, write_ahead_log_interval_ms_to_fsync, 100, "Interval in milliseconds after which fsync for WAL is being done.", 0) \
    M(Bool, in_memory_parts_insert_sync, false, "If true insert of part with in-memory format will wait for fsync of WAL", 0) \
    M(UInt64, non_replicated_deduplication_window, 0, "How many last blocks of hashes should be kept on disk (0 - disabled).", 0) \
    M(UInt64, \
      max_parts_to_merge_at_once, \
      100, \
      "Max amount of parts which can be merged at once (0 - disabled). Doesn't affect OPTIMIZE FINAL query.", \
      0) \
    M(UInt64, gc_remove_bitmap_batch_size, 1000, "Submit a batch of bitmaps to a background thread", 0) \
    M(UInt64, gc_remove_bitmap_thread_pool_size, 16, "Turn up the thread pool size to speed up GC processing of bitmaps", 0) \
    M(UInt64, gc_partition_batch_size, 100, "The batch size for partition meta GC", 0) \
    M(Seconds, gc_partition_lifetime_before_remove, 30 * 60, "The duration between mark partition deleted and remove from metastore finally", 0) \
    \
    M(UInt64, max_refresh_materialized_view_task_num, 10, "Max threads to refresh for each materialized view.", 0) \
    \
    /** Inserts settings. */ \
    M(UInt64, parts_to_delay_insert, 0, "If table contains at least that many active parts in single partition, artificially slow down insert into table.", 0) \
    M(UInt64, inactive_parts_to_delay_insert, 0, "If table contains at least that many inactive parts in single partition, artificially slow down insert into table.", 0) \
    M(UInt64, parts_to_throw_insert, 0, "If more than this number active parts in single partition, throw 'Too many parts ...' exception.", 0) \
    M(UInt64, inactive_parts_to_throw_insert, 0, "If more than this number inactive parts in single partition, throw 'Too many inactive parts ...' exception.", 0) \
    M(UInt64, max_delay_to_insert, 1, "Max delay of inserting data into MergeTree table in seconds, if there are a lot of unmerged parts in single partition.", 0) \
    M(UInt64, max_parts_in_total, 0, "If more than this number active parts in all partitions in total, throw 'Too many parts ...' exception.", 0) \
    M(UInt64, max_partitions_per_insert_block, 100, "Table level setting; The same as max_partitions_per_insert_block in Core/Settings", 0) \
    M(UInt64, insert_check_parts_interval, 60, "Check the parts number to delay or block insert with such a interval(in Seconds)", 0) \
    \
    /** Replication settings. */ \
    M(UInt64, \
      replicated_deduplication_window, \
      100, \
      "How many last blocks of hashes should be kept in ZooKeeper (old blocks will be deleted).", \
      0) \
    M(UInt64, \
      replicated_deduplication_window_seconds, \
      7 * 24 * 60 * 60 /* one week */, \
      "Similar to \"replicated_deduplication_window\", but determines old blocks by their lifetime. Hash of an inserted block will be " \
      "deleted (and the block will not be deduplicated after) if it outside of one \"window\". You can set very big " \
      "replicated_deduplication_window to avoid duplicating INSERTs during that period of time.", \
      0) \
    M(UInt64, \
      max_replicated_logs_to_keep, \
      1000, \
      "How many records may be in log, if there is inactive replica. Inactive replica becomes lost when when this number exceed.", \
      0) \
    M(UInt64, \
      min_replicated_logs_to_keep, \
      10, \
      "Keep about this number of last records in ZooKeeper log, even if they are obsolete. It doesn't affect work of tables: used only " \
      "to diagnose ZooKeeper log before cleaning.", \
      0) \
    M(Seconds, \
      prefer_fetch_merged_part_time_threshold, \
      3600, \
      "If time passed after replication log entry creation exceeds this threshold and sum size of parts is greater than " \
      "\"prefer_fetch_merged_part_size_threshold\", prefer fetching merged part from replica instead of doing merge locally. To speed up " \
      "very long merges.", \
      0) \
    M(UInt64, \
      prefer_fetch_merged_part_size_threshold, \
      10ULL * 1024 * 1024 * 1024, \
      "If sum size of parts exceeds this threshold and time passed after replication log entry creation is greater than " \
      "\"prefer_fetch_merged_part_time_threshold\", prefer fetching merged part from replica instead of doing merge locally. To speed up " \
      "very long merges.", \
      0) \
    M(Seconds, \
      execute_merges_on_single_replica_time_threshold, \
      0, \
      "When greater than zero only a single replica starts the merge immediately, others wait up to that amount of time to download the " \
      "result instead of doing merges locally. If the chosen replica doesn't finish the merge during that amount of time, fallback to " \
      "standard behavior happens.", \
      0) \
    M(Seconds, \
      remote_fs_execute_merges_on_single_replica_time_threshold, \
      3 * 60 * 60, \
      "When greater than zero only a single replica starts the merge immediatelys when merged part on shared storage and " \
      "'allow_remote_fs_zero_copy_replication' is enabled.", \
      0) \
    M(Seconds, \
      try_fetch_recompressed_part_timeout, \
      7200, \
      "Recompression works slow in most cases, so we don't start merge with recompression until this timeout and trying to fetch " \
      "recompressed part from replica which assigned this merge with recompression.", \
      0) \
    M(Bool, always_fetch_merged_part, 0, "If true, replica never merge parts and always download merged parts from other replicas.", 0) \
    M(UInt64, max_suspicious_broken_parts, 10, "Max broken parts, if more - deny automatic deletion.", 0) \
    M(UInt64, \
      max_files_to_modify_in_alter_columns, \
      75, \
      "Not apply ALTER if number of files for modification(deletion, addition) more than this.", \
      0) \
    M(UInt64, max_files_to_remove_in_alter_columns, 50, "Not apply ALTER, if number of files for deletion more than this.", 0) \
    M(Float, \
      replicated_max_ratio_of_wrong_parts, \
      0.5, \
      "If ratio of wrong parts to total number of parts is less than this - allow to start.", \
      0) \
    M(UInt64, replicated_max_parallel_fetches, 0, "Limit parallel fetches.", 0) \
    M(UInt64, replicated_max_parallel_fetches_for_table, 0, "Limit parallel fetches for one table.", 0) \
    M(UInt64, \
      replicated_max_parallel_fetches_for_host, \
      DEFAULT_COUNT_OF_HTTP_CONNECTIONS_PER_ENDPOINT, \
      "Limit parallel fetches from endpoint (actually pool size).", \
      0) \
    M(UInt64, replicated_max_parallel_sends, 0, "Limit parallel sends.", 0) \
    M(UInt64, replicated_max_parallel_sends_for_table, 0, "Limit parallel sends for one table.", 0) \
    M(Seconds, \
      replicated_fetches_http_connection_timeout, \
      0, \
      "HTTP connection timeout for part fetch requests. Inherited from default profile `http_connection_timeout` if not set explicitly.", \
      0) \
    M(Seconds, \
      replicated_fetches_http_send_timeout, \
      0, \
      "HTTP send timeout for part fetch requests. Inherited from default profile `http_send_timeout` if not set explicitly.", \
      0) \
    M(Seconds, \
      replicated_fetches_http_receive_timeout, \
      0, \
      "HTTP receive timeout for fetch part requests. Inherited from default profile `http_receive_timeout` if not set explicitly.", \
      0) \
    M(Bool, replicated_can_become_leader, true, "If true, Replicated tables replicas on this node will try to acquire leadership.", 0) \
    M(Seconds, zookeeper_session_expiration_check_period, 60, "ZooKeeper session expiration check period, in seconds.", 0) \
    M(Bool, detach_old_local_parts_when_cloning_replica, 1, "Do not remove old local parts when repairing lost replica.", 0) \
    M(UInt64, \
      max_replicated_fetches_network_bandwidth, \
      0, \
      "The maximum speed of data exchange over the network in bytes per second for replicated fetches. Zero means unlimited.", \
      0) \
    M(UInt64, \
      max_replicated_sends_network_bandwidth, \
      0, \
      "The maximum speed of data exchange over the network in bytes per second for replicated sends. Zero means unlimited.", \
      0) \
\
    /** Check delay of replicas settings. */ \
    M(UInt64, min_relative_delay_to_measure, 120, "Calculate relative replica delay only if absolute delay is not less that this value.", 0) \
    M(UInt64, cleanup_delay_period, 30, "Sleep interval between each scan for phase-two GC. (in seconds)", 0) \
    M(UInt64, cleanup_delay_period_upper_bound, 15 * 60, "Max sleep interval for phase-two GC when are no items to delete in a round. (in seconds)", 0) \
    M(UInt64, cleanup_delay_period_random_add, 10, "Add uniformly distributed value from 0 to x seconds to cleanup_delay_period to avoid thundering herd effect and subsequent DoS of ZooKeeper in case of very large number of tables.", 0) \
    M(UInt64, min_relative_delay_to_close, 300, "Minimal delay from other replicas to close, stop serving requests and not return Ok during status check.", 0) \
    M(UInt64, min_absolute_delay_to_close, 0, "Minimal absolute delay to close, stop serving requests and not return Ok during status check.", 0) \
    M(UInt64, enable_vertical_merge_algorithm, 1, "Enable usage of Vertical merge algorithm.", 0) \
    M(UInt64, vertical_merge_algorithm_min_rows_to_activate, 16 * DEFAULT_MERGE_BLOCK_SIZE, "Minimal (approximate) sum of rows in merging parts to activate Vertical merge algorithm.", 0) \
    M(UInt64, vertical_merge_algorithm_min_columns_to_activate, 11, "Minimal amount of non-PK columns to activate Vertical merge algorithm.", 0) \
    M(String, cluster_by_hint, "", "same as cluster by in ddl, but not enforced, user ensure data is correct.", 0)                                  \
    M(Int64, partition_by_monotonicity_hint, 0, "Hint on whether partition by expression is a monotonic function or not, e.g., '(toYYYYMMDD(ts), toHour(ts))' is a monotonic non-decreasing function. 0 means unknown, Positive means monotonic non-decrasing, Negative means monotonic non-increasing", 0) \
    /** Compatibility settings */ \
    M(Bool, \
      compatibility_allow_sampling_expression_not_in_primary_key, \
      false, \
      "Allow to create a table with sampling expression not in primary key. This is needed only to temporarily allow to run the server " \
      "with wrong tables for backward compatibility.", \
      0) \
    M(Bool, \
      use_minimalistic_checksums_in_zookeeper, \
      true, \
      "Use small format (dozens bytes) for part checksums in ZooKeeper instead of ordinary ones (dozens KB). Before enabling check that " \
      "all replicas support new format.", \
      0) \
    M(Bool, \
      use_minimalistic_part_header_in_zookeeper, \
      true, \
      "Store part header (checksums and columns) in a compact format and a single part znode instead of separate znodes (<part>/columns " \
      "and <part>/checksums). This can dramatically reduce snapshot size in ZooKeeper. Before enabling check that all replicas support " \
      "new format.", \
      0) \
    M(UInt64, \
      finished_mutations_to_keep, \
      100, \
      "How many records about mutations that are done to keep. If zero, then keep all of them.", \
      0) \
    M(UInt64, \
      min_merge_bytes_to_use_direct_io, \
      10ULL * 1024 * 1024 * 1024, \
      "Minimal amount of bytes to enable O_DIRECT in merge (0 - disabled).", \
      0) \
    M(UInt64, index_granularity_bytes, /*10 * 1024 * 1024*/ 0, "Approximate amount of bytes in single granule (0 - disabled).", 0) \
    M(UInt64, min_index_granularity_bytes, 1024, "Minimum amount of bytes in single granule.", 1024) \
    M(Int64, merge_with_ttl_timeout, 3600 * 4, "Minimal time in seconds, when merge with delete TTL can be repeated.", 0) \
    M(Int64, \
      merge_with_recompression_ttl_timeout, \
      3600 * 4, \
      "Minimal time in seconds, when merge with recompression TTL can be repeated.", \
      0) \
    M(Bool, ttl_only_drop_parts, false, "Only drop altogether the expired parts and not partially prune them.", 0) \
    M(Bool, enable_partition_ttl_fallback, true, "When TTL expression doesn't match partition expression, Try to calculate partition's TTL value and mark expired partitions by scanning parts' ttl info", 0) \
    \
    M(Bool, write_final_mark, 1, "Write final mark after end of column (0 - disabled, do nothing if index_granularity_bytes=0)", 0) \
    M(Bool, enable_mixed_granularity_parts, 1, "Enable parts with adaptive and non adaptive granularity", 0) \
    M(MaxThreads, max_part_loading_threads, 0, "The number of threads to load data parts at startup.", 0) \
    M(MaxThreads, \
      max_part_removal_threads, \
      0, \
      "The number of threads for concurrent removal of inactive data parts. One is usually enough, but in 'Google Compute Environment " \
      "SSD Persistent Disks' file removal (unlink) operation is extraordinarily slow and you probably have to increase this number " \
      "(recommended is up to 16).", \
      0) \
    M(UInt64, \
      concurrent_part_removal_threshold, \
      100, \
      "Activate concurrent part removal (see 'max_part_removal_threads') only if the number of inactive data parts is at least this.", \
      0) \
    M(String, storage_policy, "default", "Name of storage disk policy", 0) \
    M(Bool, allow_nullable_key, false, "Allow Nullable types as primary keys.", 0) \
    M(Bool, extract_partition_nullable_date, false, "Extract date value from Nullable Date column when allow_nullable_key is true", 0) \
    M(Bool, allow_remote_fs_zero_copy_replication, false, "Allow Zero-copy replication over remote fs", 0) \
    M(Bool, remove_empty_parts, true, "Remove empty parts after they were pruned by TTL, mutation, or collapsing merge algorithm", 0) \
    M(Bool, assign_part_uuids, false, "Generate UUIDs for parts. Before enabling check that all replicas support new format.", 0) \
    M(Int64, \
      max_partitions_to_read, \
      -1, \
      "Limit the max number of partitions that can be accessed in one query. <= 0 means unlimited. This setting is the default that can " \
      "be overridden by the query-level setting with the same name.", \
      0) \
    M(UInt64, \
      max_concurrent_queries, \
      0, \
      "Max number of concurrently executed queries related to the MergeTree table (0 - disabled). Queries will still be limited by other " \
      "max_concurrent_queries settings.", \
      0) \
    M(UInt64, \
      min_marks_to_honor_max_concurrent_queries, \
      0, \
      "Minimal number of marks to honor the MergeTree-level's max_concurrent_queries (0 - disabled). Queries will still be limited by " \
      "other max_concurrent_queries settings.", \
      0) \
    M(UInt64, \
      min_bytes_to_rebalance_partition_over_jbod, \
      0, \
      "Minimal amount of bytes to enable part rebalance over JBOD array (0 - disabled).", \
      0) \
    M(Bool, enable_compact_map_data, false, "Enable usage of compact map impl, this parameter is invalid in replicated engine", 0) \
    /** Experimental/work in progress feature. Unsafe for production. */ \
    M(UInt64, \
      part_moves_between_shards_enable, \
      0, \
      "Experimental/Incomplete feature to move parts between shards. Does not take into account sharding expressions.", \
      0) \
    M(UInt64, part_moves_between_shards_delay_seconds, 30, "Time to wait before/after moving parts between shards.", 0) \
\
    /** ByteDance settings */ \
    \
    M(Bool, enable_build_ab_index, true, "", 0) \
    M(Bool, enable_segment_bitmap_index, true, "", 0) \
    M(Bool, build_bitmap_index_in_merge, true, "", 0) \
    M(Bool, build_segment_bitmap_index_in_merge, true, "", 0) \
    M(UInt64, bitmap_index_segment_granularity, 65536, "", 0) \
    M(UInt64, bitmap_index_serializing_granularity, 65536, "", 0) \
    M(UInt64, max_parallel_threads_for_bitmap, 16, "", 0) \
    M(Bool, enable_run_optimization, true, "", 0) \
    M(UInt64, delta_merge_interval, 60, "", 0) \
    M(Bool, enable_publish_version_on_commit, false, "Experimental. New table version is published sequentially when commit happens. ", 0) \
    M(Seconds, checkpoint_delay_period, 120, "Interval between two rounds of checkpoint tasks in Checkpoint BG thread", 0) \
    M(Seconds, manifest_list_lifetime, 1800, "Lifetime of the manifest list. Oudated manifests can be cleaned by Checkpoint GC thread", 0) \
    M(UInt64, max_active_manifest_list, 10, "Max number of new manifest list before trigger one checkpoint task.", 0) \
    M(Bool, as_main_table, false, "Always being main table for query forwarding when there are multiple tables in the query", 0) \
    /** Minimal amount of bytes to enable O_DIRECT in merge (0 - disabled, "", 0) */                                 \
    \
    /** If true, replicated tables would prefer to merge locally rather than                                  |\
      * fetching of merged part from replica */                                                               \
    M(Bool, prefer_merge_than_fetch, false, "", 0) \
    M(Bool, heuristic_part_source_replica_lookup, true, "", 0) \
    /** Using in ingest partition function. If true, we'll insert default when                                |\
      * the user have not provided values for some rows of a column */ \
    M(Bool, ingest_default_column_value_if_not_provided, true, "", 0) \
    M(Bool, enable_ingest_wide_part, false, "", 0) \
    /** detach partition lightweight rename directory instead of makeClone */ \
    M(Bool, light_detach_partition, false, "", 0) \
    M(Bool, ha_fast_create_table, false, "", 0) \
    M(UInt64, zk_local_diff_threshold, 12, "", 0) \
    M(Bool, only_use_ttl_of_metadata, true, "", 0) \
\
    M(Bool, enable_download_partition, false, "", 0) \
    M(UInt64, max_memory_usage_for_merge, 0, "", 0) \
    M(String, storage_policy_name, "default", "", 0) \
    M(Bool, offline_overwrite_realtime, 0, "", 0) \
    M(Bool, enable_async_init_metasotre, false, "", 0) \
    M(Bool, cnch_temporary_table, false, "", 0) \
    M(MaxThreads, cnch_parallel_prefetching, 0, "", 0) \
    M(Bool, enable_prefetch_checksums, true, "", 0) \
    M(Bool, enable_calculate_columns_size_with_sample, 1, "", 0) \
    M(Bool, enable_calculate_columns_size_without_map, 1, "", 0) \
                                                                                                              \
    M(Bool, disable_block_output, false, "", 0) \
    M(UInt64, min_drop_ranges_to_enable_cleanup, 365, "", 0) \
    M(Seconds, drop_ranges_lifetime, 60 * 60 * 36, "", 0) \
\
    M(String, cnch_vw_default, "vw_default", "", 0) \
    M(String, cnch_vw_read, "vw_read", "", 0) \
    M(String, cnch_vw_write, "vw_write", "", 0) \
    M(String, cnch_vw_task, "vw_task", "", 0) \
    M(String, cnch_server_vw, DEFAULT_SERVER_VW_NAME, "", 0) \
    \
    M(UInt64, insertion_label_ttl, 8400 * 2, "", 0) \
    \
    M(Bool, cnch_merge_enable_batch_select, true, "", 0)                                                     \
    M(Bool, enable_addition_bg_task, false, "", 0) \
    M(UInt64, max_addition_bg_task_num, 32, "", 0) \
    M(Int64, max_addition_mutation_task_num, 10, "", 0) \
    M(UInt64, max_partition_for_multi_select, 10, "", 0) \
    \
    /** Settings for parts cache on server for MergeTasks. Cache speed up the task scheduling. */             \
    M(UInt64, cnch_merge_parts_cache_timeout, 10 * 60, "", 0)                                  \
    M(UInt64, cnch_merge_parts_cache_min_count, 1000, "", 0)                                                  \
    M(UInt64, cnch_merge_max_total_rows_to_merge, 50000000, "", 0) \
    M(UInt64, cnch_merge_max_total_bytes_to_merge, 150ULL * 1024 * 1024 * 1024, "", 0) \
    M(UInt64, cnch_merge_max_parts_to_merge, 100, "", 0) \
    M(Int64, cnch_merge_expected_parts_number, 0, "Expected part numbers per partition, used to control merge selecting frequency and task size. 0 means using worker numbers in vw settings, negative value means disable this feature.", 0) \
    M(UInt64, cnch_mutate_max_parts_to_mutate, 100, "", 0) \
    M(UInt64, cnch_mutate_max_total_bytes_to_mutate, 50UL * 1024 * 1024 * 1024, "", 0) \
    \
    /* Unique table related settings */\
    M(Bool, cloud_enable_staging_area, false, "", 0) \
    M(Bool, cloud_enable_dedup_worker, false, "", 0) \
    M(UInt64, dedup_worker_max_heartbeat_interval, 16, "", 0) \
    M(Bool, partition_level_unique_keys, true, "", 0) \
    M(UInt64, staged_part_lifetime_threshold_ms_to_block_kafka_consume, 10000, "", 0) \
    M(Seconds, unique_acquire_write_lock_timeout, 300, "It has lower priority than session setting. Only when session setting is zero, use this setting", 0) \
    M(MaxThreads, cnch_parallel_dumping_threads, 8, "", 0) \
    M(MaxThreads, unique_table_dedup_threads, 8, "", 0) \
    M(Seconds, dedup_worker_progress_log_interval, 120, "", 0) \
    M(UInt64, max_delete_bitmap_meta_depth, 10, "", 0) \
    M(UInt64, enable_delete_mutation_on_unique_table, false, "Allow to run delete mutation on unique table. It's disabled by default and DELETE FROM is recommended for unique table.", 0) \
    M(UInt64, unique_merge_acquire_lock_retry_time, 10, "", 0) \
    M(Bool, enable_bucket_level_unique_keys, false, "", 0) \
    M(MaxThreads, cnch_write_part_threads, 1, "", 0) \
    M(UInt64, max_dedup_worker_number, 1, "", 0) \
    M(UInt64, max_staged_part_number_per_task, 100, "", 0) \
    M(UInt64, max_staged_part_rows_per_task, 15000000, "", 0) \
    M(Bool, enable_duplicate_check_while_writing, true, "Whether to check duplicate keys while writing for unique table. Although turning it on may have a certain impact on the tps of writing, it is recommended to enable it by default.", 0) \
    M(Bool, check_duplicate_key, false, "Whether to check duplicate keys using query. We execute the check query on the server, which may bring certain query overhead.", 0) \
    M(Bool, check_duplicate_for_big_table, false, "This takes effect when check_duplicate_key=1. Dedup grans will be checked one by one.", 0) \
    M(String, check_predicate, "", "This takes effect when check_duplicate_key=1. To avoid checking the history partitions multiple times.", 0) \
    M(Seconds, check_duplicate_key_interval, 3600, "Interval of check duplicate key", 0) \
    M(Bool, duplicate_auto_repair, false, "Whether to automatically repair duplicate keys. This process is on the worker, but it may affect other writes as the lock is held.", 0) \
    M(Seconds, duplicate_repair_interval, 600, "Interval of check duplicate key", 0) \
    M(Bool, enable_unique_partial_update, false, "Enable partial update", 0) \
    M(Bool, enable_unique_row_store, false, "TODO: support further to enhance point query perf", 0) \
    M(MaxThreads, partial_update_query_parts_thread_size, 8, "The thread size of query data parts.", 0) \
    M(MaxThreads, partial_update_query_columns_thread_size, 1, "The thread size of query columns for each part.", 0) \
    M(MaxThreads, partial_update_replace_columns_thread_size, 8, "The thread size of replace columns.", 0) \
    M(Bool, partial_update_enable_merge_map, true, "Map row will just replace the original one when it's false. Otherwise, it will merge row.", 0) \
    M(Bool, partial_update_optimize_for_batch_task, true, "Optimize partial update process when _update_columns_ are all same for batch processing.", 0) \
    M(DedupImplVersion, dedup_impl_version, DedupImplVersion::DEDUP_IN_WRITE_SUFFIX, "Choose different dedup impl version for unique table write process, current valid values: DEDUP_IN_WRITE_SUFFIX, DEDUP_IN_TXN_COMMIT.", 0) \
    M(DedupPickWorkerAlgo, dedup_pick_worker_algo, DedupPickWorkerAlgo::CONSISTENT_HASH, "", 0) \
    /** CI settings || test settings **/               \
    M(Bool, disable_dedup_parts, false, "Whether block the actual dedup progress.", 0) \
    M(Bool, partial_update_detail_logging, false, "Whether print some detailed troubleshooting information, only used for test scenarios.", 0) \
    M(Bool, pick_first_worker_to_dedup, false, "[deprecated, use dedup_pick_worker_algo] Whether always pick the first worker(for dedup stage) in vw for stress test.", 0) \
    \
    /* Metastore settings */\
    M(Bool, enable_metastore, false, "Use KV metastore to manage data parts.", 0) \
    M(Bool, enable_persistent_checksum, true, "[Deprecated] Persist checksums of part in memory. If set to false, checksums will be managed by a global cache to save memory.", 0) \
    /* Early materialze settings */\
    M(Bool, enable_late_materialize, false, "Use early materialize pipelined instead of prewhere while reading from merge tree", 0) \
    /** Obsolete settings. Kept for backward compatibility only. */ \
    \
    M(Bool, enable_local_disk_cache, true, "Enable local disk cache", 0) \
    M(Bool, enable_cloudfs, false, "CROSS feature for table level setting", 0) \
    M(Bool, enable_nexus_fs, false, "Enable local NexusFS", 0) \
    /*keep enable_preload_parts for compitable*/\
    M(Bool, enable_preload_parts, false, "Enable preload parts", 0) \
    M(UInt64, parts_preload_level, 0, "0=close preload;1=preload meta;2=preload data;3=preload meta&data", 0) \
    M(Bool, enable_parts_sync_preload, 0, "Enable sync preload parts", 0) \
    M(Bool, enable_gc_evict_disk_cache, false, "Enable gc evict disk cache", 0)      \
    M(UInt64, disk_cache_stealing_mode, 0, "Read/write remote vw local disk cache if cur local disk cache empty, 0: close; 1: read 2: write 3: read&write", 0) \
    \
    /* Renamed settings - cannot be ignored */\
    M(Bool, enable_nullable_sorting_key, false, "Alias of `allow_nullable_key`", 0) \
\
    /*************************************************************************************/ \
    /** Obsolete settings. Kept for backward compatibility only, or will add usage later */ \
    M(UInt64, min_relative_delay_to_yield_leadership, 120, "Obsolete setting, does nothing.", 0) \
    M(UInt64, check_delay_period, 60, "Obsolete setting, does nothing.", 0) \
    /** community behavior, but cnch 1.4 do not check this in mergetree. 2.0 disable this check to compatible with 1.4*/ \
    M(Bool, allow_floating_point_partition_key, true, "Allow floating point as partition key", 0) \
    M(Bool, cnch_enable_memory_buffer, false, "", 0) \
    M(UInt64, cnch_memory_buffer_size, 1, "", 0) \
    M(UInt64, max_bytes_to_write_wal, 1024 * 1024 * 45, "", 0) \
    M(UInt64, max_rows_memory_buffer_to_flush, 655360, "", 0) \
    M(UInt64, max_bytes_memory_buffer_to_flush, 1024 * 1024 * 512, "", 0) \
    M(Bool, cnch_merge_only_realtime_partition, false, "", 0) \
    M(Bool, \
      cnch_merge_select_nonadjacent_parts, \
      false, \
      "Select nonadjacent parts is allowed in the original design. Remove this when the feature is fully verified", \
      0) \
    M(String, cnch_merge_pick_worker_algo, "RM", "RM - using RM, RoundRobin: - local round robin strategy", 0) \
    M(UInt64, cnch_merge_round_robin_partitions_interval, 300, "", 0) \
    M(UInt64, cnch_gc_round_robin_partitions_interval, 600, "", 0) \
    M(UInt64, cnch_gc_round_robin_partitions_number, 10, "", 0) \
    M(UInt64, cnch_meta_rpc_timeout_ms, 8000, "", 0) \
    M(Bool, gc_ignore_running_transactions_for_test, false, "Ignore running transactions when calculating gc timestamp. Useful for tests only.", 0) \
    M(UInt64, gc_trash_part_batch_size, 5000, "Batch size to remove stale parts to trash in background tasks", 0) \
    M(UInt64, gc_trash_part_limit, 0, "Maximum number of stale parts to process per GC round, zero means no limit", 0) \
    M(UInt64, gc_trash_part_thread_pool_size, 4, "Turn up the thread pool size to speed up trashing of parts", 0) \
    M(UInt64, gc_remove_part_thread_pool_size, 20, "Turn up the thread pool size to speed up trash cleaning of parts", 0) \
    M(UInt64, gc_remove_part_batch_size, 200, "Batch size to remove trash parts from storage in background tasks", 0) \
\
    /** uuid of CnchMergeTree, as we won't use uuid in CloudMergeTree */ \
    M(String, cnch_table_uuid, "", "Used for CloudMergeTree to get uuid of Cnch Table for ingestion task, like Kafka", 0) \
\
    M(String, remote_storage_type, "hdfs", "Table's storage type[deprcated]", 0) \
    /** BitEngine related settings */ \
    M(UInt64, bitengine_split_index, 0, "Copatible setting for split BitEngine dict data, no real use", 0) \
    M(Float, bitengine_encode_loss_rate, 0.1, "The threshold that BitEngine discard some data and no exception will be thrown when encoding", 0) \
    M(Bool, bitengine_discard_source_bitmap, false, "BitEngine doesn't store the original data column in part", 0) \
    M(Bool, bitengine_use_key_string, false, "BitEngine support String key", 0) \
    M(Bool, bitengine_use_key_int, true, "BitEngine support UInt64 key, default way", 0) \
    M(String, underlying_dictionary_tables, "{}", "To specify a table to store BitEngine dictionary data, only in Cnch", 0) \
    /** Hybrid allocation related settings */ \
    M(Bool, enable_hybrid_allocation, false, "Whether or not enable hybrid allocation, default disabled", 0) \
    M(UInt64, min_rows_per_virtual_part, 2000000, "Minimum size of a virtual part", 0) \
    M(Float, part_to_vw_size_ratio, 0, "Part to vw worker size's ration", 0) \
    \
    /** MYSQL related settings */ \
    M(DialectType, storage_dialect_type, DialectType::CLICKHOUSE, "If the storage's dialect_type is not CLICKHOUSE, need to persist the information for creating/running queries on worker", 0) \
    \
    /** JSON related settings start*/ \
    M(UInt64, json_subcolumns_threshold, 1000, "Max number of json sub columns", 0) \
    M(UInt64, json_partial_schema_assemble_batch_size, 100, "Batch size to assemble dynamic object column schema", 0) \
    M(Int64, cnch_part_allocation_algorithm, -1, "Part allocation algorithm, -1: disable table setting and use query setting, 0: jump consistent hashing, 1: bounded hash ring consistent hashing, 2: strict ring consistent hashing.", 0) \
    /** JSON related settings end*/ \
    \
    M(String, column_compress_block_settings, "", "Column compressed block size for each column, if not specified, use max_compress_block_size.", 0) \
    M(UInt64, filtered_ratio_to_use_skip_read, 0, "Ratio of origin rows to filtered rows when using skip reading, 0 means disable", 0) \

    /// Settings that should not change after the creation of a table.
#define APPLY_FOR_IMMUTABLE_MERGE_TREE_SETTINGS(M) \
    M(index_granularity)

DECLARE_SETTINGS_TRAITS(MergeTreeSettingsTraits, LIST_OF_MERGE_TREE_SETTINGS)


/** Settings for the MergeTree family of engines.
  * Could be loaded from config or from a CREATE TABLE query (SETTINGS clause).
  */
struct MergeTreeSettings : public BaseSettings<MergeTreeSettingsTraits>
{
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config, bool skip_unknown_settings = false);

    /// NOTE: will rewrite the AST to add immutable settings.
    void loadFromQuery(ASTStorage & storage_def, bool attach = false);

    /// We check settings after storage creation
    static bool isReadonlySetting(const String & name)
    {
        return name == "index_granularity" || name == "index_granularity_bytes" || name == "write_final_mark"
            || name == "enable_mixed_granularity_parts" || name == "storage_policy" || name == "remote_storage_type";
    }

    static bool isPartFormatSetting(const String & name)
    {
        return name == "min_bytes_for_wide_part" || name == "min_rows_for_wide_part"
            || name == "min_bytes_for_compact_part" || name == "min_rows_for_compact_part";
    }

    /// Check that the values are sane taking also query-level settings into account.
    void sanityCheck(const Settings & query_settings) const;
};

using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;

}
