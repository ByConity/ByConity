/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include <Common/ConfigurationCommon.h>

namespace DB
{
#define RM_CONFIG_FIELDS_LIST(M) \
    M(UInt64, port, "", 9000, ConfigFlag::Default, "desc: rpc port") \
    M(UInt64, init_client_tries, "", 3, ConfigFlag::Default, "") \
    M(UInt64, init_client_retry_interval_ms, "", 3000, ConfigFlag::Default, "") \
    M(UInt64, max_retry_times, "", 3, ConfigFlag::Default, "") \
    M(UInt64, worker_register_visible_granularity_sec, "", 5, ConfigFlag::Default, "change workers' state from Registering to Running every N seconds to avoid changing worker topology frequently.") \
    M(UInt64, worker_heartbeat_timeout_sec, "", 6, ConfigFlag::Default, "RM will mark a worker as outdated if don't receive its heartbeat in this timeout. And outdated workers will be removed periodcally.") \

DECLARE_CONFIG_DATA(RMConfigurationData, RM_CONFIG_FIELDS_LIST)
struct RMConfiguration final : public RMConfigurationData
{
};

#define SD_CONFIG_FIELDS_LIST(M) \
    M(String, mode, "", "local", ConfigFlag::Recommended, "") \
    M(String, server_psm, "server.psm", "data.cnch.server", ConfigFlag::Recommended, "") \
    M(String, vw_psm, "vw.psm", "data.cnch.vw", ConfigFlag::Recommended, "") \
    M(String, tso_psm, "tso.psm", "data.cnch.tso", ConfigFlag::Recommended, "") \
    M(String, daemon_manager_psm, "daemon_manager.psm", "data.cnch.daemon_manager", ConfigFlag::Recommended, "") \
    M(String, resource_manager_psm, "resource_manager.psm", "data.cnch.resource_manager", ConfigFlag::Default, "") \

DECLARE_CONFIG_DATA(SDConfigurationData, SD_CONFIG_FIELDS_LIST)

struct SDConfiguration final : public SDConfigurationData
{
};

#define SDKV_CONFIG_FIELDS_LIST(M) \
    M(String, election_prefix, "election_prefix", "", ConfigFlag::Recommended, "common prefix for all election keys") \
    M(String, server_manager_host_path, "server_manager.host_path", "data.cnch.server-election", ConfigFlag::Recommended, "election key of server manager") \
    M(UInt64, server_manager_refresh_interval_ms, "server_manager.refresh_interval_ms", 1000, ConfigFlag::Default, "") \
    M(UInt64, server_manager_expired_interval_ms, "server_manager.expired_interval_ms", 5000, ConfigFlag::Default, "") \
    M(String, resource_manager_host_path, "resource_manager.host_path", "data.cnch.resource_manager-election", ConfigFlag::Recommended, "election key of resource manager") \
    M(UInt64, resource_manager_refresh_interval_ms, "resource_manager.refresh_interval_ms", 1000, ConfigFlag::Default, "") \
    M(UInt64, resource_manager_expired_interval_ms, "resource_manager.expired_interval_ms", 5000, ConfigFlag::Default, "") \
    M(String, tso_host_path, "tso.host_path", "data.cnch.tso-election", ConfigFlag::Recommended, "election key of tso") \
    M(UInt64, tso_refresh_interval_ms, "tso.refresh_interval_ms", 1000, ConfigFlag::Default, "") \
    M(UInt64, tso_expired_interval_ms, "tso.expired_interval_ms", 5000, ConfigFlag::Default, "") \

DECLARE_CONFIG_DATA(SDKVConfigurationData, SDKV_CONFIG_FIELDS_LIST)

struct SDKVConfiguration final : public SDKVConfigurationData
{
};

#define TSO_CONFIG_FIELDS_LIST(M) \
    M(UInt64, port, "", 9000, ConfigFlag::Default, "desc: rpc port") \
    M(UInt64, tso_window_ms, "", 3000, ConfigFlag::Default, "") \
    M(UInt64, tso_max_retry_count, "", 3, ConfigFlag::Default, "") \

DECLARE_CONFIG_DATA(TSOConfigurationData, TSO_CONFIG_FIELDS_LIST)
struct TSOConfiguration final: public TSOConfigurationData
{
};

constexpr auto default_otlp_http_exporter_url = "http://localhost:10023/v1/traces";
#define TRACER_CONFIG_FIELDS_LISTS(M) \
    M(Bool, enable_tracer_onboot, "", false, ConfigFlag::Default, "") \
    M(UInt64, max_span_export_batch_size, "", 512, ConfigFlag::Default, "") \
    M(UInt64, max_span_queue_size, "", 2048, ConfigFlag::Default, "") \
    M(Float32, sampler_ratio, "", 0.5, ConfigFlag::Default, "") \
    M(String, tracer_exporter_type, "", "SYSTEM_LOG", ConfigFlag::Default, "") \
    M(UInt64, export_span_schedule_delay_millis, "", 500, ConfigFlag::Default, "") \
    M(String, http_exporter_url, "otlp.http_exporter_url", default_otlp_http_exporter_url, ConfigFlag::Default, "") \
    M(String, database, "system_log.database", "system", ConfigFlag::Default, "") \
    M(String, table, "system_log.table", "opentelemetry_trace_log", ConfigFlag::Default, "") \
    M(String, partition_by, "system_log.partition_by", "toYYYYMM(event_date)", ConfigFlag::Default, "") \
    M(String, order_by, "system_log.order_by", "(span_name, event_date, event_time, trace_id)", ConfigFlag::Default, "") \
    M(String, ttl, "system_log.ttl", "(span_name, event_date, event_time, trace_id)", ConfigFlag::Default, "") \
    M(UInt64, flush_interval_milliseconds, "system_log.flush_interval_milliseconds", 7500, ConfigFlag::Default, "") \


DECLARE_CONFIG_DATA(TracerConfigurationData, TRACER_CONFIG_FIELDS_LISTS)

struct TracerConfiguration final : public TracerConfigurationData
{
};

#define BSP_CONFIG_FIELDS_LISTS(M) \
    M(String, storage_policy, "", "default", ConfigFlag::Default, "storage policy for bsp mode disk exchange data") \
    M(String, volume, "", "local", ConfigFlag::Default, "volume to store bsp mode disk exchange data") \
    M(UInt64, gc_interval_seconds, "", 1800, ConfigFlag::Default, "by default, garbage collection for bsp directory is done every 1800 seconds") \
    M(UInt64, file_expire_seconds, "", 172800, ConfigFlag::Default, "by default, bsp mode disk exchange data expire after two days") \
    M(Int64, max_disk_bytes, "", 1024LL * 1024 * 1024 * 500, ConfigFlag::Default, "by default, exchange data file will consume at most 500 GB disk space")

DECLARE_CONFIG_DATA(BSPConfigurationData, BSP_CONFIG_FIELDS_LISTS)

struct BSPConfiguration final : public BSPConfigurationData
{
};

#define ROOT_CONFIG_FIELDS_LIST(M) \
    M(UInt64, tcp_port, "", 9000, ConfigFlag::Recommended, "") \
    M(UInt64, http_port, "", 8123, ConfigFlag::Recommended, "") \
    M(UInt64, rpc_port, "", 9100, ConfigFlag::Recommended, "") \
    M(UInt64, exchange_port, "", 0, ConfigFlag::Default, "") \
    M(UInt64, exchange_status_port, "", 0, ConfigFlag::Default, "") \
    M(Int64, keep_alive_timeout, "", 10, ConfigFlag::Default, "") \
    M(UInt64, max_connections, "", 1024 * 16, ConfigFlag::Default, "") \
    M(Bool, listen_try, "", false, ConfigFlag::Default, "") \
    M(Bool, listen_reuse_port, "", false, ConfigFlag::Default, "") \
    M(UInt64, listen_backlog, "", 64, ConfigFlag::Default, "") \
    M(UInt64, asynchronous_metrics_update_period_s, "", 60, ConfigFlag::Default, "") \
    M(String, cnch_type, "", "server", ConfigFlag::Recommended, "") \
    M(UInt64, max_concurrent_queries, "", 0, ConfigFlag::Default, "") \
    M(UInt64, max_concurrent_insert_queries, "", 0, ConfigFlag::Default, "") \
    M(UInt64, max_concurrent_system_queries, "", 0, ConfigFlag::Default, "") \
    M(Float32, cache_size_to_ram_max_ratio, "", 0.5, ConfigFlag::Default, "") \
    M(UInt64, shutdown_wait_unfinished, "", 5, ConfigFlag::Default, "") \
    M(UInt64, cnch_transaction_ts_expire_time, "", 2 * 60 * 60 * 1000, ConfigFlag::Default, "") \
    M(UInt64, cnch_task_heartbeat_interval, "", 5, ConfigFlag::Default, "") \
    M(UInt64, cnch_task_heartbeat_max_retries, "", 5, ConfigFlag::Default, "") \
    M(Int64, cnch_merge_txn_commit_timeout, "", 10, ConfigFlag::Default, "") \
    M(Float32, max_ratio_of_cnch_tasks_to_threads, "", 1.5, ConfigFlag::Default, "") \
    M(Float32, vw_ratio_of_busy_worker, "", 1.2, ConfigFlag::Default, "The ratio for detecting busy worker in the VW.") \
    M(UInt64, max_async_query_threads, "", 5000, ConfigFlag::Default, "Maximum threads that async queries use.") \
    M(UInt64, async_query_status_ttl, "", 86400, ConfigFlag::Default, "TTL for async query status stored in catalog, in seconds.") \
    M(UInt64, async_query_expire_time, "", 3600, ConfigFlag::Default, "Expire time for async query, in seconds.") \
    M(UInt64, \
      async_query_status_check_period, \
      "", \
      15 * 60, \
      ConfigFlag::Default, \
      "Cycle for checking expired async query status stored in catalog, in seconds.") \
    M(Bool, enable_cnch_write_remote_catalog, "", true, ConfigFlag::Default, "Set to false to disable writing catalog") \
    M(Bool, enable_cnch_write_remote_disk, "", true, ConfigFlag::Default, "set to false to disable writing data") \
    /**
     * Memory caches */ \
    M(UInt64, bitengine_memory_cache_size, "", 50UL * 1024 * 1024 * 1024, ConfigFlag::Default, "") \
    M(UInt64, checksum_cache_size, "", 5UL * 1024 * 1024 * 1024, ConfigFlag::Default, "") \
    M(UInt64, checksum_cache_bucket, "", 5000, ConfigFlag::Default, "") \
    M(UInt64, checksum_cache_shard, "", 8, ConfigFlag::Default, "") \
    M(UInt64, checksum_cache_lru_update_interval, "", 60, ConfigFlag::Default, "In seconds") \
    M(UInt64, cnch_checksums_cache_size, "", 5UL * 1024 * 1024 * 1024, ConfigFlag::Default, "") /* Checksums cache configs in cnch 1.4 */ \
    M(UInt64, cnch_primary_index_cache_size, "", 5UL * 1024 * 1024 * 1024, ConfigFlag::Default, "") \
    M(UInt64, compiled_expression_cache_size, "", 128UL * 1024 * 1024, ConfigFlag::Default, "") \
    M(UInt64, compressed_data_index_cache, "", 5UL * 1024 * 1024 * 1024, ConfigFlag::Default, "") \
    M(UInt64, delete_bitmap_cache_size, "", 1UL * 1024 * 1024 * 1024, ConfigFlag::Default, "") \
    M(UInt64, footer_cache_size, "", 3UL * 1024 * 1024 * 1024, ConfigFlag::Default, "") \
    M(UInt64, geometry_primary_index_cache_size, "", 1UL * 1024 * 1024 * 1024, ConfigFlag::Default, "") \
    M(UInt64, gin_index_filter_result_cache, "", 5UL * 1024 * 1024 * 1024, ConfigFlag::Default, "") \
    M(UInt64, ginindex_store_cache_size, "", 5UL * 1024 * 1024 * 1024, ConfigFlag::Default, "") \
    M(UInt64, ginindex_store_cache_bucket, "", 5000, ConfigFlag::Default, "") \
    M(UInt64, ginindex_store_cache_shard, "", 2, ConfigFlag::Default, "") \
    M(UInt64, ginindex_store_cache_ttl, "", 60, ConfigFlag::Default, "") \
    M(UInt64, ginindex_store_cache_lru_update_interval, "", 60, ConfigFlag::Default, "In seconds") \
    M(UInt64, ginindex_store_cache_sst_block_cache_size, "", 5UL * 1024 * 1024 * 1024, ConfigFlag::Default, "") \
    M(UInt64, intermediate_result_cache_size, "", 1UL * 1024 * 1024 * 1024, ConfigFlag::Default, "") \
    M(UInt64, mark_cache_size, "", 5UL * 1024 * 1024 * 1024, ConfigFlag::Default, "") \
    M(UInt64, mmap_cache_size, "", 1000, ConfigFlag::Default, "") \
    M(UInt64, unique_key_index_data_cache_size, "", 1UL * 1024 * 1024 * 1024, ConfigFlag::Default, "") \
    M(UInt64, unique_key_index_meta_cache_size, "", 1UL * 1024 * 1024 * 1024, ConfigFlag::Default, "") \
    M(UInt64, uncompressed_cache_size, "", 0, ConfigFlag::Default, "") \
    M(Bool, enable_uncompressed_cache_shard_mode, "", false, ConfigFlag::Default, "") \
    /**
     * Cache default size max ratio */ \
    M(Float32, bitengine_memory_cache_size_default_max_ratio, "", 0.15, ConfigFlag::Default, "") \
    M(Float32, checksum_cache_size_default_max_ratio, "", 0.05, ConfigFlag::Default, "") \
    M(Float32, cnch_primary_index_cache_size_default_max_ratio, "", 0.05, ConfigFlag::Default, "") \
    M(Float32, compiled_expression_cache_size_default_max_ratio, "", 0.001, ConfigFlag::Default, "") \
    M(Float32, compressed_data_index_cache_default_max_ratio, "", 0.025, ConfigFlag::Default, "") \
    M(Float32, delete_bitmap_cache_size_default_max_ratio, "", 0.005, ConfigFlag::Default, "") \
    M(Float32, footer_cache_size_default_max_ratio, "", 0.015, ConfigFlag::Default, "") \
    M(Float32, gin_index_filter_result_cache_default_max_ratio, "", 0.025, ConfigFlag::Default, "") \
    M(Float32, ginindex_store_cache_size_default_max_ratio, "", 0.025, ConfigFlag::Default, "") \
    M(Float32, intermediate_result_cache_size_default_max_ratio, "", 0.005, ConfigFlag::Default, "") \
    M(Float32, mark_cache_size_default_max_ratio, "", 0.05, ConfigFlag::Default, "") \
    M(Float32, unique_key_index_data_cache_size_default_max_ratio, "", 0.005, ConfigFlag::Default, "") \
    M(Float32, unique_key_index_meta_cache_size_default_max_ratio, "", 0.01, ConfigFlag::Default, "") \
    /**
     * Mutable */ \
    M(MutableUInt64, max_server_memory_usage, "", 0, ConfigFlag::Default, "") \
    M(MutableFloat32, max_server_memory_usage_to_ram_ratio, "", 0.9, ConfigFlag::Default, "") \
    M(MutableUInt64, kafka_max_partition_fetch_bytes, "", 1048576, ConfigFlag::Default, "") \
    M(MutableUInt64, stream_poll_timeout_ms, "", 500, ConfigFlag::Default, "") \
    M(MutableUInt64, debug_disable_merge_mutate_thread, "", false, ConfigFlag::Default, "") \
    M(MutableBool, debug_disable_merge_commit, "", false, ConfigFlag::Default, "") \
    /**
     * Might be removed */ \
    M(MutableUInt64, max_table_size_to_drop, "", 50000000000lu, ConfigFlag::Default, "") \
    M(MutableUInt64, max_partition_size_to_drop, "", 50000000000lu, ConfigFlag::Default, "") \
    M(MutableUInt64, databases_load_pool_size, "", 3, ConfigFlag::Default, "") \
    M(MutableUInt64, tables_load_pool_size, "", 8, ConfigFlag::Default, "") \
    M(MutableUInt64, parts_load_pool_size, "", 48, ConfigFlag::Default, "")

DECLARE_CONFIG_DATA(RootConfigurationData, ROOT_CONFIG_FIELDS_LIST)

#define QM_CONFIG_FIELDS_LIST(M) \
    M(MutableUInt64, max_vw_parallelize_size, "max_vw_parallelize_size", 20, ConfigFlag::Default, "")  \
    M(MutableUInt64, batch_size, "batch_size", 2, ConfigFlag::Default, "") \
    M(MutableUInt64, query_queue_size, "query_queue_size", 100, ConfigFlag::Default, "")  \
    M(MutableUInt64, trigger_interval, "trigger_interval", 100, ConfigFlag::Default, "")

DECLARE_CONFIG_DATA(QMConfigurationData, QM_CONFIG_FIELDS_LIST)
struct QMConfiguration final : public QMConfigurationData
{
};

#define AS_CONFIG_FIELDS_LIST(M) \
    M(MutableUInt64, mem_weight, "", 2, ConfigFlag::Default, "") \
    M(MutableUInt64, query_num_weight, "", 4, ConfigFlag::Default, "") \
    M(MutableUInt64, max_plan_segment_size, "", 500, ConfigFlag::Default, "") \
    M(MutableUInt64, unhealth_segment_size, "", 480, ConfigFlag::Default, "") \
    M(MutableFloat32, heavy_load_threshold, "", 0.75, ConfigFlag::Default, "") \
    M(MutableFloat32, only_source_threshold, "", 0.85, ConfigFlag::Default, "") \
    M(MutableFloat32, unhealth_threshold, "", 0.95, ConfigFlag::Default, "") \
    M(MutableUInt64, need_reset_seconds, "", 300, ConfigFlag::Default, "") \
    M(MutableUInt64, unhealth_recheck_seconds, "", 10, ConfigFlag::Default, "") \
    M(MutableUInt64, heartbeat_interval, "", 10000, ConfigFlag::Default, "")


DECLARE_CONFIG_DATA(ASConfigurationData, AS_CONFIG_FIELDS_LIST)
struct ASConfiguration final : public ASConfigurationData
{
};

struct RootConfiguration final : public RootConfigurationData
{
    RMConfiguration resource_manager;
    SDConfiguration service_discovery;
    SDKVConfiguration service_discovery_kv;
    QMConfiguration queue_manager;
    ASConfiguration adaptive_scheduler;
    TSOConfiguration tso_service;
    BSPConfiguration bulk_synchronous_parallel;


    RootConfiguration()
    {
        sub_configs.push_back(&resource_manager);
        sub_configs.push_back(&service_discovery);
        sub_configs.push_back(&service_discovery_kv);
        sub_configs.push_back(&queue_manager);
        sub_configs.push_back(&adaptive_scheduler);
        sub_configs.push_back(&tso_service);
        sub_configs.push_back(&bulk_synchronous_parallel);
    }

    void loadFromPocoConfigImpl(const PocoAbstractConfig & config, const String & current_prefix) override;
};

}
