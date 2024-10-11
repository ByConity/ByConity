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


#include <Storages/System/StorageSystemHuAllocStats.h>
#include <Databases/IDatabase.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/System/attachSystemTablesImpl.h>

#include <Storages/System/StorageSystemAggregateFunctionCombinators.h>
#include <Storages/System/StorageSystemAsynchronousMetrics.h>
#include <Storages/System/StorageSystemCnchBackups.h>
#include <Storages/System/StorageSystemBuildOptions.h>
#include <Storages/System/StorageSystemCollations.h>
#include <Storages/System/StorageSystemClusters.h>
#include <Storages/System/StorageSystemCnchStagedParts.h>
#include <Storages/System/StorageSystemColumns.h>
#include <Storages/System/StorageSystemDatabases.h>
#include <Storages/System/StorageSystemDataSkippingIndices.h>
#include <Storages/System/StorageSystemDataTypeFamilies.h>
#include <Storages/System/StorageSystemDetachedParts.h>
#include <Storages/System/StorageSystemDictionaries.h>
#include <Storages/System/StorageSystemEvents.h>
#include <Storages/System/StorageSystemFormats.h>
#include <Storages/System/StorageSystemFunctions.h>
#include <Storages/System/StorageSystemGraphite.h>

#include <Storages/System/StorageSystemCloudTables.h>
#include <Storages/System/StorageSystemDistributionQueue.h>
#include <Storages/System/StorageSystemMacros.h>
#include <Storages/System/StorageSystemMergeTreeSettings.h>
#include <Storages/System/StorageSystemMerges.h>
#include <Storages/System/StorageSystemMetrics.h>
#include <Storages/System/StorageSystemModels.h>
#include <Storages/System/StorageSystemMutations.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Storages/System/StorageSystemNexusFS.h>
#include <Storages/System/StorageSystemOne.h>
#include <Storages/System/StorageSystemPartMovesBetweenShards.h>
#include <Storages/System/StorageSystemParts.h>
#include <Storages/System/StorageSystemPartsColumns.h>
#include <Storages/System/StorageSystemProcesses.h>
#include <Storages/System/StorageSystemProjectionParts.h>
#include <Storages/System/StorageSystemProjectionPartsColumns.h>
#include <Storages/System/StorageSystemQueryQueue.h>
#include <Storages/System/StorageSystemReplicas.h>
#include <Storages/System/StorageSystemReplicatedFetches.h>
#include <Storages/System/StorageSystemReplicationQueue.h>
#include <Storages/System/StorageSystemSettings.h>
#include <Storages/System/StorageSystemTableEngines.h>
#include <Storages/System/StorageSystemTableFunctions.h>
#include <Storages/System/StorageSystemTables.h>
#include <Storages/System/StorageSystemZooKeeper.h>
#include <Storages/System/StorageSystemContributors.h>
#include <Storages/System/StorageSystemResourceGroups.h>
#include <Storages/System/StorageSystemErrors.h>
#include <Storages/System/StorageSystemDDLWorkerQueue.h>
#include <Storages/System/StorageSystemAutoStatsManagerSettings.h>
#include <Storages/System/StorageSystemAutoStatsManagerStatus.h>
#include <Storages/System/StorageSystemAutoStatsScopeSettings.h>
#include <Storages/System/StorageSystemAutoStatsUdiCounter.h>
#include <Storages/System/StorageSystemStatisticsTableView.h>
#if USE_RDKAFKA
#include <Storages/System/StorageSystemKafkaTasks.h>
#include <Storages/System/StorageSystemKafkaTables.h>
#include <Storages/System/StorageSystemCnchKafkaTables.h>
#endif
#include <Storages/System/StorageSystemVirtualWarehouseQueryQueue.h>
#include <Storages/System/StorageSystemCnchFilesystemLock.h>
#include "Storages/System/StorageSystemExternalTables.h"

#if !defined(ARCADIA_BUILD)
    #include <Storages/System/StorageSystemLicenses.h>
    #include <Storages/System/StorageSystemTimeZones.h>
#endif
#include <Storages/System/StorageSystemDisks.h>
#include <Storages/System/StorageSystemStoragePolicies.h>
#include <Storages/System/StorageSystemZeros.h>

#include <Storages/System/StorageSystemUsers.h>
#include <Storages/System/StorageSystemRoles.h>
#include <Storages/System/StorageSystemGrants.h>
#include <Storages/System/StorageSystemSensitiveGrants.h>
#include <Storages/System/StorageSystemRoleGrants.h>
#include <Storages/System/StorageSystemCurrentRoles.h>
#include <Storages/System/StorageSystemEnabledRoles.h>
#include <Storages/System/StorageSystemSettingsProfiles.h>
#include <Storages/System/StorageSystemSettingsProfileElements.h>
#include <Storages/System/StorageSystemRowPolicies.h>
#include <Storages/System/StorageSystemQuotas.h>
#include <Storages/System/StorageSystemQuotaLimits.h>
#include <Storages/System/StorageSystemQuotaUsage.h>
#include <Storages/System/StorageSystemQuotasUsage.h>
#include <Storages/System/StorageSystemUserDirectories.h>
#include <Storages/System/StorageSystemPrivileges.h>
#include <Storages/System/StorageSystemQueryCache.h>
#include <Storages/System/StorageSystemMetastore.h>
#include <Storages/System/StorageSystemBrokenTables.h>
#include <Storages/System/StorageSystemVirtualWarehouses.h>
#include <Storages/System/StorageSystemWorkerGroups.h>
#include <Storages/System/StorageSystemWorkers.h>
#include <Storages/System/StorageSystemManipulations.h>

#ifdef OS_LINUX
#include <Storages/System/StorageSystemStackTrace.h>
#endif

#include <Storages/System/StorageSystemBGThreads.h>
#include <Storages/System/StorageSystemCnchAsyncQueries.h>
#include <Storages/System/StorageSystemCnchColumns.h>
#include <Storages/System/StorageSystemCnchDatabases.h>
#include <Storages/System/StorageSystemCnchDatabasesHistory.h>
#include <Storages/System/StorageSystemCnchDedupWorkers.h>
#include <Storages/System/StorageSystemCnchDictionaries.h>
#include <Storages/System/StorageSystemCnchManipulations.h>
#include <Storages/System/StorageSystemCnchSnapshots.h>
#include <Storages/System/StorageSystemCnchTransactions.h>
#include <Storages/System/StorageSystemGlobalGCManager.h>
#include <Storages/System/StorageSystemCnchParts.h>
#include <Storages/System/StorageSystemCnchPartsColumns.h>
#include <Storages/System/StorageSystemCnchPartsInfo.h>
#include <Storages/System/StorageSystemCnchPartsInfoLocal.h>
#include <Storages/System/StorageSystemCnchTrashItemsInfo.h>
#include <Storages/System/StorageSystemCnchTrashItemsInfoLocal.h>
#include <Storages/System/StorageSystemCnchTableHost.h>
#include <Storages/System/StorageSystemCnchTableInfo.h>
#include <Storages/System/StorageSystemCnchTableTransactions.h>
#include <Storages/System/StorageSystemCnchTables.h>
#include <Storages/System/StorageSystemCnchViewTables.h>
#include <Storages/System/StorageSystemCnchTablesHistory.h>
#include <Storages/System/StorageSystemCnchTrashItems.h>
#include <Storages/System/StorageSystemCnchManifestList.h>
#include <Storages/System/StorageSystemCnchUserPriv.h>
#include <Storages/System/StorageSystemCnchDBPriv.h>
#include <Storages/System/StorageSystemDMBGJobs.h>
#include <Storages/System/StorageSystemGlobalGCManager.h>
#include <Storages/System/StorageSystemLockMap.h>
#include <Storages/System/StorageSystemPersistentBGJobStatus.h>
#include <Storages/System/StorageSystemVirtualWarehouses.h>
#include <Storages/System/StorageSystemWorkerGroups.h>
#include <Storages/System/StorageSystemWorkers.h>
#include <Storages/System/StorageSystemIOSchedulers.h>
#include <Storages/System/StorageSystemIOWorkers.h>
#include <Storages/System/StorageSystemCnchDetachedParts.h>
#if USE_HIVE
#include <Storages/System/StorageSystemExternalCatalogs.h>
#include <Storages/System/StorageSystemExternalDatabases.h>
#include <Storages/System/StorageSystemExternalTables.h>
#endif
#include <Storages/System/StorageSystemMaterializedMySQL.h>
#include <Storages/System/StorageSystemCnchMaterializedMySQL.h>
#include <Storages/System/StorageSystemCnchTransactionCleanTasks.h>
#include <Storages/System/StorageSystemSchemaInferenceCache.h>
#include <Storages/System/StorageSystemBGTaskStatistics.h>

namespace DB
{

void attachSystemTablesLocal(IDatabase & system_database)
{
    attach<StorageSystemOne>(system_database, "one");
    attach<StorageSystemNumbers>(system_database, "numbers", false);
    attach<StorageSystemNumbers>(system_database, "numbers_mt", true);
    attach<StorageSystemZeros>(system_database, "zeros", false);
    attach<StorageSystemZeros>(system_database, "zeros_mt", true);
    attach<StorageSystemDatabases>(system_database, "databases");
    attach<StorageSystemTables>(system_database, "tables");
    attach<StorageSystemColumns>(system_database, "columns");
    attach<StorageSystemFunctions>(system_database, "functions");
    attach<StorageSystemEvents>(system_database, "events");
    attach<StorageSystemSettings>(system_database, "settings");
    attach<SystemMergeTreeSettings<false>>(system_database, "merge_tree_settings");
    attach<SystemMergeTreeSettings<true>>(system_database, "replicated_merge_tree_settings");
    attach<StorageSystemBuildOptions>(system_database, "build_options");
    attach<StorageSystemFormats>(system_database, "formats");
    attach<StorageSystemTableFunctions>(system_database, "table_functions");
    attach<StorageSystemAggregateFunctionCombinators>(system_database, "aggregate_function_combinators");
    attach<StorageSystemDataTypeFamilies>(system_database, "data_type_families");
    attach<StorageSystemCollations>(system_database, "collations");
    attach<StorageSystemTableEngines>(system_database, "table_engines");
    attach<StorageSystemContributors>(system_database, "contributors");
    attach<StorageSystemUsers>(system_database, "users");
    attach<StorageSystemRoles>(system_database, "roles");
    attach<StorageSystemGrants>(system_database, "grants");
    attach<StorageSystemSensitiveGrants>(system_database, "sensitive_grants");
    attach<StorageSystemRoleGrants>(system_database, "role_grants");
    attach<StorageSystemCurrentRoles>(system_database, "current_roles");
    attach<StorageSystemEnabledRoles>(system_database, "enabled_roles");
    attach<StorageSystemSettingsProfiles>(system_database, "settings_profiles");
    attach<StorageSystemSettingsProfileElements>(system_database, "settings_profile_elements");
    attach<StorageSystemRowPolicies>(system_database, "row_policies");
    attach<StorageSystemQueryCache>(system_database, "query_cache");
    attach<StorageSystemQuotas>(system_database, "quotas");
    attach<StorageSystemQuotaLimits>(system_database, "quota_limits");
    attach<StorageSystemQuotaUsage>(system_database, "quota_usage");
    attach<StorageSystemQuotasUsage>(system_database, "quotas_usage");
    attach<StorageSystemUserDirectories>(system_database, "user_directories");
    attach<StorageSystemPrivileges>(system_database, "privileges");
    attach<StorageSystemErrors>(system_database, "errors");
    attach<StorageSystemDataSkippingIndices>(system_database, "data_skipping_indices");
    attach<StorageSystemManipulations>(system_database, "manipulations");
    attach<StorageSystemCloudTables>(system_database, "cloud_tables");
#if !defined(ARCADIA_BUILD)
    attach<StorageSystemLicenses>(system_database, "licenses");
    attach<StorageSystemTimeZones>(system_database, "time_zones");
#endif
#ifdef OS_LINUX
    attach<StorageSystemStackTrace>(system_database, "stack_trace");
#endif
    attach<StorageSystemIOSchedulers>(system_database, "io_schedulers");
    attach<StorageSystemIOWorkers>(system_database, "io_workers");
    attach<StorageSystemNexusFS>(system_database, "nexus_fs");

}

void attachSystemTablesServer(IDatabase & system_database, bool has_zookeeper)
{
    attachSystemTablesLocal(system_database);

    attach<StorageSystemParts>(system_database, "parts");
    attach<StorageSystemProjectionParts>(system_database, "projection_parts");
    attach<StorageSystemDetachedParts>(system_database, "detached_parts");
    attach<StorageSystemPartsColumns>(system_database, "parts_columns");
    attach<StorageSystemProjectionPartsColumns>(system_database, "projection_parts_columns");
    attach<StorageSystemDisks>(system_database, "disks");
    attach<StorageSystemStoragePolicies>(system_database, "storage_policies");
    attach<StorageSystemProcesses>(system_database, "processes");
    attach<StorageSystemQueryQueue>(system_database, "query_queue");
    attach<StorageSystemVirtualWarehouseQueryQueue>(system_database, "virtual_warehouse_queue");
    attach<StorageSystemMetrics>(system_database, "metrics");
    attach<StorageSystemMerges>(system_database, "merges");
    attach<StorageSystemMutations>(system_database, "mutations");
    attach<StorageSystemReplicas>(system_database, "replicas");
    attach<StorageSystemReplicationQueue>(system_database, "replication_queue");
    attach<StorageSystemDistributionQueue>(system_database, "distribution_queue");
    attach<StorageSystemDictionaries>(system_database, "dictionaries");
    attach<StorageSystemModels>(system_database, "models");
    attach<StorageSystemClusters>(system_database, "clusters");
    attach<StorageSystemGraphite>(system_database, "graphite_retentions");
    attach<StorageSystemMacros>(system_database, "macros");
    attach<StorageSystemReplicatedFetches>(system_database, "replicated_fetches");
    attach<StorageSystemPartMovesBetweenShards>(system_database, "part_moves_between_shards");
#if USE_RDKAFKA
    attach<StorageSystemKafkaTasks>(system_database, "kafka_tasks");
    attach<StorageSystemKafkaTables>(system_database, "kafka_tables");
    attach<StorageSystemCnchKafkaTables>(system_database, "cnch_kafka_tables");
#endif
#if USE_MYSQL
    attach<StorageSystemMaterializedMySQL>(system_database, "materialized_mysql");
    attach<StorageSystemCnchMaterializedMySQL>(system_database, "cnch_materialized_mysql");
#endif
    attach<StorageSystemResourceGroups>(system_database, "resource_groups");

    if (has_zookeeper)
        attach<StorageSystemZooKeeper>(system_database, "zookeeper");

    attach<StorageSystemMetastore>(system_database, "metastore");
    attach<StorageSystemBrokenTables>(system_database, "broken_tables");
    attach<StorageSystemBGThreads>(system_database, "bg_threads");
    attach<StorageSystemCnchParts>(system_database, "cnch_parts");
    attach<StorageSystemCnchTransactions>(system_database, "cnch_transactions");
    attach<StorageSystemCnchFilesystemLock>(system_database, "cnch_fs_lock");
    attach<StorageSystemCnchPartsColumns>(system_database, "cnch_parts_columns");
    attach<StorageSystemCnchPartsInfoLocal>(system_database, "cnch_parts_info_local");
    attach<StorageSystemCnchPartsInfo>(system_database, "cnch_parts_info");
    attach<StorageSystemCnchTrashItemsInfoLocal>(system_database, "cnch_trash_items_info_local");
    attach<StorageSystemCnchTrashItemsInfo>(system_database, "cnch_trash_items_info");
    attach<StorageSystemCnchTableInfo>(system_database, "cnch_table_info");
    attach<StorageSystemCnchTableTransactions>(system_database, "cnch_table_transactions");
    attach<StorageSystemCnchTablesHistory>(system_database, "cnch_tables_history");
    attach<StorageSystemCnchDatabases>(system_database, "cnch_databases");
    attach<StorageSystemCnchDatabasesHistory>(system_database, "cnch_databases_history");
    attach<StorageSystemCnchColumns>(system_database, "cnch_columns");
    attach<StorageSystemCnchDictionaries>(system_database, "cnch_dictionaries");
    attach<StorageSystemCnchTables>(system_database, "cnch_tables");
    attach<StorageSystemCnchViewTables>(system_database, "cnch_view_tables");
    attach<StorageSystemCnchManipulations>(system_database, "cnch_manipulations");
    attach<StorageSystemCnchSnapshots>(system_database, "cnch_snapshots");
    attach<StorageSystemCnchBackups>(system_database, "cnch_backups");
    attach<StorageSystemCnchUserPriv>(system_database, "cnch_user_priv");
    attach<StorageSystemCnchDBPriv>(system_database, "cnch_db_priv");
    attach<StorageSystemDMBGJobs>(system_database, "dm_bg_jobs");
    attach<StorageSystemPersistentBGJobStatus>(system_database, "persistent_bg_job_status");
    attach<StorageSystemGlobalGCManager>(system_database, "global_gc_manager");
    attach<StorageSystemLockMap>(system_database, "lock_map");
    attach<StorageSystemHuAllocStats>( system_database, "hualloc_stats");
    attach<StorageSystemAutoStatsManagerSettings>(system_database, "auto_stats_manager_settings");
    attach<StorageSystemAutoStatsManagerStatus>(system_database, "auto_stats_manager_status");
    attach<StorageSystemAutoStatsUdiCounter>(system_database, "memory_auto_stats_udi_counter");
    attach<StorageSystemAutoStatsScopeSettings>(system_database, "auto_stats_scope_settings");
    attach<StorageSystemStatisticsTableView>(system_database, "statistics_table_view");

    attach<StorageSystemWorkers>(system_database, "workers");
    attach<StorageSystemWorkerGroups>(system_database, "worker_groups");
    attach<StorageSystemVirtualWarehouses>(system_database, "virtual_warehouses");
    attach<StorageSystemCnchStagedParts>(system_database, "cnch_staged_parts");
    attach<StorageSystemCnchTableHost>(system_database, "cnch_table_host");
    attach<StorageSystemCnchDedupWorkers>(system_database, "cnch_dedup_workers");
    attach<StorageSystemCnchAsyncQueries>(system_database, "cnch_async_queries");
    attach<StorageSystemCnchTrashItems>(system_database, "cnch_trash_items");
    attach<StorageSystemCnchDetachedParts>(system_database, "cnch_detached_parts");
    attach<StorageSystemCnchManifestList>(system_database, "cnch_manifest_list");
#if USE_HIVE
    attach<StorageSystemExternalCatalogs>(system_database, "external_catalogs");
    attach<StorageSystemExternalDatabases>(system_database, "external_databases");
    attach<StorageSystemExternalTables>(system_database, "external_tables");
#endif
    attach<StorageSystemCnchTransactionCleanTasks>(system_database, "cnch_transaction_clean_tasks");
    attach<StorageSystemSchemaInferenceCache>(system_database, "schema_inference_cache");
    attach<StorageSystemBGTaskStatistics>(system_database, "bg_task_statistics");

}

void attachSystemTablesAsync(IDatabase & system_database, AsynchronousMetrics & async_metrics)
{
    attach<StorageSystemAsynchronousMetrics>(system_database, "asynchronous_metrics", async_metrics);
}


}
