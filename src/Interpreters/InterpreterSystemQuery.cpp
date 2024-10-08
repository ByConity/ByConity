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

#include <Interpreters/InterpreterSystemQuery.h>
#include <Catalog/Catalog.h>
#include <Common/DNSResolver.h>
#include <Common/ActionLock.h>
#include <Common/typeid_cast.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/JeprofControl.h>
#include <Common/SymbolIndex.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Common/ShellCommand.h>
#include <MergeTreeCommon/CnchServerTopology.h>
#include <MergeTreeCommon/CnchServerManager.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <MergeTreeCommon/GlobalGCManager.h>
#include <CloudServices/CnchBGThreadCommon.h>
#include <CloudServices/CnchPartGCThread.h>
#include <CloudServices/CnchManifestCheckpointThread.h>
#include <CloudServices/CnchServerClientPool.h>
#include <DaemonManager/DaemonManagerClient.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ExternalModelsLoader.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/TraceLog.h>
#include <Interpreters/TextLog.h>
#include <Interpreters/MetricLog.h>
#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/ZooKeeperLog.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/loadMetadata.h>
#include <BridgeHelper/CatBoostLibraryBridgeHelper.h>
#include <Access/ContextAccess.h>
#include <Access/AllowedClientHosts.h>
#include <CloudServices/CnchBGThreadsMap.h>
#include <CloudServices/DedupWorkerManager.h>
#include <Databases/IDatabase.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Disks/DiskRestartProxy.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageFactory.h>
#include <Storages/Kafka/StorageCnchKafka.h>
#include <Storages/MergeTree/ChecksumsCache.h>
#include <Storages/PartCacheManager.h>
#include <Storages/StorageMaterializedView.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Poco/UUIDGenerator.h>
#include <Poco/URI.h>
#include <Poco/JSON/Parser.h>
#include <TableFunctions/ITableFunction.h>
#include <FormaterTool/HDFSDumper.h>
#include <csignal>
#include <algorithm>
#include <unistd.h>
#include <Interpreters/QueryExchangeLog.h>
#include <Interpreters/CnchSystemLog.h>
#include <Interpreters/AutoStatsTaskLog.h>
#include <Interpreters/RemoteReadLog.h>
#include <common/sleep.h>
#include <Core/UUID.h>
#include <Interpreters/StorageID.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Transaction/LockManager.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_RDKAFKA
#include <Storages/Kafka/CnchKafkaOffsetManager.h>
#endif

#if USE_MYSQL
#include <Databases/MySQL/DatabaseCnchMaterializedMySQL.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_KILL;
    extern const int NOT_IMPLEMENTED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int NO_ZOOKEEPER;
}


namespace ActionLocks
{
    extern StorageActionBlockType PartsMerge;
    extern StorageActionBlockType PartsFetch;
    extern StorageActionBlockType PartsSend;
    extern StorageActionBlockType ReplicationQueue;
    extern StorageActionBlockType DistributedSend;
    extern StorageActionBlockType PartsTTLMerge;
    extern StorageActionBlockType PartsMove;
}


namespace
{

ExecutionStatus getOverallExecutionStatusOfCommands()
{
    return ExecutionStatus(0);
}

/// Consequently tries to execute all commands and generates final exception message for failed commands
template <typename Callable, typename ... Callables>
ExecutionStatus getOverallExecutionStatusOfCommands(Callable && command, Callables && ... commands)
{
    ExecutionStatus status_head(0);
    try
    {
        command();
    }
    catch (...)
    {
        status_head = ExecutionStatus::fromCurrentException();
    }

    ExecutionStatus status_tail = getOverallExecutionStatusOfCommands(std::forward<Callables>(commands)...);

    auto res_status = status_head.code != 0 ? status_head.code : status_tail.code;
    auto res_message = status_head.message + (status_tail.message.empty() ? "" : ("\n" + status_tail.message));

    return ExecutionStatus(res_status, res_message);
}

/// Consequently tries to execute all commands and throws exception with info about failed commands
template <typename ... Callables>
void executeCommandsAndThrowIfError(Callables && ... commands)
{
    auto status = getOverallExecutionStatusOfCommands(std::forward<Callables>(commands)...);
    if (status.code != 0)
        throw Exception(status.message, status.code);
}


AccessType getRequiredAccessType(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge)
        return AccessType::SYSTEM_MERGES;
    else if (action_type == ActionLocks::PartsFetch)
        return AccessType::SYSTEM_FETCHES;
    else if (action_type == ActionLocks::PartsSend)
        return AccessType::SYSTEM_REPLICATED_SENDS;
    else if (action_type == ActionLocks::ReplicationQueue)
        return AccessType::SYSTEM_REPLICATION_QUEUES;
    else if (action_type == ActionLocks::DistributedSend)
        return AccessType::SYSTEM_DISTRIBUTED_SENDS;
    else if (action_type == ActionLocks::PartsTTLMerge)
        return AccessType::SYSTEM_TTL_MERGES;
    else if (action_type == ActionLocks::PartsMove)
        return AccessType::SYSTEM_MOVES;
    else
        throw Exception("Unknown action type: " + std::to_string(action_type), ErrorCodes::LOGICAL_ERROR);
}

constexpr std::string_view table_is_not_replicated = "Table {} is not replicated";

}

/// Implements SYSTEM [START|STOP] <something action from ActionLocks>
void InterpreterSystemQuery::startStopAction(StorageActionBlockType action_type, bool start)
{
    auto manager = getContext()->getActionLocksManager();
    manager->cleanExpired();

    if (volume_ptr && action_type == ActionLocks::PartsMerge)
    {
        volume_ptr->setAvoidMergesUserOverride(!start);
    }
    else if (table_id)
    {
        auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
        if (table)
        {
            if (start)
            {
                manager->remove(table, action_type);
                table->onActionLockRemove(action_type);
            }
            else
                manager->add(table, action_type);
        }
    }
    else
    {
        auto access = getContext()->getAccess();
        for (auto & elem : DatabaseCatalog::instance().getDatabases(getContext()))
        {
            for (auto iterator = elem.second->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
            {
                StoragePtr table = iterator->table();
                if (!table)
                    continue;

                if (!access->isGranted(getRequiredAccessType(action_type), elem.first, iterator->name()))
                {
                    LOG_INFO(
                        log,
                        "Access {} denied, skipping {}.{}",
                        toString(getRequiredAccessType(action_type)),
                        elem.first,
                        iterator->name());
                    continue;
                }

                if (start)
                {
                    manager->remove(table, action_type);
                    table->onActionLockRemove(action_type);
                }
                else
                    manager->add(table, action_type);
            }
        }
    }
}


InterpreterSystemQuery::InterpreterSystemQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_->clone()), log(getLogger("InterpreterSystemQuery"))
{
}


BlockIO InterpreterSystemQuery::execute()
{
    auto & query = query_ptr->as<ASTSystemQuery &>();

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, getContext(), getRequiredAccessForDDLOnCluster());

    using Type = ASTSystemQuery::Type;

    /// Use global context with fresh system profile settings
    auto system_context = Context::createCopy(getContext()->getGlobalContext());
    system_context->setSetting("profile", getContext()->getSystemProfileName());

    /// Make canonical query for simpler processing
    if (query.type == Type::RELOAD_DICTIONARY)
    {
        if (!query.database.empty())
            query.table = query.database + "." + query.table;
    }
    else if (!query.table.empty())
    {
        table_id = getContext()->resolveStorageID(StorageID(query.database, query.table), Context::ResolveOrdinary);
    }


    volume_ptr = {};
    if (!query.storage_policy.empty() && !query.volume.empty())
        volume_ptr = getContext()->getStoragePolicy(query.storage_policy)->getVolumeByName(query.volume, true);

    /// Common system command
    switch (query.type)
    {
        case Type::SHUTDOWN:
        {
            getContext()->checkAccess(AccessType::SYSTEM_SHUTDOWN);
            if (kill(0, SIGTERM))
                throwFromErrno("System call kill(0, SIGTERM) failed", ErrorCodes::CANNOT_KILL);
            break;
        }
        case Type::KILL:
        {
            getContext()->checkAccess(AccessType::SYSTEM_SHUTDOWN);
            /// Exit with the same code as it is usually set by shell when process is terminated by SIGKILL.
            /// It's better than doing 'raise' or 'kill', because they have no effect for 'init' process (with pid = 0, usually in Docker).
            LOG_INFO(log, "Exit immediately as the SYSTEM KILL command has been issued.");
            _exit(128 + SIGKILL);
            // break; /// unreachable
        }
        case Type::SUSPEND:
        {
            auto command = fmt::format("kill -STOP {0} && sleep {1} && kill -CONT {0}", getpid(), query.seconds);
            LOG_DEBUG(log, "Will run {}", command);
            auto res = ShellCommand::execute(command);
            res->in.close();
            WriteBufferFromOwnString out;
            copyData(res->out, out);
            copyData(res->err, out);
            if (!out.str().empty())
                LOG_DEBUG(log, "The command returned output: {}", command, out.str());
            res->wait();
            break;
        }
        case Type::DROP_DNS_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_DNS_CACHE);
            DNSResolver::instance().dropCache();
            /// Reinitialize clusters to update their resolved_addresses
            system_context->reloadClusterConfig();
            break;
        }
        case Type::DROP_MARK_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_MARK_CACHE);
            system_context->dropMarkCache();
            break;
        case Type::DROP_UNCOMPRESSED_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_UNCOMPRESSED_CACHE);
            system_context->dropUncompressedCache();
            break;
        case Type::DROP_NVM_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_NVM_CACHE);
            system_context->dropNvmCache();
            break;
        case Type::DROP_SCHEMA_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_SCHEMA_CACHE);
            std::unordered_set<String> caches_to_drop;
            if (query.schema_cache_storage.empty())
                caches_to_drop = {"S3"};
            else
                caches_to_drop = {query.schema_cache_storage};
#if USE_AWS_S3
            if (caches_to_drop.contains("S3"))
                StorageS3::getSchemaCache(getContext()).clear();
#endif
            break;
        }
        case Type::DROP_MMAP_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_MMAP_CACHE);
            system_context->dropMMappedFileCache();
            break;
        case Type::DROP_QUERY_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_QUERY_CACHE);
            getContext()->dropQueryCache();
            break;
        case Type::DROP_CHECKSUMS_CACHE:
            dropChecksumsCache(table_id);
            break;
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_COMPILED_EXPRESSION_CACHE);
            if (auto * cache = CompiledExpressionCacheFactory::instance().tryGetCache())
                cache->reset();
            break;
#endif
        case Type::RELOAD_DICTIONARY:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_DICTIONARY);

            auto & external_dictionaries_loader = system_context->getExternalDictionariesLoader();
            external_dictionaries_loader.reloadDictionary(query.table, getContext());

            ExternalDictionariesLoader::resetAll();
            break;
        }
        case Type::RELOAD_DICTIONARIES:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_DICTIONARY);
            executeCommandsAndThrowIfError(
                [&] { system_context->getExternalDictionariesLoader().reloadAllTriedToLoad(); },
                [&] { system_context->getEmbeddedDictionaries().reload(); }
            );
            ExternalDictionariesLoader::resetAll();
            break;
        }
        case Type::RELOAD_MODEL:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_MODEL);

            auto bridge_helper = std::make_unique<CatBoostLibraryBridgeHelper>(getContext(), query.target_model);
            bridge_helper->removeModel();
            break;
        }
        case Type::RELOAD_MODELS:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_MODEL);

            auto bridge_helper = std::make_unique<CatBoostLibraryBridgeHelper>(getContext());
            bridge_helper->removeAllModels();
            break;
        }
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_EMBEDDED_DICTIONARIES);
            system_context->getEmbeddedDictionaries().reload();
            break;
        case Type::RELOAD_CONFIG:
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_CONFIG);
            system_context->reloadConfig();
            break;
        case Type::RELOAD_FORMAT_SCHEMA:
            // REUSE SYSTEM_RELOAD_CONF
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_CONFIG);
            reloadFormatSchema(system_context, system_context->getFormatSchemaPath(true), system_context->getFormatSchemaPath(false));
            break;
        case Type::RELOAD_SYMBOLS:
        {
#if defined(__ELF__) && !defined(__FreeBSD__)
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_SYMBOLS);
            (void)SymbolIndex::instance(true);
            break;
#else
            throw Exception("SYSTEM RELOAD SYMBOLS is not supported on current platform", ErrorCodes::NOT_IMPLEMENTED);
#endif
        }
        case Type::RESTART_DISK:
            restartDisk(query.disk);
            break;
        case Type::FLUSH_LOGS:
        {
            getContext()->checkAccess(AccessType::SYSTEM_FLUSH_LOGS);
            executeCommandsAndThrowIfError(
                [&] { if (auto query_log = getContext()->getQueryLog()) query_log->flush(true); },
                [&] { if (auto part_log = getContext()->getPartLog("")) part_log->flush(true); },
                [&] { if (auto query_thread_log = getContext()->getQueryThreadLog()) query_thread_log->flush(true); },
                [&] { if (auto query_exchange_log = getContext()->getQueryExchangeLog()) query_exchange_log->flush(true); },
                [&] { if (auto trace_log = getContext()->getTraceLog()) trace_log->flush(true); },
                [&] { if (auto text_log = getContext()->getTextLog()) text_log->flush(true); },
                [&] { if (auto metric_log = getContext()->getMetricLog()) metric_log->flush(true); },
                [&] { if (auto asynchronous_metric_log = getContext()->getAsynchronousMetricLog()) asynchronous_metric_log->flush(true); },
                [&] { if (auto opentelemetry_span_log = getContext()->getOpenTelemetrySpanLog()) opentelemetry_span_log->flush(true); },
                [&] { if (auto zookeeper_log = getContext()->getZooKeeperLog()) zookeeper_log->flush(true); },
                [&] { if (auto processors_profile_log = getContext()->getProcessorsProfileLog()) processors_profile_log->flush(true); },
                [&] { if (auto auto_stats_task_log = getContext()->getAutoStatsTaskLog()) auto_stats_task_log->flush(true); },
                [&] { if (auto remote_read_log = getContext()->getRemoteReadLog()) remote_read_log->flush(true); }
            );
            break;
        }
        case Type::FLUSH_CNCH_LOG:
        case Type::STOP_CNCH_LOG:
        case Type::RESUME_CNCH_LOG:
        {
            executeActionOnCNCHLog(query.table, query.type);
            break;
        }
        case Type::METASTORE:
            executeMetastoreCmd(query);
            break;
        case Type::START_RESOURCE_GROUP:
            system_context->startResourceGroup();
            break;
        case Type::STOP_RESOURCE_GROUP:
            system_context->stopResourceGroup();
            break;
        case Type::JEPROF_DUMP:
#if USE_JEMALLOC
            JeprofControl::instance().dump();
            break;
#else
            throw Exception("Jemalloc is not used", ErrorCodes::BAD_ARGUMENTS);
#endif
        default:
        {
            if (getContext()->getServerType() == ServerType::cnch_server)
                return executeCnchCommand(query, system_context);
            return executeLocalCommand(query, system_context);
        }
    }

    return BlockIO{};
}

BlockIO InterpreterSystemQuery::executeCnchCommand(ASTSystemQuery & query, ContextMutablePtr & system_context)
{
    using Type = ASTSystemQuery::Type;
    switch (query.type)
    {
        case Type::START_CONSUME:
        case Type::STOP_CONSUME:
        case Type::DROP_CONSUME:
        case Type::RESTART_CONSUME:
            controlConsume(query.type);
            break;
        case Type::START_MATERIALIZEDMYSQL:
        case Type::STOP_MATERIALIZEDMYSQL:
        case Type::RESYNC_MATERIALIZEDMYSQL_TABLE:
            executeMaterializedMyQLInCnchServer(query);
            break;
        case Type::RESET_CONSUME_OFFSET:
            resetConsumeOffset(query, system_context);
            break;
        case Type::STOP_MERGES:
        case Type::START_MERGES:
        case Type::REMOVE_MERGES:
        case Type::RESUME_ALL_MERGES:
        case Type::SUSPEND_ALL_MERGES:
        case Type::STOP_GC:
        case Type::START_GC:
        case Type::FORCE_GC:
        case Type::RESUME_ALL_GC:
        case Type::SUSPEND_ALL_GC:
        case Type::START_DEDUP_WORKER:
        case Type::STOP_DEDUP_WORKER:
        case Type::START_CLUSTER:
        case Type::STOP_CLUSTER:
        case Type::START_VIEW:
        case Type::STOP_VIEW:
        case Type::STOP_MOVES:
        case Type::START_MOVES:
            executeBGTaskInCnchServer(system_context, query.type);
            break;
        case Type::GC:
            executeGc(query);
            break;
        case Type::MANIFEST_CHECKPOINT:
            executeCheckpoint(query);
            break;
        case Type::DEDUP_WITH_HIGH_PRIORITY:
            dedupWithHighPriority(query);
            break;
        case Type::DEDUP:
            executeDedup(query);
            break;
        case Type::DUMP_SERVER_STATUS:
            dumpCnchServerStatus();
            break;
        case Type::DROP_CNCH_META_CACHE:
            dropCnchMetaCache();
            break;
        case Type::DROP_CNCH_PART_CACHE:
            dropCnchMetaCache(false, true);
            break;
        case Type::DROP_CNCH_DELETE_BITMAP_CACHE:
            dropCnchMetaCache(true);
            break;
        case Type::SYNC_DEDUP_WORKER:
            executeSyncDedupWorker(system_context);
            break;
        case Type::SYNC_REPAIR_TASK:
            executeSyncRepairTask(system_context);
            break;
        case Type::CLEAN_TRANSACTION:
            cleanTransaction(query.txn_id);
            break;
        case Type::CLEAN_TRASH_TABLE:
            executeCleanTrashTable(query);
            break;
        case Type::CLEAN_FILESYSTEM_LOCK:
            cleanFilesystemLock();
            break;
        case Type::LOCK_MEMORY_LOCK:
            lockMemoryLock(query, table_id, system_context);
            break;
        case Type::RECALCULATE_METRICS:
            recalculateMetrics(query);
            break;
        case Type::DROP_VIEW_META:
            dropMvMeta(query);
            break;
        case Type::RELEASE_MEMORY_LOCK:
            releaseMemoryLock(query, table_id, system_context);
            break;
        case Type::TRIGGER_HDFS_CONFIG_UPDATE:
            triggerHDFSConfigUpdate();
            break;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "System command {} is not supported in CNCH", ASTSystemQuery::typeToString(query.type));
    }
    return {};
}

void InterpreterSystemQuery::dropMvMeta(ASTSystemQuery & query)
{
    if (!query.database.empty() && !query.table.empty())
    {
        auto storage = DatabaseCatalog::instance().getTable(StorageID{query.database, query.table}, getContext());
        if (auto * materialized_view = dynamic_cast<StorageMaterializedView *>(storage.get()))
            materialized_view->dropMvMeta(getContext());
        else
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "System command {} is only accept materialized view", ASTSystemQuery::typeToString(query.type));
    }
    else
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "system drop view meta need both databse and table name with format database.table", ASTSystemQuery::typeToString(query.type));
    }
}

BlockIO InterpreterSystemQuery::executeLocalCommand(ASTSystemQuery & query, ContextMutablePtr & system_context)
{
    using Type = ASTSystemQuery::Type;
    switch (query.type)
    {
        case Type::STOP_MERGES:
            startStopAction(ActionLocks::PartsMerge, false);
            break;
        case Type::START_MERGES:
            startStopAction(ActionLocks::PartsMerge, true);
            break;
        case Type::STOP_TTL_MERGES:
            startStopAction(ActionLocks::PartsTTLMerge, false);
            break;
        case Type::START_TTL_MERGES:
            startStopAction(ActionLocks::PartsTTLMerge, true);
            break;
        case Type::STOP_MOVES:
            startStopAction(ActionLocks::PartsMove, false);
            break;
        case Type::START_MOVES:
            startStopAction(ActionLocks::PartsMove, true);
            break;
        case Type::STOP_FETCHES:
            startStopAction(ActionLocks::PartsFetch, false);
            break;
        case Type::START_FETCHES:
            startStopAction(ActionLocks::PartsFetch, true);
            break;
        case Type::STOP_REPLICATED_SENDS:
            startStopAction(ActionLocks::PartsSend, false);
            break;
        case Type::START_REPLICATED_SENDS:
            startStopAction(ActionLocks::PartsSend, true);
            break;
        case Type::STOP_REPLICATION_QUEUES:
            startStopAction(ActionLocks::ReplicationQueue, false);
            break;
        case Type::START_REPLICATION_QUEUES:
            startStopAction(ActionLocks::ReplicationQueue, true);
            break;
        case Type::STOP_DISTRIBUTED_SENDS:
            startStopAction(ActionLocks::DistributedSend, false);
            break;
        case Type::START_DISTRIBUTED_SENDS:
            startStopAction(ActionLocks::DistributedSend, true);
            break;
        case Type::DROP_REPLICA:
            dropReplica(query);
            break;
        case Type::SYNC_REPLICA:
            syncReplica(query);
            break;
        case Type::FLUSH_DISTRIBUTED:
            flushDistributed(query);
            break;
        case Type::RESTART_REPLICAS:
            restartReplicas(system_context);
            break;
        case Type::RESTART_REPLICA:
            if (!tryRestartReplica(table_id, system_context))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, table_is_not_replicated.data(), table_id.getNameForLogs());
            break;
        case Type::RESTORE_REPLICA:
            restoreReplica();
            break;
        case Type::STOP_LISTEN_QUERIES:
        case Type::START_LISTEN_QUERIES:
            throw Exception(String(ASTSystemQuery::typeToString(query.type)) + " is not supported yet", ErrorCodes::NOT_IMPLEMENTED);
        case Type::FETCH_PARTS:
            fetchParts(query, table_id, system_context);
            break;
        case Type::CLEAR_BROKEN_TABLES:
            clearBrokenTables(system_context);
            break;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "System command {} is not supported", ASTSystemQuery::typeToString(query.type));
    }
    return {};
}

void InterpreterSystemQuery::executeBGTaskInCnchServer(ContextMutablePtr & system_context, ASTSystemQuery::Type type) const
{
    using Type = ASTSystemQuery::Type;
    auto daemon_manager = getContext()->getDaemonManagerClient();

    if (table_id.empty())
    {
        switch (type)
        {
            case Type::RESUME_ALL_MERGES:
                daemon_manager->controlDaemonJob(StorageID::createEmpty(), CnchBGThreadType::MergeMutate, CnchBGThreadAction::Start, CurrentThread::getQueryId().toString());
                break;
            case Type::SUSPEND_ALL_MERGES:
                daemon_manager->controlDaemonJob(StorageID::createEmpty(), CnchBGThreadType::MergeMutate, CnchBGThreadAction::Remove, CurrentThread::getQueryId().toString());
                break;
            case Type::RESUME_ALL_GC:
                daemon_manager->controlDaemonJob(StorageID::createEmpty(), CnchBGThreadType::PartGC, CnchBGThreadAction::Start, CurrentThread::getQueryId().toString());
                break;
            case Type::SUSPEND_ALL_GC:
                daemon_manager->controlDaemonJob(StorageID::createEmpty(), CnchBGThreadType::PartGC, CnchBGThreadAction::Remove, CurrentThread::getQueryId().toString());
                break;
            default:
                throw Exception("Table name should be specified for control specified background task", ErrorCodes::LOGICAL_ERROR);
        }
        return;
    }

    auto storage = DatabaseCatalog::instance().getTable(table_id, system_context);

    if (!dynamic_cast<StorageCnchMergeTree *>(storage.get()) && !dynamic_cast<StorageMaterializedView *>(storage.get()))
        throw Exception("StorageCnchMergeTree or StorageMaterializedView is expected, but got " + storage->getName(), ErrorCodes::BAD_ARGUMENTS);

    switch (type)
    {
        case Type::START_MERGES:
            daemon_manager->controlDaemonJob(storage->getStorageID(), CnchBGThreadType::MergeMutate, CnchBGThreadAction::Start, CurrentThread::getQueryId().toString());
            break;
        case Type::STOP_MERGES:
            daemon_manager->controlDaemonJob(storage->getStorageID(), CnchBGThreadType::MergeMutate, CnchBGThreadAction::Stop, CurrentThread::getQueryId().toString());
            break;
        case Type::REMOVE_MERGES:
            daemon_manager->controlDaemonJob(storage->getStorageID(), CnchBGThreadType::MergeMutate, CnchBGThreadAction::Remove, CurrentThread::getQueryId().toString());
            break;
        case Type::START_GC:
            daemon_manager->controlDaemonJob(storage->getStorageID(), CnchBGThreadType::PartGC, CnchBGThreadAction::Start, CurrentThread::getQueryId().toString());
            break;
        case Type::STOP_GC:
            daemon_manager->controlDaemonJob(storage->getStorageID(), CnchBGThreadType::PartGC, CnchBGThreadAction::Stop, CurrentThread::getQueryId().toString());
            break;
        case Type::FORCE_GC:
            daemon_manager->controlDaemonJob(storage->getStorageID(), CnchBGThreadType::PartGC, CnchBGThreadAction::Wakeup, CurrentThread::getQueryId().toString());
            break;
        case Type::START_DEDUP_WORKER:
            daemon_manager->controlDaemonJob(storage->getStorageID(), CnchBGThreadType::DedupWorker, CnchBGThreadAction::Start, CurrentThread::getQueryId().toString());
            break;
        case Type::STOP_DEDUP_WORKER:
            daemon_manager->controlDaemonJob(storage->getStorageID(), CnchBGThreadType::DedupWorker, CnchBGThreadAction::Stop, CurrentThread::getQueryId().toString());
            break;
        case Type::START_CLUSTER:
            daemon_manager->controlDaemonJob(storage->getStorageID(), CnchBGThreadType::Clustering, CnchBGThreadAction::Start, CurrentThread::getQueryId().toString());
            break;
        case Type::STOP_CLUSTER:
            daemon_manager->controlDaemonJob(storage->getStorageID(), CnchBGThreadType::Clustering, CnchBGThreadAction::Stop, CurrentThread::getQueryId().toString());
            break;
        case Type::START_VIEW:
            daemon_manager->controlDaemonJob(storage->getStorageID(), CnchBGThreadType::CnchRefreshMaterializedView, CnchBGThreadAction::Start, CurrentThread::getQueryId().toString());
            break;
        case Type::STOP_VIEW:
            daemon_manager->controlDaemonJob(storage->getStorageID(), CnchBGThreadType::CnchRefreshMaterializedView, CnchBGThreadAction::Stop, CurrentThread::getQueryId().toString());
            break;
        case Type::START_MOVES:
            daemon_manager->controlDaemonJob(storage->getStorageID(), CnchBGThreadType::PartMover, CnchBGThreadAction::Start, CurrentThread::getQueryId().toString());
            break;
        case Type::STOP_MOVES:
            daemon_manager->controlDaemonJob(storage->getStorageID(), CnchBGThreadType::PartMover, CnchBGThreadAction::Stop, CurrentThread::getQueryId().toString());
            break;
        default:
            throw Exception("Unknown command type " + toString(ASTSystemQuery::typeToString(type)), ErrorCodes::LOGICAL_ERROR);
    }
}

void InterpreterSystemQuery::executeSyncDedupWorker(ContextMutablePtr & system_context) const
{
    if (table_id.empty())
        throw Exception("Table name should be specified for control background task", ErrorCodes::BAD_ARGUMENTS);

    auto storage = DatabaseCatalog::instance().getTable(table_id, system_context);

    auto cnch_storage = dynamic_cast<StorageCnchMergeTree *>(storage.get());
    if (!cnch_storage)
        throw Exception("StorageCnchMergeTree is expected, but got " + storage->getName(), ErrorCodes::BAD_ARGUMENTS);

    cnch_storage->waitForStagedPartsToPublish(system_context);
}

void InterpreterSystemQuery::executeSyncRepairTask(ContextMutablePtr & system_context) const
{
    if (table_id.empty())
        throw Exception("Table name should be specified for control background task", ErrorCodes::BAD_ARGUMENTS);

    auto storage = DatabaseCatalog::instance().getTable(table_id, system_context);

    auto * cnch_storage = dynamic_cast<StorageCnchMergeTree *>(storage.get());
    if (!cnch_storage)
        throw Exception("StorageCnchMergeTree is expected, but got " + storage->getName(), ErrorCodes::BAD_ARGUMENTS);

    auto data_checker = std::make_shared<DedupDataChecker>(getContext(), storage->getCnchStorageID().getNameForLogs() + "(data_checker)", *cnch_storage);
    data_checker->waitForNoDuplication(system_context);
}

void InterpreterSystemQuery::controlConsume(ASTSystemQuery::Type type)
{
    if (table_id.empty())
        throw Exception("Empty table id for executing START/STOP consume", ErrorCodes::LOGICAL_ERROR);

    auto storage = DatabaseCatalog::instance().getTable(table_id, getContext());
    auto * cnch_kafka = dynamic_cast<StorageCnchKafka *>(storage.get());
    if (!cnch_kafka)
        throw Exception("CnchKafka is supported but provided " + storage->getName(), ErrorCodes::BAD_ARGUMENTS);

    auto local_context = getContext();
    auto daemon_manager = local_context->getDaemonManagerClient();

    auto catalog = local_context->getCnchCatalog();
    using Type = ASTSystemQuery::Type;
    switch (type)
    {
        case Type::START_CONSUME:
            daemon_manager->controlDaemonJob(cnch_kafka->getStorageID(), CnchBGThreadType::Consumer, CnchBGThreadAction::Start, local_context->getCurrentQueryId());
            break;
        case Type::STOP_CONSUME:
            daemon_manager->controlDaemonJob(cnch_kafka->getStorageID(), CnchBGThreadType::Consumer, CnchBGThreadAction::Stop, local_context->getCurrentQueryId());
            break;
        case Type::RESTART_CONSUME:
            daemon_manager->controlDaemonJob(cnch_kafka->getStorageID(), CnchBGThreadType::Consumer, CnchBGThreadAction::Stop, local_context->getCurrentQueryId());
            usleep(500 * 1000);
            daemon_manager->controlDaemonJob(cnch_kafka->getStorageID(), CnchBGThreadType::Consumer, CnchBGThreadAction::Start, local_context->getCurrentQueryId());
            break;
        case Type::DROP_CONSUME:
            daemon_manager->controlDaemonJob(cnch_kafka->getStorageID(), CnchBGThreadType::Consumer, CnchBGThreadAction::Drop, local_context->getCurrentQueryId());
            break;
        default:
            throw Exception("Unknown command type " + String(ASTSystemQuery::typeToString(type)), ErrorCodes::LOGICAL_ERROR);
    }
}

void InterpreterSystemQuery::resetConsumeOffset(ASTSystemQuery & query, ContextMutablePtr & system_context)
{
    Poco::JSON::Parser json_parser;
    Poco::Dynamic::Var result = json_parser.parse(query.string_data);
    auto object = result.extract<Poco::JSON::Object::Ptr>();

    if (!object->has("database_name") || !object->has("table_name"))
        throw Exception("Database and table name are required for ResetConsumeOffset", ErrorCodes::BAD_ARGUMENTS);

    String database_name = object->getValue<std::string>("database_name");
    String table_name = object->getValue<std::string>("table_name");
    auto storage = system_context->getCnchCatalog()->getTable(*system_context, database_name, table_name, system_context->getTimestamp());
    auto * cnch_kafka = dynamic_cast<StorageCnchKafka *>(storage.get());
    if (!cnch_kafka)
        throw Exception("CnchKafka is expected but provided " + storage->getName(), ErrorCodes::BAD_ARGUMENTS);
    CnchKafkaOffsetManagerPtr kafka_offset_manager = std::make_shared<CnchKafkaOffsetManager>(cnch_kafka->getStorageID(), system_context);

    if (object->has("offset_value"))
    {
        int64_t offset = object->getValue<int64_t>("offset_value");
        kafka_offset_manager->resetOffsetToSpecialPosition(offset);
    }
    else if (object->has("timestamp"))
    {
        uint64_t timestamp = object->getValue<uint64_t>("timestamp");
        kafka_offset_manager->resetOffsetWithTimestamp(timestamp);
    }
    else if (object->has("offset_values"))
    {
        String topic_name = object->getValue<std::string>("topic_name");
        Poco::JSON::Array::Ptr children = object->getArray("offset_values");
        cppkafka::TopicPartitionList tpl;
        for (const auto & e : *children)
        {
            auto e_object = e.extract<Poco::JSON::Object::Ptr>();
            if (e_object->has("offset") && e_object->has("partition"))
            {
                int64_t offset = e_object->getValue<int64_t>("offset");
                int partition_number = e_object->getValue<int>("partition");
                tpl.push_back({topic_name, partition_number, offset});
            }
            else
                throw Exception("sub-object under offset_values Array need to has offset and partition!, input: "
                                    + query.string_data, ErrorCodes::BAD_ARGUMENTS);
        }

        kafka_offset_manager->resetOffsetWithSpecificOffsets(tpl);
    }
    else
        throw Exception("offset should be set by one of the parameters: timestamp, offset_value, offset_values. input: "
                            + query.string_data, ErrorCodes::BAD_ARGUMENTS);
}

void InterpreterSystemQuery::executeMaterializedMyQLInCnchServer(const ASTSystemQuery & query)
{
    ContextPtr local_context = getContext();
    auto database = DatabaseCatalog::instance().getDatabase(query.database, local_context);
    auto * materialized_mysql = dynamic_cast<DatabaseCnchMaterializedMySQL*>(database.get());
    if (!materialized_mysql)
        throw Exception("Expect CnchMaterializedMySQL, but got " + database->getEngineName(), ErrorCodes::BAD_ARGUMENTS);

    auto daemon_manager = getContext()->getDaemonManagerClient();

    using Type = ASTSystemQuery::Type;
    switch (query.type)
    {
        /// TODO: record the status in catalog for persistent
        case Type::START_MATERIALIZEDMYSQL:
            daemon_manager->controlDaemonJob(materialized_mysql->getStorageID(), CnchBGThreadType::MaterializedMySQL, CnchBGThreadAction::Start, local_context->getCurrentQueryId());
            break;
        case Type::STOP_MATERIALIZEDMYSQL:
            daemon_manager->controlDaemonJob(materialized_mysql->getStorageID(), CnchBGThreadType::MaterializedMySQL, CnchBGThreadAction::Stop, local_context->getCurrentQueryId());
            break;
        case Type::RESYNC_MATERIALIZEDMYSQL_TABLE:
            /// Here we use getContext() rather than system_context as query forwarding needs process_list_entry
            materialized_mysql->manualResyncTable(query.table, getContext());
            break;
        default:
            throw Exception(String(ASTSystemQuery::typeToString(query.type)) + " is not supported now", ErrorCodes::NOT_IMPLEMENTED);
    }
}

void InterpreterSystemQuery::restoreReplica()
{
    getContext()->checkAccess(AccessType::SYSTEM_RESTORE_REPLICA, table_id);

    const zkutil::ZooKeeperPtr& zookeeper = getContext()->getZooKeeper();

    if (zookeeper->expired())
        throw Exception(ErrorCodes::NO_ZOOKEEPER,
            "Cannot restore table metadata because ZooKeeper session has expired");

    const StoragePtr table_ptr = DatabaseCatalog::instance().getTable(table_id, getContext());

    auto * const table_replicated_ptr = dynamic_cast<StorageReplicatedMergeTree *>(table_ptr.get());

    if (table_replicated_ptr == nullptr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, table_is_not_replicated.data(), table_id.getNameForLogs());

    auto & table_replicated = *table_replicated_ptr;

    StorageReplicatedMergeTree::Status status;
    table_replicated.getStatus(status);

    if (!status.is_readonly)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replica must be readonly");

    const String replica_name = table_replicated.getReplicaName();
    const String& zk_root_path = status.zookeeper_path;

    if (String replica_path = zk_root_path + "replicas/" + replica_name; zookeeper->exists(replica_path))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Replica path is present at {} -- nothing to restore. "
            "If you are sure that metadata it lost and replica path contain some garbage, "
            "then use SYSTEM DROP REPLICA query first.", replica_path);

    table_replicated.restoreMetadataInZooKeeper();
}

StoragePtr InterpreterSystemQuery::tryRestartReplica(const StorageID & replica, ContextMutablePtr system_context, bool need_ddl_guard)
{
    getContext()->checkAccess(AccessType::SYSTEM_RESTART_REPLICA, replica);

    auto table_ddl_guard = need_ddl_guard
        ? DatabaseCatalog::instance().getDDLGuard(replica.getDatabaseName(), replica.getTableName())
        : nullptr;

    auto [database, table] = DatabaseCatalog::instance().tryGetDatabaseAndTable(replica, getContext());
    ASTPtr create_ast;

    /// Detach actions
    if (!table)
        return nullptr;
    if (!dynamic_cast<const StorageReplicatedMergeTree *>(table.get()))
        return nullptr;

    table->flushAndShutdown();
    {
        /// If table was already dropped by anyone, an exception will be thrown
        auto table_lock = table->lockExclusively(getContext()->getCurrentQueryId(), getContext()->getSettingsRef().lock_acquire_timeout);
        create_ast = database->getCreateTableQuery(replica.table_name, getContext());

        database->detachTable(replica.table_name);

        MergeTreeData * storage = dynamic_cast<MergeTreeData *>(table.get());

        if (storage && storage->getMetastore())
            storage->getMetastore()->closeMetastore();
    }
    table.reset();

    /// Attach actions
    /// getCreateTableQuery must return canonical CREATE query representation, there are no need for AST postprocessing
    auto & create = create_ast->as<ASTCreateQuery &>();
    create.attach = true;

    auto columns = InterpreterCreateQuery::getColumnsDescription(*create.columns_list->columns, system_context, true, true);
    auto constraints = InterpreterCreateQuery::getConstraintsDescription(create.columns_list->constraints);
    auto foreign_keys = InterpreterCreateQuery::getForeignKeysDescription(create.columns_list->foreign_keys);
    auto unique = InterpreterCreateQuery::getUniqueNotEnforcedDescription(create.columns_list->unique);
    auto data_path = database->getTableDataPath(create);

    table = StorageFactory::instance().get(
        create, data_path, system_context, system_context->getGlobalContext(), columns, constraints, foreign_keys, unique, false);

    database->attachTable(replica.table_name, table, data_path);

    table->startup();
    return table;
}

void InterpreterSystemQuery::restartReplicas(ContextMutablePtr system_context)
{
    std::vector<StorageID> replica_names;
    auto & catalog = DatabaseCatalog::instance();

    for (auto & elem : catalog.getDatabases(getContext()))
        for (auto it = elem.second->getTablesIterator(getContext()); it->isValid(); it->next())
            if (dynamic_cast<const StorageReplicatedMergeTree *>(it->table().get()))
                replica_names.emplace_back(it->databaseName(), it->name());

    if (replica_names.empty())
        return;

    TableGuards guards;

    for (const auto & name : replica_names)
        guards.emplace(UniqueTableName{name.database_name, name.table_name}, nullptr);

    for (auto & guard : guards)
        guard.second = catalog.getDDLGuard(guard.first.database_name, guard.first.table_name);

    ThreadPool pool(std::min(size_t(getNumberOfPhysicalCPUCores()), replica_names.size()));

    for (auto & replica : replica_names)
    {
        LOG_TRACE(log, "Restarting replica on {}", replica.getNameForLogs());
        pool.scheduleOrThrowOnError([&]() { tryRestartReplica(replica, system_context, false); });
    }
    pool.wait();
}

void InterpreterSystemQuery::dropReplica(ASTSystemQuery & query)
{
    if (query.replica.empty())
        throw Exception("Replica name is empty", ErrorCodes::BAD_ARGUMENTS);

    if (!table_id.empty())
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA, table_id);
        StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

        if (!dropReplicaImpl(query, table))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, table_is_not_replicated.data(), table_id.getNameForLogs());
    }
    else if (!query.database.empty())
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA, query.database);
        DatabasePtr database = DatabaseCatalog::instance().getDatabase(query.database, getContext());
        for (auto iterator = database->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
            dropReplicaImpl(query, iterator->table());
        LOG_TRACE(log, "Dropped replica {} from database {}", query.replica, backQuoteIfNeed(database->getDatabaseName()));
    }
    else if (query.is_drop_whole_replica)
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA);
        auto databases = DatabaseCatalog::instance().getDatabases(getContext());

        for (auto & elem : databases)
        {
            DatabasePtr & database = elem.second;
            for (auto iterator = database->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
                dropReplicaImpl(query, iterator->table());
            LOG_TRACE(log, "Dropped replica {} from database {}", query.replica, backQuoteIfNeed(database->getDatabaseName()));
        }
    }
    else if (!query.replica_zk_path.empty())
    {
        getContext()->checkAccess(AccessType::SYSTEM_DROP_REPLICA);
        String remote_replica_path = fs::path(query.replica_zk_path)  / "replicas" / query.replica;

        /// This check is actually redundant, but it may prevent from some user mistakes
        for (auto & elem : DatabaseCatalog::instance().getDatabases(getContext()))
        {
            DatabasePtr & database = elem.second;
            for (auto iterator = database->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
            {
                if (auto * storage_replicated = dynamic_cast<StorageReplicatedMergeTree *>(iterator->table().get()))
                {
                    StorageReplicatedMergeTree::Status status;
                    storage_replicated->getStatus(status);
                    if (status.zookeeper_path == query.replica_zk_path)
                        throw Exception("There is a local table " + storage_replicated->getStorageID().getNameForLogs() +
                                        ", which has the same table path in ZooKeeper. Please check the path in query. "
                                        "If you want to drop replica of this table, use `DROP TABLE` "
                                        "or `SYSTEM DROP REPLICA 'name' FROM db.table`", ErrorCodes::TABLE_WAS_NOT_DROPPED);
                }
            }
        }

        auto zookeeper = getContext()->getZooKeeper();

        bool looks_like_table_path = zookeeper->exists(query.replica_zk_path + "/replicas") ||
                                     zookeeper->exists(query.replica_zk_path + "/dropped");
        if (!looks_like_table_path)
            throw Exception("Specified path " + query.replica_zk_path + " does not look like a table path",
                            ErrorCodes::TABLE_WAS_NOT_DROPPED);

        if (zookeeper->exists(remote_replica_path + "/is_active"))
            throw Exception("Can't remove replica: " + query.replica + ", because it's active",
                ErrorCodes::TABLE_WAS_NOT_DROPPED);

        StorageReplicatedMergeTree::dropReplica(zookeeper, query.replica_zk_path, query.replica, log);
        LOG_INFO(log, "Dropped replica {}", remote_replica_path);
    }
    else
        throw Exception("Invalid query", ErrorCodes::LOGICAL_ERROR);
}

bool InterpreterSystemQuery::dropReplicaImpl(ASTSystemQuery & query, const StoragePtr & table)
{
    auto * storage_replicated = dynamic_cast<StorageReplicatedMergeTree *>(table.get());
    if (!storage_replicated)
        return false;

    StorageReplicatedMergeTree::Status status;
    auto zookeeper = getContext()->getZooKeeper();
    storage_replicated->getStatus(status);

    /// Do not allow to drop local replicas and active remote replicas
    if (query.replica == status.replica_name)
        throw Exception("We can't drop local replica, please use `DROP TABLE` "
                        "if you want to clean the data and drop this replica", ErrorCodes::TABLE_WAS_NOT_DROPPED);

    /// NOTE it's not atomic: replica may become active after this check, but before dropReplica(...)
    /// However, the main usecase is to drop dead replica, which cannot become active.
    /// This check prevents only from accidental drop of some other replica.
    if (zookeeper->exists(status.zookeeper_path + "/replicas/" + query.replica + "/is_active"))
        throw Exception("Can't drop replica: " + query.replica + ", because it's active",
                        ErrorCodes::TABLE_WAS_NOT_DROPPED);

    storage_replicated->dropReplica(zookeeper, status.zookeeper_path, query.replica, log);
    LOG_TRACE(log, "Dropped replica {} of {}", query.replica, table->getStorageID().getNameForLogs());

    return true;
}

void InterpreterSystemQuery::recalculateMetrics(ASTSystemQuery & query)
{
    getContext()->checkAccess(AccessType::SYSTEM_RECALCULATE_METRICS, table_id);

    auto mgr = getContext()->getPartCacheManager();
    if (!mgr)
        throw Exception("No PartCacheManager found", ErrorCodes::LOGICAL_ERROR);

    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table) {
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} not found.", quoteString(query.database), quoteString(query.table));
    }
    mgr->forceRecalculate(table);
}

void InterpreterSystemQuery::syncReplica(ASTSystemQuery &)
{
    getContext()->checkAccess(AccessType::SYSTEM_SYNC_REPLICA, table_id);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

    if (auto * storage_replicated = dynamic_cast<StorageReplicatedMergeTree *>(table.get()))
    {
        LOG_TRACE(log, "Synchronizing entries in replica's queue with table's log and waiting for it to become empty");
        if (!storage_replicated->waitForShrinkingQueueSize(0, getContext()->getSettingsRef().receive_timeout.totalMilliseconds()))
        {
            LOG_ERROR(log, "SYNC REPLICA {}: Timed out!", table_id.getNameForLogs());
            throw Exception(
                    "SYNC REPLICA " + table_id.getNameForLogs() + ": command timed out! "
                    "See the 'receive_timeout' setting", ErrorCodes::TIMEOUT_EXCEEDED);
        }
        LOG_TRACE(log, "SYNC REPLICA {}: OK", table_id.getNameForLogs());
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, table_is_not_replicated.data(), table_id.getNameForLogs());
}

void InterpreterSystemQuery::flushDistributed(ASTSystemQuery &)
{
    getContext()->checkAccess(AccessType::SYSTEM_FLUSH_DISTRIBUTED, table_id);

    if (auto * storage_distributed = dynamic_cast<StorageDistributed *>(DatabaseCatalog::instance().getTable(table_id, getContext()).get()))
        storage_distributed->flushClusterNodesAllData(getContext());
    else
        throw Exception("Table " + table_id.getNameForLogs() + " is not distributed", ErrorCodes::BAD_ARGUMENTS);
}

void InterpreterSystemQuery::restartDisk(String & name)
{
    getContext()->checkAccess(AccessType::SYSTEM_RESTART_DISK);

    auto disk = getContext()->getDisk(name);

    if (DiskRestartProxy * restart_proxy = dynamic_cast<DiskRestartProxy*>(disk.get()))
        restart_proxy->restart();
    else
        throw Exception("Disk " + name + " doesn't have possibility to restart", ErrorCodes::BAD_ARGUMENTS);
}

void InterpreterSystemQuery::executeMetastoreCmd(ASTSystemQuery & query) const
{
    switch (query.meta_ops.operation)
    {
        case MetastoreOperation::START_AUTO_SYNC:
            getContext()->getGlobalContext()->setMetaCheckerStatus(false);
            break;
        case MetastoreOperation::STOP_AUTO_SYNC:
            getContext()->getGlobalContext()->setMetaCheckerStatus(true);
            break;
        case MetastoreOperation::SYNC:
        {
            StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());
            MergeTreeData * storage = dynamic_cast<MergeTreeData *>(table.get());
            if (storage)
                storage->syncMetaData();
            break;
        }
        case MetastoreOperation::DROP_ALL_KEY:
            /// TODO : implement later. directly remove all metadata for one table.
            throw Exception("Not implemented yet. ", ErrorCodes::NOT_IMPLEMENTED);
        case MetastoreOperation::DROP_BY_KEY:
        {
            StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());
            MergeTreeData * storage = dynamic_cast<MergeTreeData *>(table.get());
            if (!storage)
                return;
            if (auto metastore = storage->getMetastore())
            {
                if (!query.meta_ops.drop_key.empty())
                    metastore->dropMetaData(*storage, query.meta_ops.drop_key);
            }
            break;
        }
        default:
            throw Exception("Unknown metastore operation.", ErrorCodes::LOGICAL_ERROR);
    }
}

void InterpreterSystemQuery::executeCleanTrashTable(const ASTSystemQuery & query)
{
    auto local_context = getContext();
    // note: don't use table_id because it's not from trash
    StorageID id(query.database, query.table, query.table_uuid);
    if (id.database_name.empty())
        id.database_name = local_context->getCurrentDatabase();
    GlobalGCManager::systemCleanTrash(local_context, id, log);
}

void InterpreterSystemQuery::executeGc(const ASTSystemQuery & query)
{
    auto local_context = getContext();
    if (auto server_type = local_context->getServerType(); server_type != ServerType::cnch_server)
        throw Exception("SYSTEM GC is only available on CNCH server", ErrorCodes::NOT_IMPLEMENTED);

    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());
    /// Try to forward query to the target server if needed
    if (getContext()->getSettings().enable_auto_query_forwarding)
    {
        auto cnch_table_helper = CnchStorageCommonHelper(table->getStorageID(), table->getDatabaseName(), table->getTableName());
        if (cnch_table_helper.forwardQueryToServerIfNeeded(getContext(), table->getStorageID()))
            return;
    }

    CnchPartGCThread gc_thread(local_context, table->getStorageID());
    gc_thread.executeManually(query.partition, local_context);
}

void InterpreterSystemQuery::executeCheckpoint(const ASTSystemQuery & )
{
    auto local_context = getContext();
    if (auto server_type = local_context->getServerType(); server_type != ServerType::cnch_server)
        throw Exception("SYSTEM CHECKPOINT is only available on CNCH server", ErrorCodes::NOT_IMPLEMENTED);

    auto storage = DatabaseCatalog::instance().getTable(table_id, local_context);
    CnchManifestCheckpointThread checkpoint_thread(local_context, storage->getStorageID());
    checkpoint_thread.executeManually();
}

void InterpreterSystemQuery::dedupWithHighPriority(const ASTSystemQuery & query)
{
    if (getContext()->getServerType() != ServerType::cnch_server)
        throw Exception("SYSTEM DEDUP WITH HIGH PRIORITY is only available on CNCH server", ErrorCodes::NOT_IMPLEMENTED);

    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

    /// Try to forward query to the target server if needs to
    if (getContext()->getSettings().enable_auto_query_forwarding)
    {
        auto cnch_table_helper = CnchStorageCommonHelper(table->getStorageID(), table->getDatabaseName(), table->getTableName());
        if (cnch_table_helper.forwardQueryToServerIfNeeded(getContext(), table->getStorageID()))
            return;
    }

    auto dedup_thread = getContext()->getCnchBGThreadsMap(CnchBGThread::DedupWorker)->tryGetThread(table->getStorageID());
    if (auto dedup_worker_manager = dynamic_cast<DedupWorkerManager *>(dedup_thread.get()))
        dedup_worker_manager->dedupWithHighPriority(query.partition, getContext());
}

void InterpreterSystemQuery::executeDedup(const ASTSystemQuery & query)
{
    if (auto server_type = getContext()->getServerType(); server_type != ServerType::cnch_server && server_type != ServerType::cnch_worker)
        throw Exception("SYSTEM DEDUP is only available on CNCH server or worker", ErrorCodes::NOT_IMPLEMENTED);

    auto storage = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(storage.get()))
        cnch_table->executeDedupForRepair(query, getContext());
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {} is not a CnchMergeTree", table_id.getNameForLogs());
}

void InterpreterSystemQuery::dumpCnchServerStatus()
{
    auto context = getContext();
    auto server_manager = context->getCnchServerManager();
    if (server_manager)
        server_manager->dumpServerStatus();
    auto topology_master = context->getCnchTopologyMaster();
    if (topology_master)
        topology_master->dumpStatus();
}

void InterpreterSystemQuery::dropCnchMetaCache(bool skip_part_cache, bool skip_delete_bitmap_cache)
{
    auto local_context = getContext();
    if (local_context->getPartCacheManager())
    {
        auto storage = DatabaseCatalog::instance().getTable(table_id, local_context);
        local_context->getPartCacheManager()->invalidPartAndDeleteBitmapCache(
            storage->getStorageUUID(), skip_part_cache, skip_delete_bitmap_cache);
        LOG_DEBUG(log, "Dropped cnch part cache of table {}", table_id.getNameForLogs());
    }
}

void InterpreterSystemQuery::dropChecksumsCache(const StorageID & table_id) const
{
    auto checksums_cache = getContext()->getChecksumsCache();

    if (!checksums_cache)
        return;

    if (table_id.empty())
    {
        LOG_INFO(log, "Reset checksums cache.");
        checksums_cache->reset();
    }
    else
    {
        StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());
        MergeTreeData * storage = dynamic_cast<MergeTreeData *>(table.get());
        if (storage)
        {
            LOG_INFO(log, "Drop checksums cache for table {}", table_id.getFullNameNotQuoted());
            checksums_cache->dropChecksumCache(storage->getStorageUniqueID());
        }
    }
}

void InterpreterSystemQuery::clearBrokenTables(ContextMutablePtr & ) const
{
    const auto databases = DatabaseCatalog::instance().getDatabases(getContext());

    for (const auto & elem : databases)
        elem.second->clearBrokenTables();
}

AccessRightsElements InterpreterSystemQuery::getRequiredAccessForDDLOnCluster() const
{
    const auto & query = query_ptr->as<const ASTSystemQuery &>();
    using Type = ASTSystemQuery::Type;
    AccessRightsElements required_access;

    switch (query.type)
    {
        case Type::SHUTDOWN: [[fallthrough]];
        case Type::KILL: [[fallthrough]];
        case Type::SUSPEND:
        {
            required_access.emplace_back(AccessType::SYSTEM_SHUTDOWN);
            break;
        }
        case Type::DROP_DNS_CACHE: [[fallthrough]];
        case Type::DROP_MARK_CACHE: [[fallthrough]];
        case Type::DROP_MMAP_CACHE: [[fallthrough]];
        case Type::DROP_QUERY_CACHE:
            [[fallthrough]];
        case Type::DROP_CHECKSUMS_CACHE: [[fallthrough]];
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE: [[fallthrough]];
#endif
        case Type::DROP_NVM_CACHE: [[fallthrough]];
        case Type::DROP_SCHEMA_CACHE: [[fallthrough]];
        case Type::DROP_UNCOMPRESSED_CACHE:
        {
            required_access.emplace_back(AccessType::SYSTEM_DROP_CACHE);
            break;
        }
        case Type::RELOAD_DICTIONARY: [[fallthrough]];
        case Type::RELOAD_DICTIONARIES: [[fallthrough]];
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_DICTIONARY);
            break;
        }
        case Type::RELOAD_MODEL: [[fallthrough]];
        case Type::RELOAD_MODELS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_MODEL);
            break;
        }
        case Type::RELOAD_CONFIG:
        case Type::RELOAD_FORMAT_SCHEMA:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_CONFIG);
            break;
        }
        case Type::RELOAD_SYMBOLS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_SYMBOLS);
            break;
        }
        case Type::STOP_MERGES: [[fallthrough]];
        case Type::START_MERGES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_MERGES);
            else
                required_access.emplace_back(AccessType::SYSTEM_MERGES, query.database, query.table);
            break;
        }
        case Type::STOP_TTL_MERGES: [[fallthrough]];
        case Type::START_TTL_MERGES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_TTL_MERGES);
            else
                required_access.emplace_back(AccessType::SYSTEM_TTL_MERGES, query.database, query.table);
            break;
        }
        case Type::STOP_MOVES: [[fallthrough]];
        case Type::START_MOVES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_MOVES);
            else
                required_access.emplace_back(AccessType::SYSTEM_MOVES, query.database, query.table);
            break;
        }
        case Type::STOP_FETCHES: [[fallthrough]];
        case Type::START_FETCHES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_FETCHES);
            else
                required_access.emplace_back(AccessType::SYSTEM_FETCHES, query.database, query.table);
            break;
        }
        case Type::STOP_DISTRIBUTED_SENDS: [[fallthrough]];
        case Type::START_DISTRIBUTED_SENDS:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_DISTRIBUTED_SENDS);
            else
                required_access.emplace_back(AccessType::SYSTEM_DISTRIBUTED_SENDS, query.database, query.table);
            break;
        }
        case Type::STOP_REPLICATED_SENDS: [[fallthrough]];
        case Type::START_REPLICATED_SENDS:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_REPLICATED_SENDS);
            else
                required_access.emplace_back(AccessType::SYSTEM_REPLICATED_SENDS, query.database, query.table);
            break;
        }
        case Type::STOP_REPLICATION_QUEUES: [[fallthrough]];
        case Type::START_REPLICATION_QUEUES:
        {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_REPLICATION_QUEUES);
            else
                required_access.emplace_back(AccessType::SYSTEM_REPLICATION_QUEUES, query.database, query.table);
            break;
        }
        case Type::DROP_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_DROP_REPLICA, query.database, query.table);
            break;
        }
        case Type::RESTORE_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTORE_REPLICA, query.database, query.table);
            break;
        }
        case Type::SYNC_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_SYNC_REPLICA, query.database, query.table);
            break;
        }
        case Type::RESTART_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTART_REPLICA, query.database, query.table);
            break;
        }
        case Type::RESTART_REPLICAS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTART_REPLICA);
            break;
        }
        case Type::RECALCULATE_METRICS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RECALCULATE_METRICS, query.database, query.table);
            break;
        }
        case Type::FLUSH_DISTRIBUTED:
        {
            required_access.emplace_back(AccessType::SYSTEM_FLUSH_DISTRIBUTED, query.database, query.table);
            break;
        }
        case Type::FLUSH_LOGS:
        {
            required_access.emplace_back(AccessType::SYSTEM_FLUSH_LOGS);
            break;
        }
        case Type::RESTART_DISK:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTART_DISK);
            break;
        }
        case Type::METASTORE:
        case Type::CLEAR_BROKEN_TABLES:
        /// TODO:
        break;

        case Type::START_CONSUME: [[fallthrough]];
        case Type::STOP_CONSUME: [[fallthrough]];
        case Type::DROP_CONSUME: [[fallthrough]];
        case Type::RESTART_CONSUME:
        {
            required_access.emplace_back(AccessType::SYSTEM_CONSUME, query.database, query.table);
            break;
        }

        case Type::FETCH_PARTS:
        {
            required_access.emplace_back(AccessType::INSERT, query.database, query.table);
            break;
        }

        case Type::STOP_LISTEN_QUERIES: break;
        case Type::START_LISTEN_QUERIES: break;
        // FIXME(xuruiliang): fix for resource group
        case Type::START_RESOURCE_GROUP: break;
        case Type::STOP_RESOURCE_GROUP: break;
        case Type::DEDUP: break;
        case Type::UNKNOWN: break;
        case Type::END: break;
        default:
            break;
    }
    return required_access;
}

void InterpreterSystemQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & /*ast*/, ContextPtr) const
{
    elem.query_kind = "System";
}

void InterpreterSystemQuery::fetchParts(const ASTSystemQuery & query, const StorageID & table_id, ContextPtr local_context)
{

    if (table_id.empty())
        throw Exception("Database and table must be specified when fetch parts from remote.", ErrorCodes::LOGICAL_ERROR);

    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, local_context);

    auto merge_tree_storage = dynamic_cast<MergeTreeData *>(table.get());

    if (!merge_tree_storage)
        throw Exception("FetchParts only support MergeTree family, but got " + table->getName(), ErrorCodes::LOGICAL_ERROR);

    String hdfs_base = query.target_path->as<ASTLiteral &>().value.safeGet<String>();

    String prefix, fuzzyname;
    size_t pos = hdfs_base.find_last_of('/');
    if (pos == hdfs_base.length() - 1)
    {
        size_t lpos = hdfs_base.rfind('/', pos - 1);
        prefix = hdfs_base.substr(0, lpos + 1);
        fuzzyname = hdfs_base.substr(lpos + 1, pos - lpos - 1);
    }
    else
    {
        prefix = hdfs_base.substr(0, pos + 1);
        fuzzyname = hdfs_base.substr(pos + 1);
    }

    /// hard code max subdir size
    std::vector<String> matched = parseDescription(fuzzyname, 0, fuzzyname.length(), ',', 100);

    auto disks = merge_tree_storage->getDisksByType(DiskType::Type::Local);

    if (disks.empty())
        throw Exception("Cannot fetch parts to storage with no local disk.", ErrorCodes::LOGICAL_ERROR);

    Poco::UUIDGenerator & generator = Poco::UUIDGenerator::defaultGenerator();
    HDFSDumper dumper(getContext()->getHdfsUser(), getContext()->getHdfsNNProxy());
    for (auto const & remote_dir : matched)
    {
        String remote_path = prefix + remote_dir + "/";
        String part_relative_path = "detached/" + generator.createRandom().toString() + "/";

        SCOPE_EXIT(
        {
            // clean tmp directory, which is created during fetching part from remote
            for (const auto & disk: disks)
            {
                Poco::File local_tmp_path(fullPath(disk, fs::path(merge_tree_storage->getRelativeDataPath(IStorage::StorageLocation::MAIN)) / part_relative_path));
                local_tmp_path.remove(true);
            }
        });

        /// There may have network exceptions when fetching parts from remote, just try again it happens.
        std::vector<std::pair<String, DiskPtr>> fetched;
        int max_retry = 2;
        while (max_retry)
        {
            try
            {
                fetched = dumper.fetchPartsFromRemote(disks, remote_path, fs::path(merge_tree_storage->getRelativeDataPath(IStorage::StorageLocation::MAIN)) / part_relative_path);
                break;
            }
            catch (Exception & e)
            {
                if (--max_retry)
                {
                    LOG_DEBUG(log, "Failed to fetch from remote, error : {}, try again", e.message());
                }
                else
                    throw;
            }
        }
        // attach fetched part
        merge_tree_storage->attachPartsInDirectory(fetched, part_relative_path, local_context);
    }

}

namespace
{

template<typename T>
void executeActionOnCNCHLogImpl(std::shared_ptr<T> cnch_log, ASTSystemQuery::Type type, const String & table_name , LoggerPtr log)
{
    using Type = ASTSystemQuery::Type;
    if (cnch_log)
    {
        switch(type)
        {
            case Type::FLUSH_CNCH_LOG:
                cnch_log->flush(true);
                LOG_INFO(log, "flush cnch log for {}", table_name);
                break;
            case Type::STOP_CNCH_LOG:
                cnch_log->stop();
                LOG_INFO(log, "stop cnch log for {}", table_name);
                break;
            case Type::RESUME_CNCH_LOG:
                cnch_log->resume();
                LOG_INFO(log, "resume cnch log for {}", table_name);
                break;
            default:
                throw Exception("invalid action on log", ErrorCodes::LOGICAL_ERROR);
        }
    }
}
}

void InterpreterSystemQuery::executeActionOnCNCHLog(const String & table_name, ASTSystemQuery::Type type)
{
    if (table_name == CNCH_SYSTEM_LOG_KAFKA_LOG_TABLE_NAME)
        executeActionOnCNCHLogImpl(getContext()->getCloudKafkaLog(), type, table_name, log);
    else if (table_name == CNCH_SYSTEM_LOG_MATERIALIZED_MYSQL_LOG_TABLE_NAME)
        executeActionOnCNCHLogImpl(getContext()->getCloudMaterializedMySQLLog(), type, table_name, log);
    else if (table_name == CNCH_SYSTEM_LOG_UNIQUE_TABLE_LOG_TABLE_NAME)
        executeActionOnCNCHLogImpl(getContext()->getCloudUniqueTableLog(), type, table_name, log);
    else if (table_name == CNCH_SYSTEM_LOG_QUERY_LOG_TABLE_NAME)
        executeActionOnCNCHLogImpl(getContext()->getCnchQueryLog(), type, table_name, log);
    else if (table_name == CNCH_SYSTEM_LOG_VIEW_REFRESH_TASK_LOG_TABLE_NAME)
        executeActionOnCNCHLogImpl(getContext()->getViewRefreshTaskLog(), type, table_name, log);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "there is no log corresponding to table name {}, available names are {}, {}, {}, {}, {}",
            table_name,
            CNCH_SYSTEM_LOG_KAFKA_LOG_TABLE_NAME,
            CNCH_SYSTEM_LOG_MATERIALIZED_MYSQL_LOG_TABLE_NAME,
            CNCH_SYSTEM_LOG_UNIQUE_TABLE_LOG_TABLE_NAME,
            CNCH_SYSTEM_LOG_QUERY_LOG_TABLE_NAME,
            CNCH_SYSTEM_LOG_VIEW_REFRESH_TASK_LOG_TABLE_NAME);
}

void InterpreterSystemQuery::cleanTransaction(UInt64 txn_id)
{
    /// similar to daemon manager implementation
    TxnTimestamp current_time = getContext()->getTimestamp();
    auto catalog = getContext()->getCnchCatalog();
    auto & server_pool = getContext()->getCnchServerClientPool();
    const UInt64 safe_remove_interval = getContext()->getConfigRef().getInt("cnch_txn_safe_remove_seconds", 5 * 60); // default 5 min
    auto txn_record = catalog->getTransactionRecord(txn_id);
    try
    {
        auto client = server_pool.tryGetByRPCAddress(txn_record.location());
        bool server_exists = static_cast<bool>(client);
        if (!client)
            client = server_pool.get();

        LOG_TRACE(
            log,
            "Select server ({}) for txn {}, stats: {}, coordinator location: {}", client->getRPCAddress(), txn_record.txnID(), txnStatusToString(txn_record.status()), txn_record.location());

        switch (txn_record.status())
        {
            case CnchTransactionStatus::Aborted:
            {
                client->cleanTransaction(txn_record);
                break;
            }
            case CnchTransactionStatus::Finished:
            {
                if (!txn_record.cleanTs())
                {
                    if (txn_record.hasMainTableUUID())
                    {
                        auto table = getContext()->getCnchCatalog()->tryGetTableByUUID(*getContext(), UUIDHelpers::UUIDToString(txn_record.mainTableUUID()), txn_record.txnID());
                        auto server_vw_name = table ? table->getStorageID().server_vw_name : "server_vw_default";
                        auto host_port = getContext()->getCnchTopologyMaster()->getTargetServer(
                            UUIDHelpers::UUIDToString(txn_record.mainTableUUID()), server_vw_name, false);
                        client = server_pool.get(host_port);
                    }
                    client->cleanTransaction(txn_record);
                }
                else if (current_time.toSecond() - txn_record.cleanTs().toSecond() > safe_remove_interval) {
                    catalog->removeTransactionRecord(txn_record);
                }
                break;
            }
            case CnchTransactionStatus::Running:
            {
                if (!server_exists)
                {
                    client->cleanTransaction(txn_record);
                }
                else if (client->getTransactionStatus(txn_record.txnID()) == CnchTransactionStatus::Inactive)
                {
                    client->cleanTransaction(txn_record);
                }
                break;
            }
            default:
                throw Exception("Invalid status received", ErrorCodes::LOGICAL_ERROR);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void InterpreterSystemQuery::cleanFilesystemLock()
{
    auto lock_records = getContext()->getCnchCatalog()->getAllFilesysLock();
    for (const auto & record : lock_records)
    {
        cleanTransaction(record.txn_id());
    }
}

void InterpreterSystemQuery::lockMemoryLock(const ASTSystemQuery & query, const StorageID & table_id, ContextPtr local_context)
{
    /// SYSTEM LOCK MEMORY LOCK db.tb PARTITON '2012-01-01' FOR 4 SECONDS TASK_DOMAIN

    auto & txn_coordinator = local_context->getCnchTransactionCoordinator();
    auto transaction = txn_coordinator.createTransaction(
        CreateTransactionOption().setInitiator(CnchTransactionInitiator::Merge).setPriority(CnchTransactionPriority::low));

    SCOPE_EXIT({
        try
        {
            txn_coordinator.finishTransaction(transaction);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    });

    StoragePtr storage = local_context->getCnchCatalog()->tryGetTable(*local_context, table_id.database_name, table_id.table_name);
    if (!storage)
        throw Exception("Failed to get StoragePtr for table", ErrorCodes::BAD_ARGUMENTS);

    auto * merge_tree = dynamic_cast<StorageCnchMergeTree *>(storage.get());
    if (!merge_tree)
        throw Exception("storage is not merge tree table", ErrorCodes::LOGICAL_ERROR);

    String partition_id = merge_tree->getPartitionIDFromQuery(query.partition, local_context);
    LOG_DEBUG(log, "execute lock Memory lock on partition_id {} on table {} for {} s with string data {}", partition_id, table_id.getFullTableName(), query.seconds, query.string_data);

    TxnTimestamp txn_id = transaction->getTransactionID();
    LockInfoPtr partition_lock = std::make_shared<LockInfo>(txn_id);
    partition_lock->setMode(LockMode::X);
    partition_lock->setTimeout(1000); //1 seconds
    partition_lock->setPartition(partition_id);
    partition_lock->setUUIDAndPrefix(storage->getStorageUUID(), query.string_data.empty() ? LockInfo::default_domain : LockInfo::task_domain);

    Stopwatch lock_watch;

    auto cnch_lock = std::make_shared<CnchLockHolder>(local_context, std::move(partition_lock));
    cnch_lock->lock();
    LOG_DEBUG(log, "Acquired lock in {} ms", lock_watch.elapsedMilliseconds());
    sleepForSeconds(query.seconds);
}

void InterpreterSystemQuery::releaseMemoryLock(const ASTSystemQuery & query, const StorageID & table_id, ContextPtr local_context)
{
    if (query.specify_txn)
    {
        /// SYSTEM RELEASE MEMORY LOCK OF TXN xxx;
        LockManager::instance().releaseLocksForTxn(query.txn_id, *local_context);
    }
    else
    {
        /// SYSTEM RELEASE MEMORY LOCK db.table;
        auto storage = DatabaseCatalog::instance().getTable(table_id, local_context);
        if (!dynamic_cast<StorageCnchMergeTree *>(storage.get()))
            throw Exception("StorageCnchMergeTree is expected, but got " + storage->getName(), ErrorCodes::BAD_ARGUMENTS);

        LockManager::instance().releaseLocksForTable(storage->getCnchStorageID(), *local_context);
    }
}

void InterpreterSystemQuery::triggerHDFSConfigUpdate()
{
    // only for internal use
    // HDFSConfigManager::instance().updateAll(true);
}
}
