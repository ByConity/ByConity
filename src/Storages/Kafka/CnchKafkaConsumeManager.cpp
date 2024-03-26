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

#include <Common/config.h>
#if USE_RDKAFKA

#include <Storages/Kafka/CnchKafkaConsumeManager.h>

#include <Catalog/Catalog.h>
#include <CloudServices/CnchWorkerClient.h>
#include <Databases/DatabasesCommon.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/CnchSystemLog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/StorageCnchMergeTree.h>
#include <Storages/Kafka/StorageCnchKafka.h>
#include <Storages/Kafka/KafkaCommon.h>
#include <Storages/StorageMaterializedView.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <TSO/TSOClient.h>

///#include <DaemonManager/DaemonManagerClient.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RDKAFKA_EXCEPTION;
    extern const int UNKNOWN_EXCEPTION;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_TABLE;
}

CnchKafkaConsumeManager::CnchKafkaConsumeManager(ContextPtr context_, const StorageID & storage_id_)
    : ICnchBGThread(context_, CnchBGThreadType::Consumer, storage_id_)
{
}

CnchKafkaConsumeManager::~CnchKafkaConsumeManager()
{
    try
    {
        stop();
    }
    catch(...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void CnchKafkaConsumeManager::preStart()
{
    /// This logic may be able to be pulled up
    auto istorage = getStorageFromCatalog();
    auto & storage = checkAndGetCnchKafka(istorage);
    if (!storage.tableIsActive())
        throw Exception(storage_id.getFullTableName() + " is not active now", ErrorCodes::LOGICAL_ERROR);
}

void CnchKafkaConsumeManager::clearData()
{
    std::lock_guard lock(consumer_info_mutex);
    stopConsumers();
    num_partitions_of_topics.clear();
}

/// The vw used for consumption of CnchKafka will have three possible settings:
/// 1. the `cnch_vw_write` of CnchKafka setting param if it has been changed;
/// 2. the `cnch_vw_write` of target table for CnchKafka;
/// 3. the default common `vw_write`
String CnchKafkaConsumeManager::getVWNameForConsumerTask(const StorageCnchKafka & kafka_table)
{
    const auto & kafka_settings = kafka_table.getSettings();
    if (kafka_settings.cnch_vw_write.changed)
        return kafka_settings.cnch_vw_write.value;

    try
    {
        /// TODO: Merge this code with `checkDependencies` to reduce code redundancy
        auto local_dependencies = getDependenciesFromCatalog(kafka_table.getStorageID());
        for (const auto & dependence : local_dependencies)
        {
            auto table = catalog->getTable(*createQueryContext(), dependence.getDatabaseName(), dependence.getTableName(), getContext()->getTimestamp());
            if (auto *mv = dynamic_cast<StorageMaterializedView*>(table.get()))
            {
                auto target_table = mv->getTargetTable();
                if (auto *cnch = dynamic_cast<StorageCnchMergeTree*>(target_table.get()))
                {
                    const auto & cnch_table_settings = cnch->getSettings();
                    if (cnch_table_settings->cnch_vw_write.changed)
                    {
                        LOG_DEBUG(log, "'cnch_vw_write' of target table has been changed, will use it as vw for consumer");
                        return cnch_table_settings->cnch_vw_write.value;
                    }
                }
            }
        }
    }
    catch(...)
    {
        tryLogCurrentException(log, "Failed to check `vw-write` of target table, will still use vw of CnchKafka");
    }

    return kafka_settings.cnch_vw_write.value;
}

void CnchKafkaConsumeManager::initConsumerScheduler()
{
    /// DatabaseCatalog::getTable API requires transaction in params context
    ///auto storage = DatabaseCatalog::instance().getTable(storage_id, getContext());
    auto storage = catalog->getTable(*createQueryContext(), storage_id.database_name, storage_id.table_name, getContext()->getTimestamp());
    auto *kafka_table = dynamic_cast<StorageCnchKafka*>(storage.get());
    if (!kafka_table)
        throw Exception("Expected StorageCnchKafka, but got: " + storage->getName(), ErrorCodes::LOGICAL_ERROR);

    auto vw_name = getVWNameForConsumerTask(*kafka_table);
    auto schedule_mode = kafka_table->getSettings().cnch_schedule_mode.value;
    LOG_INFO(log, "Setting kafka consumer vw name: {} with schedule mode: {}", vw_name, schedule_mode);

    if (schedule_mode == "random")
        consumer_scheduler = std::make_shared<KafkaConsumerSchedulerRandom>(vw_name, KafkaConsumerScheduleMode::Random, getContext());
    else if (schedule_mode == "hash")
        consumer_scheduler = std::make_shared<KafkaConsumerSchedulerHash>(vw_name, KafkaConsumerScheduleMode::Hash, getContext());
    else if (schedule_mode == "least_consumers")
        consumer_scheduler = std::make_shared<KafkaConsumerSchedulerLeastConsumers>(vw_name, KafkaConsumerScheduleMode::LeastConsumers, getContext());
    else
        throw Exception("Unsupported Kafka consumer schedule mode: " + schedule_mode \
                + ". Only random(default), hash and least_consumers are supported now", ErrorCodes::LOGICAL_ERROR);
}

void CnchKafkaConsumeManager::runImpl()
{
    try
    {
        auto istorage = getStorageFromCatalog();
        auto & storage = checkAndGetCnchKafka(istorage);
        iterate(storage);
    }
    catch (...)
    {
        logExceptionToCnchKafkaLog(getCurrentExceptionMessage(false), true);
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        {
            std::lock_guard lock(last_exception_mutex);
            last_exception = LocalDateTime(time(nullptr)).toString() + " : " + getCurrentExceptionMessage(false);
        }

        /// TODO: add settings of backoff strategy for table-level; now the longest wait time maybe 102s
        exception_occur_times = std::min(10ul, exception_occur_times + 1);
        const auto backoff_time_ms = 100 * (1 << exception_occur_times);
        LOG_WARNING(log, "Consume manager ({}) throws an exception and it will retry after {} ms this time"
                    , storage_id.getFullTableName(), backoff_time_ms);
        scheduled_task->scheduleAfter(backoff_time_ms);
        return;
    }

    exception_occur_times = 0;
    const auto CLEANUP_SLEEP_MS = 7 * 1000;
    scheduled_task->scheduleAfter(CLEANUP_SLEEP_MS);
}

void CnchKafkaConsumeManager::iterate(StorageCnchKafka & kafka_table)
{
    if (!checkDependencies(kafka_table.getStorageID()))
    {
        {
            std::lock_guard lock(consumer_info_mutex);

            if (!consumer_infos.empty())
            {
                stopConsumers();
                num_partitions_of_topics.clear();
            }
        }

        /// Here throw exception to take effect of backoff strategy
        throw Exception("Check dependencies failed for " + storage_id.getNameForLogs(), ErrorCodes::LOGICAL_ERROR);
    }

    bool partitions_changed{false};
    updatePartitionCountOfTopics(kafka_table, partitions_changed);

    if (partitions_changed)
        assignPartitionsToConsumers(kafka_table);

    std::exception_ptr exception;
    for (auto & info : consumer_infos)
    {
        std::unique_lock lock(info.mutex);

        if (info.worker_client)
            checkConsumerStatus(info, lock);

        /// DO NOT use `else if`, worker_client may be reset in `checkConsumerStatus`
        if (!info.worker_client)
            dispatchConsumerToWorker(kafka_table, info, lock, exception);
    }
    if (exception)
        std::rethrow_exception(std::move(exception));
}

ContextPtr CnchKafkaConsumeManager::createQueryContext()
{
    auto query_context = Context::createCopy(getContext());
    query_context->setQueryContext(query_context);
    query_context->setSessionContext(query_context);

    return query_context;
}

[[maybe_unused]]static bool hasBufferWorkerChanged(const HostWithPortsVec & lhs, const HostWithPortsVec & rhs)
{
    if (lhs.size() != rhs.size())
        return true;

    for (int i = 0; i < static_cast<int>(lhs.size()); ++i)
    {
        if (!isSameHost(lhs[i].getHost(), rhs[i].getHost()))
            return true;
    }

    return false;
}

bool CnchKafkaConsumeManager::checkTargetTable(const StorageCnchMergeTree * target_table)
{
    cloud_table_has_unique_key = target_table->getInMemoryMetadataPtr()->hasUniqueKey();
    return true;
}

bool CnchKafkaConsumeManager::checkDependencies(const StorageID & storage_id_)
{
    /// FIXME: dependencies of target table should also be checked
    /// We can only get dependencies of source table from catalog now

    /// check dependencies itself
    ConsumerDependencies catalog_dependencies = getDependenciesFromCatalog(storage_id_);
    if (catalog_dependencies.empty())
    {
        if (storage_id_ == this->storage_id)
        {
            LOG_DEBUG(log, "No dependencies found from catalog for {}", storage_id.getTableName());
            return false;
        }
        else
            return true;
    }

    /// update dependencies with new one
    if (storage_id_ == this->storage_id)
    {
        if (catalog_dependencies.size() > 1)
            throw Exception("Multi MVs/Target-Tables for the same kafka table is not supported now", ErrorCodes::LOGICAL_ERROR);
        std::lock_guard lock(state_mutex);
        dependencies = catalog_dependencies;
    }

    /// check each dependence, including target table and its dependencies
    for (const auto & dependence : catalog_dependencies)
    {
        auto table = catalog->getTable(*createQueryContext(), dependence.database_name, dependence.table_name, getContext()->getTimestamp());
        if (!table)
        {
            LOG_WARNING(log, "table {} not found", dependence.getNameForLogs());
            return false;
        }

        if (auto *mv = dynamic_cast<StorageMaterializedView*>(table.get()))
        {
            auto target_table = mv->tryGetTargetTable();
            if (!target_table)
            {
                LOG_WARNING(log, "target table for {} not exists", mv->getTargetTableName());
                return false;
            }

            /// target table should be CnchMergeTree
            auto *cnch_merge = dynamic_cast<StorageCnchMergeTree*>(target_table.get());
            if (!cnch_merge)
            {
                LOG_WARNING(log, "table type not matched for {}, CnchMergeTree is expected", target_table->getTableName());
                return false;
            }

            if (storage_id_ == this->storage_id && !checkTargetTable(cnch_merge))
                return false;
        }

        /// check its dependencies
        if (!checkDependencies(dependence))
            return false;
    }

    return true;
}

CnchKafkaConsumeManager::ConsumerDependencies CnchKafkaConsumeManager::getDependenciesFromCatalog(
    const StorageID & storage_id_)
{
    auto query_context = createQueryContext();

    ConsumerDependencies init_dependencies;
    auto start_time = getContext()->getTimestamp();

    auto catalog_client = getContext()->getCnchCatalog();
    if (!catalog_client)
        throw Exception("get catalog client failed", ErrorCodes::LOGICAL_ERROR);

    auto storage = catalog_client->getTable(*query_context, storage_id_.database_name, storage_id_.table_name, start_time);
    auto all_views_from_catalog = catalog_client->getAllViewsOn(*query_context, storage, start_time);
    if (all_views_from_catalog.empty())
        return {};

    for (auto & view : all_views_from_catalog)
    {
        if (auto * mv = dynamic_cast<StorageMaterializedView *>(view.get()))
        {
            if (mv->async())
                continue;
            init_dependencies.emplace(view->getStorageID());
        }
    }


    return init_dependencies;
}

CnchKafkaConsumeManager::ConsumerDependencies CnchKafkaConsumeManager::getDependencies() const
{
    std::lock_guard lock(state_mutex);
    return dependencies;
}

void CnchKafkaConsumeManager::updatePartitionCountOfTopics(StorageCnchKafka & kafka_table, bool & partitions_changed)
{
    if (!tool_consumer)
    {
        auto conf = Kafka::createConsumerConfiguration(getContext(), storage_id, kafka_table.getTopics(), kafka_table.getSettings());
        tool_consumer = std::make_shared<KafkaConsumer>(conf);
        /// Set a longer timeout as it's easy to get timeout while calling `get_metadata` with default setting 1s
        tool_consumer->set_timeout(std::chrono::milliseconds(5000));
    }

    try
    {
        std::map<String, size_t> new_num_partitions;
        for (const auto & topic_name : kafka_table.getTopics())
        {
            auto topic = tool_consumer->get_topic(topic_name);
            auto topic_metadata = tool_consumer->get_metadata(topic);
            auto partition_num = topic_metadata.get_partitions().size();

            if (num_partitions_of_topics.find(topic_name) != num_partitions_of_topics.end()
                && num_partitions_of_topics[topic_name] == partition_num)
            {
                new_num_partitions[topic_name] = partition_num;
                continue;
            }

            if (partition_num == 0)
            {
                if (num_partitions_of_topics.find(topic_name) != num_partitions_of_topics.end())
                    partitions_changed = true;

                LOG_INFO(log, "Topic {} has no partitions", topic_name);
                continue;
            }

            new_num_partitions[topic_name] = partition_num;
            max_needed_consumers = std::max(max_needed_consumers, partition_num);
            partitions_changed = true;

            LOG_TRACE(log, "Topic {} has partitions: ", topic_name, partition_num);
        }

        num_partitions_of_topics = std::move(new_num_partitions);
        if (num_partitions_of_topics.empty())
            throw Exception("No partitions found for " + kafka_table.getTableName(), ErrorCodes::LOGICAL_ERROR);
    }
    catch (const cppkafka::Exception & e)
    {
        throw Exception(String("Failed to get topic metadata: ") + e.what(), ErrorCodes::RDKAFKA_EXCEPTION);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to get topic metadata");
        throw;
    }
}

void CnchKafkaConsumeManager::assignPartitionsToConsumers(StorageCnchKafka & kafka_table)
{
    /// get sample consuming partitions list if has
    const auto & sample_partitions_list = kafka_table.getSampleConsumingPartitionList(num_partitions_of_topics);
    bool sample_consuming_enabled = !sample_partitions_list.empty();

    size_t consumers_num = kafka_table.getConsumersNum();
    consumers_num = std::min(consumers_num, max_needed_consumers);

    std::lock_guard lock(consumer_info_mutex);

    if (!consumer_infos.empty())
    {
        LOG_DEBUG(log, "Topic partitions have been changed, restart all consumers");

        stopConsumers();
    }

    /// We must ensure each consumer can be assigned a real partition to consume;
    /// Or the offsets cannot be committed when all messages are discarded
    if (unlikely(sample_consuming_enabled))
        consumers_num = std::min(consumers_num, sample_partitions_list.size());

    /// Assign the partitions not in the sample partition list to the consumers;
    for (size_t i = 0; i < consumers_num; ++i)
    {
        consumer_infos.emplace_back();
        auto & info = consumer_infos.back();
        info.index = i;

        for (auto & [topic, partition_cnt] : num_partitions_of_topics)
        {
            for (auto p = i; p < partition_cnt; p += consumers_num)
            {
                info.partitions.emplace_back(topic, p);
                if (unlikely(sample_consuming_enabled) && sample_partitions_list.contains(info.partitions.back()))
                    info.partitions.pop_back();
            }
        }
    }

    /// Assign the partitions in the sample partition list to the consuemrs specially
    /// as we need ensure that each consumer should be assigned a real partition
    if (unlikely(sample_consuming_enabled))
    {
        size_t sample_partition_idx = 0;
        for (const auto & tp : sample_partitions_list)
        {
            size_t info_idx = sample_partition_idx % consumers_num;
            ++sample_partition_idx;

            consumer_infos[info_idx].sample_partitions.emplace(tp);
            consumer_infos[info_idx].partitions.emplace_back(tp);
        }
    }
}

static String replaceCreateTableQuery(ContextPtr context, String & query, const String & new_table_name, const bool change_engine, bool enable_staging_area)
{
    const auto & context_settings = context->getSettings();
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, query, context_settings.max_query_size, context_settings.max_parser_depth);

    auto & create_query = ast->as<ASTCreateQuery &>();
    auto * storage = create_query.storage;

    if (change_engine)
    {
        auto engine = std::make_shared<ASTFunction>();
        const String & engine_name = create_query.storage->engine->name;
        if (engine_name.starts_with("Cnch") && engine_name.ends_with("MergeTree"))
        {
            engine->name = String(create_query.storage->engine->name).replace(0, strlen("Cnch"), "Cloud");
            engine->arguments = std::make_shared<ASTExpressionList>();
            engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(create_query.database));
            engine->arguments->children.push_back(std::make_shared<ASTIdentifier>(create_query.table));
            /// NOTE: Used to pass the version column for unique table here.
            if (create_query.storage->unique_key && create_query.storage->engine->arguments && create_query.storage->engine->arguments->children.size())
                engine->arguments->children.push_back(create_query.storage->engine->arguments->children[0]);

            /// set cnch uuid for CloudMergeTree to commit data on worker side
            if (!storage->settings)
            {
                storage->set(storage->settings, std::make_shared<ASTSetQuery>());
                storage->settings->is_standalone = false;
            }
            storage->settings->changes.push_back(SettingChange{"cnch_table_uuid",
                                                Field(static_cast<String>(UUIDHelpers::UUIDToString(create_query.uuid)))});
        }
        else if (engine_name == "CnchKafka")
            engine->name = String(create_query.storage->engine->name).replace(0, strlen("Cnch"), "Cloud");
        else
            throw Exception("Unknown table engine: " + engine_name, ErrorCodes::LOGICAL_ERROR);

        create_query.storage->set(create_query.storage->engine, engine);
    }

    create_query.table = new_table_name;

    /// It's not allowed to create multi tables with same uuid on Cnch-Worker side now
    create_query.uuid = UUIDHelpers::Nil;

    if (enable_staging_area)
    {
        storage->settings->changes.push_back(SettingChange{"cloud_enable_staging_area", Field(static_cast<UInt64>(1))});
    }

    return query = getTableDefinitionFromCreateQuery(ast, false);
}

[[maybe_unused]] static String replaceMaterializedViewQuery(StorageMaterializedView * mv, const StorageID & kafka_storage_id, const String & table_suffix)
{
    auto query = mv->getCreateTableSql();

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    auto & create_query = ast->as<ASTCreateQuery &>();
    create_query.table += table_suffix;
    create_query.to_table_id.table_name += table_suffix;
    create_query.uuid = UUIDHelpers::Nil;

    auto & inner_query = create_query.select->list_of_selects->children.at(0);
    if (!inner_query)
        throw Exception("select query is necessary for mv table", ErrorCodes::LOGICAL_ERROR);

    auto & select_query = inner_query->as<ASTSelectQuery &>();
    /// TODO: MV table may provide API to get StorageID of SELECT table
    select_query.replaceDatabaseAndTable(kafka_storage_id.getDatabaseName(), kafka_storage_id.getTableName() + table_suffix);

    return getTableDefinitionFromCreateQuery(ast, false);
}

void CnchKafkaConsumeManager::checkConsumerStatus(ConsumerInfo & info, std::unique_lock<std::mutex> & info_lock)
{
    StorageID worker_storage_id(storage_id.getDatabaseName(),
                                storage_id.getTableName() + info.table_suffix/* , storage_id.uuid */);

    CnchConsumerStatus status;
    auto worker_client = info.worker_client;
    try
    {
        /// check running-status of consumer first in case of failure of stopping/starting consume
        if (!info.is_running)
        {
            logExceptionToCnchKafkaLog("Consumer #" + toString(info.index) + " is not running now");
            throw Exception("Consumer #" + toString(info.index) + " is not running now", ErrorCodes::LOGICAL_ERROR);
        }

        {
            /// Do NOT hold lock for RPC call in case of some unexpected hang issues in rpc
            SCOPE_EXIT({
                info_lock.lock();
            });
            info_lock.unlock();

            status = worker_client->getConsumerStatus(worker_storage_id);
        }

        /// Minor check again as we have unlocked for some time, during which the consumer job may be stopped by some reasons
        if (!info.is_running || !info.worker_client)
            throw Exception("Consumer #" + toString(info.index) + " is not running now", ErrorCodes::LOGICAL_ERROR);

        if (!status.last_exception.empty())
        {
            std::lock_guard lock(last_exception_mutex);
            last_exception = LocalDateTime(time(nullptr)).toString() + " : " + status.last_exception;
        }

        LOG_DEBUG(log, "Check consumer status succ for {} on host {}", storage_id.getTableName() + info.table_suffix
                  , info.worker_client->getHostWithPortsID());

        /// Check if need to reschedule consumer to new worker
        if (consumer_scheduler->shouldReschedule(info.worker_client, info.table_suffix, info.index))
        {
            LOG_INFO(log, "Worker client for the consumer #{} has changed, try to reschedule it for load balance", info.index);
            info.worker_client = nullptr;
            info.is_running = false;
        }
    }
    catch (...)
    {
        consumer_scheduler->resetWorkerClient(worker_client);

        /// just reset worker client to restart consumer as consumer will check validity
        auto error_msg = "Check consumer status failed for " + worker_storage_id.getNameForLogs() + " due to: " \
                            + getCurrentExceptionMessage(false);
        LOG_ERROR(log, error_msg);
        logExceptionToCnchKafkaLog(error_msg);
        info.worker_client = nullptr;
        info.is_running = false;
    }
}

CnchWorkerClientPtr CnchKafkaConsumeManager::selectWorker(size_t index, const String & table_suffix)
{
    if (!consumer_scheduler)
        initConsumerScheduler();

    return consumer_scheduler->selectWorkerNode(table_suffix, index);
}

void CnchKafkaConsumeManager::getOffsetsFromCatalog(
    cppkafka::TopicPartitionList & offsets,
    const StorageID & /* buffer_table_id */,
    const String & consumer_group)
{
    /// First, get offsets from catalog
    getContext()->getCnchCatalog()->getKafkaOffsets(consumer_group, offsets);
}

StoragePtr CnchKafkaConsumeManager::rewriteCreateTableSQL(const DB::StorageID & dependence,
                                                    const StorageID & replace_storage_id,
                                                    const String & table_suffix,
                                                    std::vector<String> & create_commands,
                                                    bool enable_staging_area)
{
    StoragePtr target_table = nullptr;

    auto storage = getContext()->getCnchCatalog()->getTableByUUID(*getContext(), toString(dependence.uuid), getContext()->getTimestamp());
    if (auto * mv = dynamic_cast<StorageMaterializedView*>(storage.get()))
    {
        LOG_DEBUG(log, "Rewrite sql for dependence: {}", dependence.getNameForLogs());

        target_table = mv->getTargetTable();
        /// All other Cnch**MergeTree can be dynamic_cast as CnchMergeTree
        if (auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(target_table.get()))
        {
            auto crete_query = cnch_table->getCreateTableSql();
            replaceCreateTableQuery(getContext(), crete_query, cnch_table->getTableName() + table_suffix, true, enable_staging_area);
            create_commands.push_back(crete_query);

            create_commands.push_back(replaceMaterializedViewQuery(mv, replace_storage_id, table_suffix));

            if (!cnch_table->getInMemoryMetadataPtr()->hasUniqueKey())
            {
                /// The target table may have other MaterializedView tables and deeper sink tables,
                /// e.g CnchKafka -> MV1 -> CnchMergeTree1 -> MV2 -> CnchMergeTree2
                /// XXX: CnchUniqueKey don't support it now
                auto target_dependencies = getDependenciesFromCatalog(target_table->getStorageID());
                for (const auto & table_id : target_dependencies)
                    rewriteCreateTableSQL(table_id, cnch_table->getStorageID(), table_suffix, create_commands, false);
            }
        }
        else
            throw Exception("Only CnchMergeTree family is supported now", ErrorCodes::LOGICAL_ERROR);
    }

    return target_table;
}

void CnchKafkaConsumeManager::dispatchConsumerToWorker(StorageCnchKafka & kafka_table, ConsumerInfo & info,
                                                      std::unique_lock<std::mutex> & info_lock, std::exception_ptr & exception)
{
    /// When cnch-server is under high load, the transaction (RPC call) may need more time to execute commit action;
    /// If ConsumeManager launches a new consumer during this period, duplication consumption may occur.
    /// Thus, we need ensure that there is no active transactions before launching a new consumer to guarantee Exactly-Once.
    /// This checker is consumer-level; there is no conflict between consumers if the table has more than one consumers
    if (checkConsumerHasActiveTransaction(info.index))
    {
        LOG_WARNING(log, "Consumer #{} has active transaction now, we will retry after it finishes to avoid data duplication", info.index);
        return;
    }

    /// the suffix will be used to mark and check the uniqueness of consumer-table on worker client
    String table_suffix = '_' + toString(std::chrono::system_clock::now().time_since_epoch().count())
        + '_' + toString(info.index);

    /// Build query to create local tables
    auto create_kafka_query = kafka_table.getCreateTableSql();
    replaceCreateTableQuery(getContext(), create_kafka_query, kafka_table.getTableName() + table_suffix, true, false);

    KafkaTaskCommand command;
    command.type = KafkaTaskCommand::START_CONSUME;
    command.task_id = toString(info.index);
    command.rpc_port = getContext()->getRPCPort();
    command.cnch_storage_id = kafka_table.getStorageID();
    command.local_database_name = command.cnch_storage_id.database_name;
    command.local_table_name = command.cnch_storage_id.table_name + table_suffix;
    command.create_table_commands.push_back(create_kafka_query);

    bool enable_staging_area = cloud_table_has_unique_key && kafka_table.getSettings().enable_staging_area;
    StoragePtr target_table;
    for (const auto & dependency : dependencies)
        target_table = rewriteCreateTableSQL(dependency, this->storage_id, table_suffix, command.create_table_commands, enable_staging_area);
    if (!target_table)
        throw Exception("Unable to get target table for CnchKafka, this is unexpected", ErrorCodes::LOGICAL_ERROR);

    for (auto & query : command.create_table_commands)
    {
        LOG_TRACE(log, "debug query: {}", query);
    }

    /// Get latest offsets for topic-partitions
    getOffsetsFromCatalog(info.partitions, target_table->getStorageID(), kafka_table.getGroupForBytekv());

    for (auto & tp : info.partitions)
        LOG_TRACE(log, "topic: {}, partition: {}, offsets: {}", tp.get_topic(), tp.get_partition(), tp.get_offset());

    command.assigned_consumer = info.index;
    command.tpl = info.partitions;
    command.sample_partitions = info.sample_partitions;

    /// Send command to worker client to create local tables and start consume
    CnchWorkerClientPtr worker_client;
    try
    {
        worker_client = selectWorker(info.index, table_suffix);
        LOG_TRACE(log, "Selected worker {} for consumer #{}", worker_client->getHostWithPortsID(), info.index);

        {
            /// Do NOT hold lock for RPC call in case of some unexpected hang issues in rpc
            SCOPE_EXIT({
                info_lock.lock();
            });
            info_lock.unlock();

            worker_client->submitKafkaConsumeTask(command);
        }
    }
    catch (...)
    {
        if (consumer_scheduler)
            consumer_scheduler->resetWorkerClient(worker_client);

        /// Just catch exception here as consumers should not affect each other
        tryLogCurrentException(log, "Failed to dispatch consumer #" + std::to_string(info.index));
        if (!exception)
            exception = std::current_exception();
        return;
    }

    info.table_suffix = table_suffix;
    info.worker_client = worker_client;
    info.is_running = true;
    LOG_DEBUG(log, "Successfully send command 'START_CONSUME' to {}", worker_client->getHostWithPortsID());
}

bool CnchKafkaConsumeManager::checkWorkerClient(const String & consumer_table_name, size_t index) const
{
    std::lock_guard lock(consumer_info_mutex);
    if (index < consumer_infos.size())
    {
        const auto & info = consumer_infos[index];
        std::lock_guard lock_consumer(info.mutex);
        return (info.is_running && (storage_id.table_name + info.table_suffix) == consumer_table_name);
    }

    return false;
}

void CnchKafkaConsumeManager::stopConsumerOnWorker(ConsumerInfo & info)
{
    {
        std::lock_guard lock(info.mutex);
        if (!info.is_running || !info.worker_client)
        {
            LOG_INFO(log, "Consumer#{} of {} is not running, don't need stop", info.index, storage_id.getFullTableName());
            return;
        }
    }

    KafkaTaskCommand command;
    command.type = KafkaTaskCommand::STOP_CONSUME;
    command.task_id = toString(info.index);
    command.rpc_port = getContext()->getRPCPort();
    command.cnch_storage_id = storage_id;
    command.local_database_name = storage_id.getDatabaseName();
    command.local_table_name = storage_id.getTableName() + info.table_suffix;

    CnchWorkerClientPtr worker_client;
    {
        std::lock_guard lock(info.mutex);
        /// reset status of consumer-info first in case of exception of stopping consume
        info.is_running = false;

        worker_client = info.worker_client;
        info.worker_client = nullptr;
    }

    /// send stop-command to worker
    worker_client->submitKafkaConsumeTask(command);

    LOG_DEBUG(log, "Successfully send command 'STOP_CONSUME' to {}", worker_client->getHostWithPortsID());
}

void CnchKafkaConsumeManager::stopConsumers()
{
    /// there are two types of case here: 1.stop task manager; 2.stop all consumers to restart
    /// the task manager will be deleted later for case 1;
    /// the fail-to-be-stopped consumer will stop itself as validator fails for case 2;
    /// so, we can just ignore the exceptions during stopping consumers
    try
    {
        ThreadPool pool(std::min(consumer_infos.size(), getContext()->getSettings().max_threads.value));
        for (auto & info : consumer_infos)
        {
            pool.trySchedule([&c = info, this] { stopConsumerOnWorker(c); });
        }
        pool.wait();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    consumer_infos.clear();
    tool_consumer = nullptr;
    consumer_scheduler = nullptr;
}

String CnchKafkaConsumeManager::getLastException() const
{
    std::lock_guard lock(last_exception_mutex);
    return last_exception;
}

std::vector<KafkaConsumerRunningInfo> CnchKafkaConsumeManager::getConsumerInfos() const
{
    std::vector<KafkaConsumerRunningInfo> res;

    std::lock_guard lock(consumer_info_mutex);
    for (const auto & consumer : consumer_infos)
    {
        res.emplace_back();
        auto & info  = res.back();

        {
            std::lock_guard lock_consumer(consumer.mutex);
            info.is_running = consumer.is_running;
            info.table_suffix = consumer.table_suffix;
            info.partitions = consumer.partitions;
            std::transform(consumer.sample_partitions.begin(), consumer.sample_partitions.end(),
                    std::back_inserter(info.sample_partitions),
                         [](auto & tp) { return tp; });
            if (consumer.worker_client)
                info.worker_client_info = consumer.worker_client->getHostWithPortsID();
        }
    }

    return res;
}

void CnchKafkaConsumeManager::logExceptionToCnchKafkaLog(String msg, bool deduplicate)
{
    constexpr auto MSG_AGE_THRESHOLD_IN_SECOND = 60;
    try
    {
        auto cloud_kafka_log = getContext()->getCloudKafkaLog();
        if (!cloud_kafka_log)
            return;
        if (deduplicate)
        {
            size_t msg_hash = std::hash<std::string>{}(msg);
            time_t now = time(nullptr);
            if (last_exception_msg_hash.load() == msg_hash)
            {
                if ((now - last_exception_time.load()) < MSG_AGE_THRESHOLD_IN_SECOND)
                    return;
                else
                {
                    last_exception_time = now;
                    cloud_kafka_log->logException(storage_id, std::move(msg), "ConsumeManager");
                }
            }
            else
            {
                last_exception_time = now;
                last_exception_msg_hash = msg_hash;
                cloud_kafka_log->logException(storage_id, std::move(msg), "ConsumeManager");
            }
        }
        else
            cloud_kafka_log->logException(storage_id, std::move(msg), "ConsumeManager");
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void CnchKafkaConsumeManager::setCurrentTransactionForConsumer(size_t consumer_index, const TxnTimestamp & txn_id)
{
    catalog->setTransactionForKafkaConsumer(storage_id.uuid, txn_id, consumer_index);
}

bool CnchKafkaConsumeManager::checkConsumerHasActiveTransaction(size_t consumer_index)
{
    auto txn_id = catalog->getTransactionForKafkaConsumer(storage_id.uuid, consumer_index);
    if (txn_id == TxnTimestamp::maxTS())
        return false;

    auto txn_record_ptr = catalog->tryGetTransactionRecord(txn_id);
    if (!txn_record_ptr)
    {
        LOG_TRACE(log, "The latest transaction #{} for consumer #{} has finished", txn_id.toUInt64(), consumer_index);
        return false;
    }

    auto txn_record = txn_record_ptr.value();
    LOG_DEBUG(
        log,
        "Consumer #{} has the latest transaction: {} with status: {}",
         consumer_index, txn_id.toUInt64(), txnStatusToString(txn_record.status())
    );
    if (txn_record.status() == CnchTransactionStatus::Finished || txn_record.status() == CnchTransactionStatus::Aborted)
        return false;

    const auto MAX_ACTIVE_KAFKA_TXN_SEC = 60;
    auto txn_running_sec = time(nullptr) - txn_id.toSecond();
    if (txn_running_sec > MAX_ACTIVE_KAFKA_TXN_SEC)
    {
        LOG_WARNING(
            log,
            "Active transaction {} has run for {} seconds. We will try to rollback it to launch a new consumer.",
             txn_id.toUInt64(), txn_running_sec
        );
        /// Rollback transaction by API of catalog;
        /// it won't end transaction now, but the transaction will failed while committing as CAS failed and trigger real Rollback action then
        catalog->rollbackTransaction(txn_record);
    }

    return true;
}

} // namespace DB
#endif
