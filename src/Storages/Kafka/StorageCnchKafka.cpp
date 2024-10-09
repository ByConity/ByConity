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

#include <Storages/Kafka/StorageCnchKafka.h>

#include <DaemonManager/DaemonManagerClient.h>
#include <Databases/DatabaseOnDisk.h>
#include <Interpreters/InterpreterSystemQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/StorageMaterializedView.h>
#include <Transaction/Actions/DDLAlterAction.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <Transaction/CnchWorkerTransaction.h>

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

namespace DB
{

[[maybe_unused]] String & removeKafkaPrefix(String & name)
{
    if (startsWith(name, "kafka_"))
        name = name.substr(strlen("kafka_"));
    return name;
}

[[maybe_unused]] String & addKafkaPrefix(String & name)
{
    if (!startsWith(name, "kafka_"))
        name = "kafka_" + name;
    return name;
}

StorageCnchKafka::StorageCnchKafka(
    const StorageID & table_id_,
    ContextMutablePtr context_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const ASTPtr setting_changes_,
    const KafkaSettings & settings_)
    : IStorageCnchKafka(table_id_, context_, setting_changes_, settings_, columns_, constraints_),
      log(getLogger(table_id_.getNameForLogs()  + " (StorageCnchKafka)"))
{
}

void StorageCnchKafka::startup()
{
}

void StorageCnchKafka::shutdown()
{
}

void StorageCnchKafka::drop()
{
    auto cnch_catalog = getContext()->getCnchCatalog();
    const String & group = getGroupForBytekv();

    LOG_INFO(log, "Clear all offsets in catalog for group '{}' before dropping table: {}", group, getStorageID().getFullTableName());
    for (const auto & topic : getTopics())
        cnch_catalog->clearOffsetsForWholeTopic(topic, group);

    LOG_INFO(log, "Clear active transactions of consumers in catalog before dropping table {}", getStorageID().getFullTableName());
    cnch_catalog->clearKafkaTransactions(getStorageUUID());
}

void StorageCnchKafka::checkAlterIsPossible(const AlterCommands & commands, ContextPtr) const
{
    static std::set<AlterCommand::Type> cnchkafka_support_alter_types = {
        AlterCommand::ADD_COLUMN,
        AlterCommand::DROP_COLUMN,
        AlterCommand::MODIFY_COLUMN,
        AlterCommand::COMMENT_COLUMN,
        AlterCommand::MODIFY_SETTING,
        AlterCommand::RENAME_COLUMN,
        AlterCommand::ADD_CONSTRAINT,
        AlterCommand::DROP_CONSTRAINT,
    };

    for (const auto & command : commands)
    {
        if (!cnchkafka_support_alter_types.count(command.type))
            throw Exception("Alter of type '" + alterTypeToString(command.type) + "' is not supported by CnchKafka", ErrorCodes::NOT_IMPLEMENTED);

        LOG_INFO(log, "Executing CnchKafka ALTER command: {}", alterTypeToString(command.type));
    }

}

void StorageCnchKafka::alter(const AlterCommands & commands, ContextPtr local_context, TableLockHolder & /* alter_lock_holder */)
{
    auto daemon_manager = getGlobalContext()->getDaemonManagerClient();
    bool kafka_table_is_active = tableIsActive();

    const String full_name = getStorageID().getNameForLogs();

    SCOPE_EXIT({
        if (kafka_table_is_active)
        {
            usleep(500 * 1000);
            LOG_DEBUG(log, "Restart consumption no matter if ALTER succ for table {}", full_name);
            try
            {
                daemon_manager->controlDaemonJob(getStorageID(), CnchBGThreadType::Consumer, CnchBGThreadAction::Start, local_context->getCurrentQueryId());
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to restart consume for table " + full_name + " after ALTER");
            }
        }
    });

    if (kafka_table_is_active)
    {
        LOG_DEBUG(log, "Stop consumption before altering table {}", full_name);
        /// RPC timeout may occur here; remember to restart consume if needs to
        daemon_manager->controlDaemonJob(getStorageID(), CnchBGThreadType::Consumer, CnchBGThreadAction::Stop, local_context->getCurrentQueryId());
    }

    /// start alter
    LOG_DEBUG(log, "Start altering table {}", full_name);

    StorageInMemoryMetadata new_metadata = getInMemoryMetadataCopy();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadataCopy();

    TransactionCnchPtr txn = local_context->getCurrentTransaction();
    auto action = txn->createAction<DDLAlterAction>(shared_from_this(), local_context->getSettingsRef(), local_context->getCurrentQueryId());
    auto & alter_act = action->as<DDLAlterAction &>();
    alter_act.setMutationCommands(commands.getMutationCommands(
        old_metadata, false, local_context));

    bool alter_setting = commands.isSettingsAlter();
    /// Check setting changes if has
    auto new_settings = this->settings;
    for (const auto & c : commands)
    {
        if (c.type != AlterCommand::MODIFY_SETTING)
            continue;
        new_settings.applyKafkaSettingChanges(c.settings_changes);
    }

    checkAndLoadingSettings(new_settings);

    /// Add 'kafka_' prefix for changed settings
    AlterCommands new_commands = commands;
    if (alter_setting)
    {
        for (auto & command : new_commands)
        {
            if (command.type != AlterCommand::MODIFY_SETTING)
                continue;
            for (auto & change : command.settings_changes)
                addKafkaPrefix(change.name);
        }
    }

    /// Apply alter commands to metadata
    new_commands.apply(new_metadata, local_context);

    /// Apply alter commands to create-sql
    {
        String create_table_query = getCreateTableSql();
        alter_act.setOldSchema(create_table_query);
        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(parser, create_table_query, local_context->getSettingsRef().max_query_size
            , local_context->getSettingsRef().max_parser_depth);

        applyMetadataChangesToCreateQuery(ast, new_metadata, ParserSettings::CLICKHOUSE);
        alter_act.setNewSchema(queryToString(ast));
        txn->appendAction(std::move(action));
    }

    auto & txn_coordinator = local_context->getCnchTransactionCoordinator();
    txn_coordinator.commitV1(txn);

    if (alter_setting)
        this->settings = new_settings;
    setInMemoryMetadata(new_metadata);
}

bool StorageCnchKafka::tableIsActive() const
{
    auto catalog = getGlobalContext()->getCnchCatalog();
    std::optional<CnchBGThreadStatus> thread_status = catalog->getBGJobStatus(getStorageUUID(), CnchBGThreadType::Consumer);
    return (!thread_status) || (*thread_status == CnchBGThreadStatus::Running);
}

/// TODO: merge logic of `checkDependencies` in KafkaConsumeManager
StoragePtr StorageCnchKafka::tryGetTargetTable()
{
    auto catalog = getGlobalContext()->getCnchCatalog();
    auto views = catalog->getAllViewsOn(*getGlobalContext(), shared_from_this(), getContext()->getTimestamp());
    if (views.size() > 1)
        throw Exception("CnchKafka should only support ONE MaterializedView table now, but got "
                        + toString(views.size()), ErrorCodes::LOGICAL_ERROR);

    for (const auto & view : views)
    {
        auto *mv = dynamic_cast<StorageMaterializedView *>(view.get());
        if (!mv)
            throw Exception("Dependence for CnchKafka should be MaterializedView, but got "
                            + view->getName(), ErrorCodes::LOGICAL_ERROR);

        if (mv->async())
           continue;

        /// target_table should be CnchMergeTree now, but it may be some other new types
        auto target_table = mv->getTargetTable();
        if (target_table)
            return target_table;
    }

    return nullptr;
}

static std::vector<Int32> generateRandomSequence(const size_t range, const size_t sample_count)
{
    std::vector<Int32> res(range);
    std::iota(res.begin(), res.end(), 0);

    std::mt19937 g(std::random_device{}());
    std::shuffle(res.begin(), res.end(), g);

    return std::vector<Int32>(res.begin(), res.begin() + std::min(range, sample_count));
}

static void generateSamplePartitionsList(std::set<cppkafka::TopicPartition> & res,
                                        const std::map<String, size_t> & num_partitions_of_topics,
                                        size_t required_sample_count = 0, const Float64 sample_ration = 1.0)
{
    for (const auto & [topic, partition_cnt] : num_partitions_of_topics)
    {
        if (required_sample_count == 0)
            required_sample_count = std::ceil(partition_cnt * sample_ration);

        const auto & sample_list = generateRandomSequence(partition_cnt, required_sample_count);
        for (const auto partition : sample_list)
            res.emplace(cppkafka::TopicPartition{topic, partition});
    }
}

std::set<cppkafka::TopicPartition> StorageCnchKafka::getSampleConsumingPartitionList([[maybe_unused]]const std::map<String, size_t> & num_partitions_of_topics)
{
    if (!sample_consuming_partitions_list.empty() || settings.sample_consuming_params.value.empty())
        return sample_consuming_partitions_list;

    const String & sample_params = settings.sample_consuming_params.value;
    using namespace rapidjson;
    Document document;
    ParseResult ok = document.Parse(sample_params.c_str());
    if (!ok)
        throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                String("JSON parse error ") + GetParseError_En(ok.Code()) + " " + DB::toString(ok.Offset()));

    static std::set<String> sample_params_options{"sample_partitions_list", "sample_partitions_ratio", "sample_partitions_count"};
    const auto & obj = document.GetObject();
    if (obj.MemberCount() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                String("Only one param can be acceptted by sample_consuming_params but got ") + toString(obj.MemberCount()));

    const auto & member = obj.MemberBegin();
    auto && key = member->name.GetString();
    if (!sample_params_options.contains(key))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Unsupported param " + String(key) + " for sample consuming");

    if (String(key) == "sample_partitions_ratio")
    {
        auto && value = member->value.GetFloat();
        if (value <= 0.0 and value > 1.0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid sample partitions ration: " + toString(value));
        LOG_DEBUG(log, "get sample consuming param: sample_partitions_ratio {}", value);

        generateSamplePartitionsList(sample_consuming_partitions_list, num_partitions_of_topics, 0, value);
    }
    else if (String(key) == "sample_partitions_count")
    {
        auto && value = member->value.GetInt();
        LOG_DEBUG(log, "get sample consuming param: sample_partitions_count {}", value);

        generateSamplePartitionsList(sample_consuming_partitions_list, num_partitions_of_topics, value);
    }
    else /// "sample_partitions_list"
    {
        if (member->value.IsArray())
        {
            if (num_partitions_of_topics.size() > 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "The CnchKafka table has more than one topic, you should set sample_partitions_list with topic name");

            const String & topic_name = num_partitions_of_topics.begin()->first;
            for (auto & v : member->value.GetArray())
            {
                LOG_DEBUG(log, "get sample consuming param list with partition #{}", v.GetInt());
                sample_consuming_partitions_list.emplace(cppkafka::TopicPartition{topic_name, v.GetInt()});
            }
        }
        else if (member->value.IsObject())
        {
            for (auto & m : member->value.GetObject())
            {
                auto && k = m.name.GetString();
                auto && v = m.value.GetInt();
                LOG_DEBUG(log, "get sample consuming param list member: {}:{}", k, v);
                sample_consuming_partitions_list.emplace(cppkafka::TopicPartition{k, v});
            }
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "the value of sample_partitions_list should be either Array or Object");
    }

    return sample_consuming_partitions_list;
}

}

#endif
