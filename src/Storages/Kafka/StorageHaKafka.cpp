#include <Storages/Kafka/StorageHaKafka.h>

#include <DataStreams/CountingBlockOutputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Storages/Kafka/HaKafkaBlockInputStream.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageHaMergeTree.h>
#include <Storages/StorageHaUniqueMergeTree.h>
#include <Storages/StorageMaterializedView.h>
#include <Common/Exception.h>
#include <Common/escapeForFileName.h>
#include <Common/Macros.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/File.h>
#include <consul/bridge.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

namespace CurrentMetrics
{
extern const Metric LeaderReplica;
extern const Metric MemoryTrackingForConsuming;
}

namespace DB
{
namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int RDKAFKA_EXCEPTION;
extern const int UNKNOWN_EXCEPTION;
extern const int CANNOT_READ_FROM_ISTREAM;
extern const int INVALID_CONFIG_PARAMETER;
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int TIMEOUT_EXCEEDED;
extern const int NO_REPLICA_NAME_GIVEN;
extern const int REPLICA_IS_ALREADY_EXIST;
extern const int TABLE_WAS_NOT_DROPPED;
extern const int TABLE_IS_READ_ONLY;
extern const int QUERY_WAS_CANCELLED;
}

namespace
{
const auto STREAM_RESCHEDULE_MS = 100;
/// const auto MEMEORY_TABLE_CHECK_RESCHEDULE_MS = 500;
/// const auto CLEANUP_TIMEOUT_MS = 3000;

/// Configuration prefix
const String CONFIG_PREFIX = "kafka";
const String BYTEDANCE_CONFIG_PREFIX = "bytedance_kafka";
static constexpr auto CONSUL_PREFIX = "consul://";


void loadFromConfig(
    cppkafka::Configuration & conf,
    const Poco::Util::AbstractConfiguration & config,
    const String & path,
    bool replace_underline = true)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(path, keys);

    for (const auto & key : keys)
    {
        const String key_path = path + "." + key;
        const String key_name = replace_underline ? boost::replace_all_copy(key, "_", ".") : key;
        conf.set(key_name, config.getString(key_path));
    }
}

/// Make sure DC_SAME < DC_DIFFERENT < DC_UNKNOWN
#if USE_BYTEDANCE_RDKAFKA
const auto DC_SAME = 0;
const auto DC_DIFFERENT = 2;
#endif
const auto DC_UNKNOWN = 9;

const String DISTANCE_MARK = "@DISTANCE@";

String replaceConsumerDistance(String leader_priority, size_t distance)
{
    if (auto pos = leader_priority.find(DISTANCE_MARK); pos != String::npos)
    {
        leader_priority.replace(pos, DISTANCE_MARK.length(), toString(distance));
    }
    return leader_priority;
}

/// Simple parser and evaluator for `a or a + b` expr
Int64 parsePartitionNum(const String & s)
{
    auto pos = s.find('+');
    if (s.npos == pos)
    {
        return parse<Int64>(s);
    }
    else
    {
        String lhs = boost::trim_copy(s.substr(0, pos));
        String rhs = boost::trim_copy(s.substr(pos + 1, s.length()));
        return parse<Int64>(lhs) + parse<Int64>(rhs);
    }
}

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

} // namespace

StorageHaKafka::StorageHaKafka(
    const String & zookeeper_path_,
    const String & replica_name_,
    bool attach_,
    const String & path_,
    const StorageID & table_id_,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const KafkaSettings & settings_)
    : StorageReplicaComponent(zookeeper_path_, replica_name_, attach_, table_id_.getDatabaseName(), table_id_.getTableName(), context_)
    , IStorage(table_id_)
    ///, full_path(getContext()->getPath() + path_ + escapeForFileName(table_id_.getTableName()) + '/')
    , database_name(table_id_.getDatabaseName())
    , table_name(table_id_.getTableName())
    , settings(settings_)
    , settings_adjustments(createSettingsAdjustments())
    , restarting_thread(*this)
    , log(&Poco::Logger::get(database_name + "." + table_name + " (StorageHaKafka)"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);

    checkAndLoadSettings(settings, params);

    /// Create full_path to store STOP_MARK: just use the first disk now
    if (path_.empty())
        throw Exception("Data path is needed for HaKafka table", ErrorCodes::LOGICAL_ERROR);
    /// TODO: if we need add storage policy for kafka table
    auto disks = getContext()->getStoragePolicy("default")->getDisks();
    if (disks.empty())
        throw Exception("No available disk found", ErrorCodes::LOGICAL_ERROR);

    if (!disks[0]->exists(path_))
        disks[0]->createDirectories(path_);
    full_path = disks[0]->getPath() + path_;

    initConsumerContexts();

    if (is_readonly)
        return;

    if (!attach_)
    {
        createTableIfNotExists();
        checkTableStructure(false, false);
        createReplica();
    }
    else
    {
        checkStopMarkFile();

        checkTableStructure(false, true);
        attachReplica();
    }
}

void StorageHaKafka::checkAndLoadSettings(KafkaSettings & cur_settings, KafkaParams & cur_params)
{
    /// auto & global_settings = getContext()->getSettingsRef();

    if (!cur_settings.broker_list.changed && !cur_settings.cluster.changed)
        throw Exception("Required parameter kafka_broker_list or kafka_cluster", ErrorCodes::BAD_ARGUMENTS);
    else if (cur_settings.broker_list.changed && cur_settings.cluster.changed)
        throw Exception("Can't use both kafka_broker_list and kafka_cluster", ErrorCodes::BAD_ARGUMENTS);
    cur_settings.broker_list.value = getContext()->getMacros()->expand(cur_settings.broker_list.value);

    auto & topics = cur_params.topics;
    topics.clear();
    boost::split(topics, cur_settings.topic_list.value, [](char c) { return c == ','; });
    for (String & topic : topics)
        boost::trim(topic);
    std::sort(topics.begin(), topics.end());
    /// rdkafka would coredump while destroying consumer if there are duplicated topics
    if (auto iter = std::unique(topics.begin(), topics.end()); iter != topics.end())
        throw Exception("Forbid duplicated kafka topics: " + *iter, ErrorCodes::BAD_ARGUMENTS);
    topics = getContext()->getMacros()->expand(topics);

    auto shard_count_str = getContext()->getMacros()->expand(cur_settings.shard_count);
    cur_params.shard_count = parse<Int64>(shard_count_str);
    if (cur_params.shard_count < 1)
        throw Exception("shard_count should >= 1", ErrorCodes::BAD_ARGUMENTS);

    auto partition_num_str = getContext()->getMacros()->expand(cur_settings.partition_num);
    cur_params.partition_num = parsePartitionNum(partition_num_str);
    /// RD_KAFKA_PARTITION_UA == -1, cannot MOD.
    if (cur_params.partition_num != RD_KAFKA_PARTITION_UA)
        cur_params.partition_num %= cur_params.shard_count;

    if (cur_params.partition_num == RD_KAFKA_PARTITION_UA && cur_settings.enable_transaction)
        throw Exception("Cannot enable transaction if use rdkafka subscription.", ErrorCodes::BAD_ARGUMENTS);

    cur_params.leader_priority_template = getContext()->getMacros()->expand(cur_settings.leader_priority);
    cur_params.leader_priority = replaceConsumerDistance(cur_params.leader_priority_template, DC_UNKNOWN);

    ///cur_params.memory_table_read_mode = stringToReadMode(cur_settings.memory_table_read_mode);

    cur_params.memory_table_min_thresholds = Thresholds{
        time_t(cur_settings.memory_table_min_time.value), cur_settings.memory_table_min_rows, cur_settings.memory_table_min_bytes};
    cur_params.memory_table_max_thresholds = Thresholds{
        time_t(cur_settings.memory_table_max_time.value), cur_settings.memory_table_max_rows, cur_settings.memory_table_max_bytes};


    if (cur_settings.api_version_request.value != "true" && cur_settings.api_version_request.value != "false")
        throw Exception("api_version_request must be `true` or `false`", ErrorCodes::BAD_ARGUMENTS);

    static std::set<String> auto_offset_reset_options{"smallest", "earliest", "beginning", "largest", "latest", "end"};
    if (!cur_settings.auto_offset_reset.value.empty() && !auto_offset_reset_options.count(cur_settings.auto_offset_reset.value))
        throw Exception("Invalid value for auto_offset_reset", ErrorCodes::BAD_ARGUMENTS);

    if (cur_settings.enable_transaction)
        throw Exception("Transaction is not supported for kafka consumption now", ErrorCodes::LOGICAL_ERROR);

    if (cur_settings.enable_memory_table)
        throw Exception("Memory table is not supported now", ErrorCodes::LOGICAL_ERROR);
}

void StorageHaKafka::initConsumerContexts()
{
    {
        std::lock_guard lock(status_mutex);
        assigned_partitions.resize(settings.num_consumers);
    }

    {
        std::lock_guard lock(context_mutex);

        consumer_contexts.clear();
        consumer_contexts.resize(settings.num_consumers);
        for (size_t i = 0; i < settings.num_consumers; ++i)
        {
            consumer_contexts[i].mutex = std::make_unique<std::timed_mutex>();
            consumer_contexts[i].index = i;

            auto task = getContext()->getMessageBrokerSchedulePool().createTask(
                log->name(), [this, consumer_index = i] { streamThread(consumer_index); });
            task->deactivate();
            consumer_contexts[i].task = std::move(task);
        }
    }
}

void StorageHaKafka::resetConsumerContext(size_t index)
{
    {
        std::lock_guard lock(status_mutex);
        assigned_partitions[index].clear();
    }

    {
        std::lock_guard lock(context_mutex);
        consumer_contexts[index].buffer.reset();
        consumer_contexts[index].error_event = false;
    }
}

void StorageHaKafka::getStatus(Status & res)
{
    StorageReplicaComponent::getStatus(res);

    std::lock_guard lock(status_mutex);

    res.kafka_cluster = settings.cluster.value.empty() ? settings.broker_list : settings.cluster;
    res.topics = params.topics;
    res.group_name = settings.group_name;

    res.consumer_switch = checkStopMarkFile() ? Status::OFF : Status::ON;
    res.dependencies = DatabaseCatalog::instance().getDependencies(getStorageID()).size();
    res.num_consumers = settings.num_consumers;

    std::ostringstream oss;
    for (auto & consumer_partitions : assigned_partitions)
    {
        for (auto & tp : consumer_partitions)
        {
            oss << tp.get_topic() << '#' << tp.get_partition();
            res.assigned_partitions.push_back(oss.str());
            oss.str({});
        }
    }

    res.is_consuming = is_leader && is_consuming;
    res.last_exception = last_exception;
}

Pipe StorageHaKafka::read(
    const Names & column_names,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr query_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    if (!query_context->getSettingsRef().enable_debug_select_from_kafka_table)
        throw Exception("Selecting from Kafka table is disabled", ErrorCodes::BAD_ARGUMENTS);

    const size_t stream_count = std::min(size_t(num_streams), settings.num_consumers.value);

    Pipes pipes;
    pipes.reserve(stream_count);

    // Claim as many consumers as requested, but don't block
    auto local_context = Context::createCopy(query_context);
    local_context->applySettingsChanges(settings_adjustments);

    for (size_t i = 0; i < stream_count; ++i)
    {
        // Use block size of 1, otherwise LIMIT won't work properly as it will buffer excess messages in the last block
        pipes.emplace_back(std::make_shared<SourceFromInputStream>(
            std::make_shared<HaKafkaBlockInputStream>(*this, getInMemoryMetadataPtr(), local_context, column_names, 1, i, /* need_materialized */true)));

    }

    LOG_DEBUG(log, "Starting reading {} streams, {} block size", pipes.size(), max_block_size);
    return Pipe::unitePipes(std::move(pipes));
}


void StorageHaKafka::startup()
{
    LOG_DEBUG(log, "starting up table.");
    Stopwatch stopwatch;

    if (is_readonly)
        return;

    restarting_thread.start();
    startup_event.wait();

    LOG_DEBUG(log, "startup finished; cost {} ms", stopwatch.elapsedMilliseconds());
}

void StorageHaKafka::shutdown(/* bool */)
{
    restarting_thread.shutdown();
    // check_memory_table_task could be null as it is initialized during startup. shutdown
    // could be invoked before startup is scheduled.
    /*if (enableMemoryTable() && check_memory_table_task.hasTask())
    {
        check_memory_table_task->deactivate();
    }*/
}

void StorageHaKafka::updateDependencies()
{
    if (is_leader)
    {
        std::lock_guard lock(context_mutex);
        for (auto & consumer_context : consumer_contexts)
            consumer_context.task->activateAndSchedule();
    }
}

void StorageHaKafka::rename(const String & new_path_to_db, const StorageID & new_table_id)
{
    full_path = new_path_to_db + escapeForFileName(new_table_id.getTableName()) + '/';
    database_name = new_table_id.getDatabaseName();
    table_name = new_table_id.getTableName();

    /// offset_manager->rename(new_path_to_db, new_database_name, new_table_name);
}

void StorageHaKafka::removeZKNodes()
{
    auto zookeeper = tryGetZooKeeper();

    if (is_readonly || !zookeeper)
        throw Exception(
            "Can't drop readonly replicated table (need to drop data in ZooKeeper as well)",
            ErrorCodes::TABLE_IS_READ_ONLY);

    // TODO: shutdown() here is redundant?
    // shutdown();

    if (zookeeper->expired())
        throw Exception(
            "Table was not dropped because ZooKeeper session has expired.", ErrorCodes::TABLE_WAS_NOT_DROPPED);

    LOG_INFO(log, "Removing replica {}", replica_path);
    replica_is_active_node = nullptr;
    zookeeper->tryRemoveRecursive(replica_path);

    /// Check that `zookeeper_path` exists: it could have been deleted by another replica after execution of previous line.
    Strings replicas;
    if (zookeeper->tryGetChildren(zookeeper_path + "/replicas", replicas) == Coordination::Error::ZOK && replicas.empty())
    {
        LOG_INFO(log, "Removing table {} (this might take several minutes)", zookeeper_path);
        zookeeper->tryRemoveRecursive(zookeeper_path);
    }
}

void StorageHaKafka::drop()
{
    removeZKNodes();
}

void StorageHaKafka::alter(
    const DB::AlterCommands & commands,
    const ContextPtr alter_context,
    TableLockHolder & table_lock_holder)
{
    assertNotReadonly();

    bool settings_alter = commands.isSettingsAlter();
    if (settings_alter)
        exitLeaderElection();
    SCOPE_EXIT({
        if (settings_alter)
            enterLeaderElection();
    });

    /// FIXME: lock here?

    auto new_settings = this->settings;
    for (const auto & c : commands)
        if (c.type == AlterCommand::MODIFY_SETTING)
            new_settings.applyKafkaSettingChanges(c.settings_changes);
    auto new_params = this->params;
    checkAndLoadSettings(new_settings, new_params);

    /// Add `kafka_` prefix for settings
    AlterCommands new_commands = commands;
    if (settings_alter)
    {
        for (auto & command : new_commands)
        {
            if (command.type != AlterCommand::MODIFY_SETTING)
                continue;
            for (auto & change : command.settings_changes)
                addKafkaPrefix(change.name);
        }
    }

    /// execute alter commands by calling function of base class `IStorage`
    IStorage::alter(new_commands, alter_context, table_lock_holder);

    if (settings_alter)
    {
        bool num_consumers_changed = new_settings.num_consumers != this->settings.num_consumers;

        this->settings = new_settings;
        this->params = new_params;

        if (num_consumers_changed)
            initConsumerContexts();
    }
}

StorageHaKafka::~StorageHaKafka()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(log, "~StorageHaKafka");
    }
}

static String getBrokerList(const String & psm, Poco::Logger * log)
{
    if (!startsWith(psm, CONSUL_PREFIX))
        return psm;

    String res;
    bool first = true;
    auto services = cpputil::consul::translate_one(psm.substr(strlen(CONSUL_PREFIX)));

    for (auto & service : services)
    {
        res += first ? "": ", ";
        first = false;
        res += service.host + ":" + std::to_string(service.port);
    }

    LOG_DEBUG(log, "Got kafka broker list '{}' from consul.", res);
    return res;
}

cppkafka::Configuration StorageHaKafka::createConsumerConfiguration(size_t consumer_index)
{
    auto & global_settings = getContext()->getSettingsRef();

    cppkafka::Configuration conf;

    // We manually commit offsets after a stream successfully finished
    conf.set("enable.auto.commit", "false");

    // Ignore EOF messages
    conf.set("enable.partition.eof", "false");

    /// 1) set from global configuration

    // Update consumer configuration from the configuration
    const auto & config = getContext()->getConfigRef();
    if (config.has(CONFIG_PREFIX))
        loadFromConfig(conf, config, CONFIG_PREFIX);

    // Update consumer topic-specific configuration
    for (const auto & topic : params.topics)
    {
        const auto topic_config_key = CONFIG_PREFIX + "_" + topic;
        if (config.has(topic_config_key))
            loadFromConfig(conf, config, topic_config_key);
    }

    bool in_bytedance = !settings.cluster.value.empty();

    if (in_bytedance)
    {
        if (config.has(BYTEDANCE_CONFIG_PREFIX))
            loadFromConfig(conf, config, BYTEDANCE_CONFIG_PREFIX, false);

        for (const auto & topic : params.topics)
        {
            const auto bytedance_topic_config_key = BYTEDANCE_CONFIG_PREFIX + "_" + topic;
            if (config.has(bytedance_topic_config_key))
                loadFromConfig(conf, config, bytedance_topic_config_key, false);
        }

        if (!settings.bytedance_owner.value.empty())
            conf.set("owner", settings.bytedance_owner);
    }

    /// 2) load from extra_librdkafka_config in JSON format
    if (auto & extra_config = settings.extra_librdkafka_config.value; !extra_config.empty())
    {
        using namespace rapidjson;
        Document document;
        ParseResult ok = document.Parse(extra_config.c_str());
        if (!ok)
            throw Exception(
                String("JSON parse error ") + GetParseError_En(ok.Code()) + " " + toString(ok.Offset()), ErrorCodes::BAD_ARGUMENTS);

        for (auto & member : document.GetObject())
        {
            if (!member.value.IsString())
                continue;
            auto && key = member.name.GetString();
            auto && value = member.value.GetString();
            LOG_TRACE(log, "[extra_config] {} : {}", key, value);
            conf.set(key, value);
        }
    }

    // 3) apply table-level settings
    if (!in_bytedance)
    {
        LOG_TRACE(log, "Setting brokers: {}", settings.broker_list.value);
        conf.set("metadata.broker.list", getBrokerList(settings.broker_list.value, log));
    }
    else
    {
        /// BYEDANCE KAFKA
        LOG_TRACE(log, "Setting cluster: {}", settings.cluster.value);
        conf.set("cluster", settings.cluster);

        std::ostringstream oss;
        std::copy(params.topics.begin(), params.topics.end(), std::ostream_iterator<std::string>(oss, ","));
        auto topics_text = oss.str();
        LOG_TRACE(log, "Setting topics: {}", topics_text);
        conf.set("topics", topics_text);
    }

    auto client_id = database_name + '.' + table_name + '#' + toString(consumer_index);
    LOG_TRACE(log, "Setting Group ID: {}, Client ID: {}", settings.group_name.value, client_id);

    conf.set("group.id", settings.group_name);
    conf.set("client.id", client_id);
    conf.set("max.partition.fetch.bytes", std::to_string(settings.max_partition_fetch_bytes));

    conf.set("api.version.request", settings.api_version_request);
    if (!settings.broker_version_fallback.value.empty())
        conf.set("broker.version.fallback", settings.broker_version_fallback);

    if (!settings.auto_offset_reset.value.empty())
        conf.set("auto.offset.reset", settings.auto_offset_reset);

    /// TODO P1:
    /// Possible solution: 1) inc session timeout 2) desc batch size.
    /// Or save the offsets_position(), re-assign with the saved offsets.
    if (params.partition_num != RD_KAFKA_PARTITION_UA)
    {
        conf.set("session.timeout.ms", toString(global_settings.kafka_session_timeout_ms.totalMilliseconds()));
    }

    if (settings.librdkafka_enable_debug_log)
        conf.set("log_level", "7");

    /// see `man 3 syslog`
    int max_level = bool(settings.librdkafka_enable_debug_log) ? 7 : 6;
    conf.set_log_callback(
        [this, max_level](cppkafka::KafkaHandleBase & handle, int level, const std::string & facility, const std::string & message) {
    /// Follow rd_kafka_log_print()
#define M "RDKAFKA|{}|{}|{}|{}", level, facility, (handle.get_handle() ? handle.get_name() : ""), message
            if (level <= 3) // <= LOG_ERR
                LOG_ERROR(log, M);
            else if (level == 4) // LOG_WARNING
                LOG_WARNING(log, M);
            else if (level == 5) // LOG_NOTICE
                LOG_DEBUG(log, M);
            else if (level <= max_level) // LOG_INFO(6) or LOG_DEBUG(7),
                LOG_TRACE(log, M);
            else
                LOG_TRACE(log, M);
#undef M
        });

#if USE_BYTEDANCE_RDKAFKA
    conf.set_error_callback([this, consumer_index](cppkafka::KafkaHandleBase &, int error, const std::string & reason) {
        if (error == RD_KAFKA_RESP_ERR__CONSUMER_DC_CHANGE)
        {
            String msg = "[CONSUMER_DC_CHANGE] DC of Kafka cluster is changed which consumers should be destroyed and "
                         "recreated: "
                + reason;

            /// 1. to local log files
            LOG_ERROR(log, msg);

            /// 2. to system.kafka_log
            if (auto kafka_log = getContext()->getKafkaLog())
            {
                try
                {
                    auto kafka_error_log = createKafkaLog(KafkaLogElement::EXCEPTION, consumer_index);
                    kafka_error_log.has_error = true;
                    kafka_error_log.last_exception = msg;
                    kafka_log->add(kafka_error_log);

                    std::lock_guard lock(status_mutex);
                    last_exception = kafka_error_log.last_exception;
                }
                catch (...)
                {
                    tryLogCurrentException(log);
                }
            }

            /// 3. set `error_event` to trigger re-create consumer buffer
            {
                std::lock_guard lock(context_mutex);
                consumer_contexts[consumer_index].error_event = true;
            }
            /**
             * It seems that rdkafka consumer would be destroyed twice while another got a DC_CHANGE event,
             * which would cause core dump in librdkafka. This is a serious bug dut to concurrency which
             * must be fixed in librdkafka.
             *
             * Now, we use a more mild way to close consumer, which, but react not so quickly.
            std::thread([ptr=shared_from_this()]
            {
                auto & s = dynamic_cast<StorageHaKafka&>(*ptr);
                try
                {
                    s.restartConsume();
                }
                catch (...)
                {
                    tryLogCurrentException(s.log, "Failed to restart consume");
                }
            }).detach();
            */
        }
    });
#endif

    return conf;
}

BufferPtr StorageHaKafka::createBuffer(size_t consumer_index)
{
    // Create a consumer and subscribe to topics
    auto consumer = std::make_shared<KafkaConsumer>(createConsumerConfiguration(consumer_index));

    using namespace std::chrono_literals;
    consumer->set_timeout(5s);

    std::ostringstream logger_name;
    logger_name << database_name << '.' << table_name + " (Consumer #" << consumer_index << ')';

    const Settings & global_settings = getContext()->getSettingsRef();
    size_t batch_size = settings.max_block_size;
    size_t poll_timeout = global_settings.stream_poll_timeout_ms.totalMilliseconds();
    size_t expire_timeout = global_settings.stream_flush_interval_ms.totalMilliseconds();

    auto buffer = std::make_shared<DelimitedReadBuffer>(
        std::make_unique<HaReadBufferFromKafkaConsumer>(
            consumer, logger_name.str(), batch_size, poll_timeout, expire_timeout, &is_leader),
        settings.row_delimiter);
    return buffer;
}

BufferPtr StorageHaKafka::tryClaimBuffer(size_t consumer_index, long wait_ms)
{
    auto & consumer_context = consumer_contexts[consumer_index];

    auto lock_result = consumer_context.mutex->try_lock_for(std::chrono::milliseconds(wait_ms));
    if (!lock_result)
        throw Exception("Failed to claim consumer buffer #" + toString(consumer_index), ErrorCodes::LOGICAL_ERROR);

    /// Handle of consumer would be destroyed if Kafka DC changed
    if (consumer_context.error_event
        || (consumer_context.buffer
        && consumer_context.buffer->subBufferAs<HaReadBufferFromKafkaConsumer>()->getConsumer()->check_destroyed()))
    {
        LOG_INFO(log, "Handle of Consumer #{} has been destroyed, reset it.", consumer_index);
        resetConsumerContext(consumer_index);
    }
    else if (
        startsWith(settings.broker_list.value, CONSUL_PREFIX)
        && time(nullptr) > Int64(last_lookup_time + getContext()->getSettingsRef().kafka_refresh_consul_time))
    {
        last_lookup_time = time(nullptr);
        LOG_DEBUG(log, "Refresh {}, will re-create Consumer #{}", settings.broker_list.value, consumer_index);
        resetConsumerContext(consumer_index);
    }

    /// Create consumer buffer as need
    if (!consumer_context.buffer)
    {
        try
        {
            consumer_context.buffer = createBuffer(consumer_index);
        }
        catch (...)
        {
            consumer_context.mutex->unlock();
            throw;
        }
    }

    return consumer_context.buffer;
}

void StorageHaKafka::createBuffers()
{
    std::lock_guard lock(context_mutex);
    for (size_t i = 0; i < settings.num_consumers; ++i)
    {
        consumer_contexts[i].buffer = createBuffer(i);
    }
}

void StorageHaKafka::pushBuffer(size_t consumer_index)
{
    auto & consumer_context = consumer_contexts[consumer_index];
    consumer_context.mutex->unlock();
}

[[maybe_unused]]static bool
lessEqualTopicPartitionList(const cppkafka::TopicPartitionList & lhs, const cppkafka::TopicPartitionList & rhs)
{
    if (lhs.size() != rhs.size())
        return false;
    for (size_t i = 0; i < lhs.size(); ++i)
    {
        if (!(lhs[i] < rhs[i] || lhs[i] == rhs[i]))
            return false;
    }
    return true;
}


void StorageHaKafka::subscribeBuffer(BufferPtr & buffer, size_t consumer_index)
{
    auto consumer_buf = buffer->subBufferAs<HaReadBufferFromKafkaConsumer>();
    auto & consumer = consumer_buf->getConsumer();

    if (params.partition_num == RD_KAFKA_PARTITION_UA)
    {
        consumer_buf->subscribe(params.topics);

        std::lock_guard lock(status_mutex);
        assigned_partitions[consumer_index] = consumer->get_assignment();
    }
    else
    {
        /// It's easy to get exception while requiring topic metadata.
        /// Get them only if necessary
        auto assigned = consumer->get_assignment();
        if (assigned.empty())
        {
            /// No partitions assigned; Let's find some.
            cppkafka::TopicPartitionList tpl;
            try
            {
                for (const auto & topic_name : params.topics)
                {
                    auto topic = consumer->get_topic(topic_name);
                    auto topic_metadata = consumer->get_metadata(topic);
                    auto partition_cnt = topic_metadata.get_partitions().size();

                    for (auto p = params.partition_num + params.shard_count * consumer_index; p < partition_cnt;
                         p += params.shard_count * settings.num_consumers)
                        tpl.emplace_back(topic_name, p);
                }
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

            tpl = consumer->get_offsets_committed(tpl);
            consumer_buf->assign(tpl);

            std::lock_guard lock(status_mutex);
            assigned_partitions[consumer_index] = tpl;
        }
        else
        {
            consumer_buf->assign(assigned);

            std::lock_guard lock(status_mutex);
            assigned_partitions[consumer_index] = assigned;
        }
    }

    consumer_buf->reset();
}

void StorageHaKafka::unsubscribeBuffer(BufferPtr & buffer)
{
    if (params.partition_num == RD_KAFKA_PARTITION_UA)
        buffer->subBufferAs<HaReadBufferFromKafkaConsumer>()->unsubscribe();
    else
        buffer->subBufferAs<HaReadBufferFromKafkaConsumer>()->unassign();
}

bool StorageHaKafka::checkDependencies(const StorageID & table_id)
{
    // Check if all dependencies are attached
    auto dependencies = DatabaseCatalog::instance().getDependencies(table_id);
    LOG_TRACE(log, "Get dependencies of {}.{} size {} ", table_id.getDatabaseName(), table_id.getTableName(), dependencies.size());
    if (dependencies.empty())
        return true;

    // Check the dependencies are ready?
    for (const auto & db_tab : dependencies)
    {
        auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
        LOG_TRACE(log, "Check dependent table {}.{}", db_tab.getDatabaseName(), db_tab.getTableName());
        if (!table)
            return false;

        // If it materialized view, check it's target table
        auto * materialized_view = dynamic_cast<StorageMaterializedView *>(table.get());
        if (materialized_view)
        {
            auto target_table = materialized_view->tryGetTargetTable();
            if (!target_table)
                return false;
        }

        // Check all its dependencies
        if (!checkDependencies(db_tab))
            return false;
    }

    return true;
}

bool StorageHaKafka::hasUniqueTableDependencies()
{
    auto table_id = getStorageID();

    auto dependencies = DatabaseCatalog::instance().getDependencies(table_id);
    for (const auto & db_tab : dependencies)
    {
        auto table = DatabaseCatalog::instance().tryGetTable(db_tab, getContext());
        if (!table)
            continue;

        auto mv = dynamic_cast<StorageMaterializedView *>(table.get());
        if (!mv)
            continue;

        auto target_table = mv->tryGetTargetTable();
        if (!target_table)
            continue;

        /// FIXME: support unique merge tree
        ///if (auto unique = dynamic_cast<StorageHaUniqueMergeTree *>(target_table.get()); unique != nullptr)
        if (auto merge_tree = dynamic_cast<StorageMergeTree *>(target_table.get()); merge_tree == nullptr)
            return true;
    }
    return false;
}

Names StorageHaKafka::collectRequiredColumnsFromDependencies(const Dependencies & dependencies)
{
    NameSet name_set;
    for (const auto & dependency : dependencies)
    {
        auto names = collectRequiredColumnsFromTable(dependency);
        name_set.insert(std::move_iterator(names.begin()), std::move_iterator(names.end()));
    }
    Names result;
    const auto & virtuals = getVirtuals();
    for (auto && name : name_set)
    {
        if (virtuals.contains(name))
            result.push_back(std::move(name));
        else if (getInMemoryMetadataPtr()->getColumns().get(name).default_desc.kind == ColumnDefaultKind::Default)
            result.push_back(std::move(name));
    }
    return result;
}

Names StorageHaKafka::collectRequiredColumnsFromTable(const StorageID & table_id)
{
    auto storage = DatabaseCatalog::instance().getTable(table_id, getContext());
    auto materialized_view = dynamic_cast<const StorageMaterializedView *>(storage.get());
    if (!materialized_view)
        return {};

    auto meta_data = materialized_view->getInMemoryMetadataPtr();
    if (!meta_data->hasSelectQuery())
        throw Exception("Cannot get INNER QUERY for Materialized View table: " + table_id.getFullTableName(), ErrorCodes::LOGICAL_ERROR);
    auto query = meta_data->getSelectQuery().inner_query;

    auto syntax_analyzer_result = TreeRewriter(getContext()).analyzeSelect(
        query, TreeRewriterResult({}, shared_from_this(), getInMemoryMetadataPtr()));
    return syntax_analyzer_result->requiredSourceColumns();
}

Names StorageHaKafka::filterVirtualNames(const Names & names) const
{
    Names virtual_names{};
    for (auto & name : names)
    {
        if (name == "_topic" || name == "_key" || name == "_offset" || name == "_partition" || name == "_content"
            || name == "_info" || name == "_timestamp")
        {
            virtual_names.push_back(name);
        }
    }

    std::sort(virtual_names.begin(), virtual_names.end());
    return virtual_names;
}

/// Only leader can consume
void StorageHaKafka::streamThread(size_t consumer_index)
{
    // Do nothing if not start up, to avoid instance slow startup
/*    if (!getContext()->isListenPortsReady())
    {
        consumer_contexts[consumer_index].task->scheduleAfter(STREAM_RESCHEDULE_MS);
        return;
    }*/

    if (!is_leader)
        return;

    auto table_id = getStorageID();
    try
    {
        auto dependencies = DatabaseCatalog::instance().getDependencies(table_id);
        if (dependencies.size() == 0)
        {
            LOG_WARNING(log, "Get empty dependencies");
            is_consuming = false;
        }
        else if (!checkDependencies(table_id))
        {
            LOG_WARNING(log, "Failed to check dependencies");
            is_consuming = false;
        }
        else
        {
            LOG_DEBUG(log, "Consumer #{} started streaming to {} attached views", consumer_index, dependencies.size());

            is_consuming = true;
            Names required_column_names = collectRequiredColumnsFromDependencies(dependencies);
            streamToViews(required_column_names, consumer_index);
        }

        failed_time = 0;
    }
    catch (...)
    {
        LOG_ERROR(log, "Consumer #{} failed: {}", consumer_index, getCurrentExceptionMessage(true));

        if (auto kafka_log = getContext()->getKafkaLog())
        {
            auto kafka_error_log = createKafkaLog(KafkaLogElement::EXCEPTION, consumer_index);
            kafka_error_log.has_error = true;
            kafka_error_log.last_exception = getCurrentExceptionMessage(false);
            kafka_log->add(kafka_error_log);

            std::lock_guard lock(status_mutex);
            last_exception = kafka_error_log.last_exception;
        }

        failed_time = std::min(10ul, failed_time + 1);
    }

    consumer_contexts[consumer_index].task->scheduleAfter(STREAM_RESCHEDULE_MS * (1 << failed_time));
}

[[maybe_unused]]ASTPtr StorageHaKafka::constructInsertQuery(const Names & required_column_names)
{
    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = getStorageID();

    insert->columns = std::make_shared<ASTExpressionList>();
    for (auto & name : required_column_names)
    {
        insert->columns->children.emplace_back(std::make_shared<ASTIdentifier>(name));
    }

    return insert;
}

bool StorageHaKafka::streamToViews(const Names & /*required_column_names*/, size_t consumer_index)
{
    ///   Currently, we can not track memory usage when memory table enabled.
    /// Because memory would be allocated in this thread (for consuming) but
    /// freed in another thread (for OutputStream).
    ///   XXX: This is a temporary solution and expects a better tracker based
    /// on roles instead of threads for consuming and writing in the future.
    std::optional<MemoryTracker> consume_tracker;

    MemoryTracker * thread_prev_parent = nullptr;

    if (auto thread_tracker = CurrentThread::getMemoryTracker();
        likely(thread_tracker) && settings.enable_memory_tracker && !enableMemoryTable())
    {
        consume_tracker.emplace(VariableContext::Process);
        consume_tracker->setMetric(CurrentMetrics::MemoryTrackingForConsuming);

        thread_prev_parent = thread_tracker->getParent();
        thread_tracker->setParent(&*consume_tracker);
    }

    SCOPE_EXIT({
        if (consume_tracker)
        {
            consume_tracker->logPeakMemoryUsage();
            if (auto thread_tracker = CurrentThread::getMemoryTracker())
                thread_tracker->setParent(thread_prev_parent);
        }
    });

    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
    if (!table)
        throw Exception(
            "Engine table " + database_name + "." + table_name + " doesn't exist.", ErrorCodes::LOGICAL_ERROR);

    /// auto insert = constructInsertQuery(required_column_names);
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    size_t block_size = settings.max_block_size;

    auto consume_context = Context::createCopy(getContext());
    consume_context->makeQueryContext();
    consume_context->setSetting("min_insert_block_size_rows", UInt64(1));
    consume_context->applySettingsChanges(settings_adjustments);

    try
    {
        consume_context->setSessionContext(consume_context);

        // Execute the query
        InterpreterInsertQuery interpreter(insert, consume_context, false, true, true);
        auto block_io = interpreter.execute();

        BlockInputStreamPtr in = std::make_shared<HaKafkaBlockInputStream>(
            *this, getInMemoryMetadataPtr(), consume_context, block_io.out->getHeader().getNames(), block_size, consumer_index);

        /* TODO:
         * 1. Must discard incorrect rows in InputStream to avoid infinite loop.
         *
         * 2. DO NOT catch exceptions here (e.g. ZK session expired), some parts may be
         * committed but other not so. Then it will consume and write same messages
         * after rejoining the consumer.
         *
         * 3. Because of SquashingBlockOutputStream, the messages would not be committed
         * during the copyData loop if the squashed block size was not large enough.
         * So that the messages committing happened before the block writting.
         * Set the min_insert_block_size_rows to 1 and force commit after each reading
         * to make sure the topic offset will be committed after a block written to disk.
         *
         * 3. Although we can recover from exceptions, some messages may be still
         * duplicated.  (if a insertion block across multiple partitions).
         *
         * 4. There may be still some unknown exceptions, let'us find & fix them in
         * the future.
         */
        streamCopyData(*in, *block_io.out, consumer_index);

        /// Add filter log if there are rows actually filterred
        if (getContext()->getSettingsRef().constraint_skip_violate)
        {
            if (auto counting_stream = dynamic_cast<const CountingBlockOutputStream *>(block_io.out.get()))
            {
                size_t origin_rows = in->getProfileInfo().rows;
                size_t written_rows = counting_stream->getProgress().written_rows;

                size_t origin_bytes = in->getProfileInfo().bytes;
                size_t written_bytes = counting_stream->getProgress().written_bytes;
                if (origin_rows != written_rows || origin_bytes != written_bytes)
                {
                    KafkaLogElement kafka_filter_log = createKafkaLog(KafkaLogElement::FILTER, consumer_index);
                    kafka_filter_log.metric = origin_rows - written_rows;
                    kafka_filter_log.bytes = origin_bytes - written_bytes;
                    if (auto kafka_log = getContext()->getKafkaLog())
                        kafka_log->add(kafka_filter_log);
                }
            }
        }
    }
    catch (...)
    {
        throw;
    }
    return false;
}

void StorageHaKafka::streamCopyData(IBlockInputStream & from, IBlockOutputStream & to, size_t consumer_index)
{
    bool has_unique_table = hasUniqueTableDependencies();
    /// for unique table, the underlying BOS may be RemoteBOS which doesn't guarantee that
    /// the block is written successfully after write(block) returns. Therefore we should disable
    /// commit-per-block in that case in order to achieve at-least-once semantic.
    bool commitPerBlock = !has_unique_table;

    from.readPrefix();
    to.writePrefix();

    while (Block block = from.read())
    {
        /// Being not leader means cancellation
        if (!is_leader)
        {
            if (settings.enable_transaction)
            {
                /// Trigger rollback
                throw Exception("Cancelled stream #" + toString(consumer_index), ErrorCodes::QUERY_WAS_CANCELLED);
            }
            else
            {
                /// Early cancellation && Use return instead of break:
                /// Can not reach HaKafkaBlockInputStream::readSuffix() which would commit the
                /// messages not written. And ~HaKafkaBlockInputStream() will unsubscribe/unassign
                /// to ensure the offsets are reset.
                LOG_DEBUG(log, "Cancelled stream #{}", consumer_index);
                return;
            }
        }

        KafkaLogElement kafka_write_log = createKafkaLog(KafkaLogElement::WRITE, consumer_index);
        kafka_write_log.metric = block.rows();
        kafka_write_log.bytes = block.bytes();
        Stopwatch watch;

        to.write(block);

        if (commitPerBlock)
        {
            if (auto kafka_stream = dynamic_cast<HaKafkaBlockInputStream *>(&from))
                kafka_stream->forceCommit();
        }

        kafka_write_log.duration_ms = watch.elapsedMilliseconds();
        if (auto kafka_log = getContext()->getKafkaLog())
            kafka_log->add(kafka_write_log);
    }

    /// The SquashingBlockOutputStream would not work in to.writeSuffix() because of
    /// min_insert_block_size_rows = 1, which writeSuffix() would finish quickly
    /// and do not need cancel stream here.

    /// For outputting additional information in some formats.
    if (from.getProfileInfo().hasAppliedLimit())
        to.setRowsBeforeLimit(from.getProfileInfo().getRowsBeforeLimit());

    to.setTotals(from.getTotals());
    to.setExtremes(from.getExtremes());

    /// The order of writeSuffix and readSuffix matters here because
    /// 1. Data may not be successfully written if writeSuffix throws exception
    /// 2. Consumer's read offset won't reset if readSuffix is called (see HaKafkaBIS's dtor)
    /// Therefore we call writeSuffix before readSuffix here to let the consumer retries
    /// from the last committed position next time if writeSuffix throws exception,
    /// otherwise we may lose some data.
    to.writeSuffix();
    from.readSuffix();

    /// for unique table, commit offsets only after writeSuffix() returns without exception
    if (!settings.enable_transaction && has_unique_table)
    {
        if (auto kafka_stream = dynamic_cast<HaKafkaBlockInputStream *>(&from))
            kafka_stream->forceCommit();
    }
}

void StorageHaKafka::createStopMarkFile()
{
    Poco::File(full_path).createDirectory();

    Poco::File stop_mark(full_path + "stop_mark");
    if (!stop_mark.exists())
        stop_mark.createFile();
    stop_consume = true;
}

void StorageHaKafka::removeStopMarkFile()
{
    Poco::File stop_mark(full_path + "stop_mark");
    if (stop_mark.exists())
        stop_mark.remove();
    stop_consume = false;
}

bool StorageHaKafka::checkStopMarkFile()
{
    return stop_consume = Poco::File(full_path + "stop_mark").exists();
}

void StorageHaKafka::createTableIfNotExists()
{
    auto zookeeper = getZooKeeper();

    if (zookeeper->exists(zookeeper_path))
        return;

    LOG_DEBUG(log, "Creating table {}", zookeeper_path);

    zookeeper->createAncestors(zookeeper_path);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path, "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(
        zookeeper_path + "/columns", getInMemoryMetadataPtr()->getColumns().toString(), zkutil::CreateMode::Persistent));
    ops.emplace_back(
        zkutil::makeCreateRequest(zookeeper_path + "/leader_election", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path + "/replicas", "", zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    auto code = zookeeper->tryMulti(ops, responses);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
        throw Coordination::Exception(code);
}

void StorageHaKafka::createReplica()
{
    auto zookeeper = getZooKeeper();

    LOG_DEBUG(log, "Creating replica {}", replica_path);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(replica_path, "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/host", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(
        zkutil::makeCreateRequest(replica_path + "/columns", getInMemoryMetadataPtr()->getColumns().toString(), zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(
        replica_path + "/leader_priority", params.leader_priority, zkutil::CreateMode::Persistent));

    Coordination::Responses resps;
    auto code = zookeeper->tryMulti(ops, resps);
    if (code == Coordination::Error::ZNODEEXISTS)
        throw Exception("Replica " + replica_path + " already exists.", ErrorCodes::REPLICA_IS_ALREADY_EXIST);
    zkutil::KeeperMultiException::check(code, ops, resps);
}

void StorageHaKafka::attachReplica()
{
    auto zookeeper = getZooKeeper();

    Coordination::Requests ops;
    if (!zookeeper->exists(replica_path + "/host"))
    {
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/host", "", zkutil::CreateMode::Persistent));
    }
    zookeeper->multi(ops);
}

void StorageHaKafka::checkTableStructure(bool, bool allow_alter)
{
    /// Note: must we check table columns?

    if (allow_alter)
    {
        /// XXX: REMOVE IT
        auto zookeeper = getZooKeeper();
        auto leader_priority_from_zk = zookeeper->get(replica_path + "/leader_priority");
        if (leader_priority_from_zk != params.leader_priority)
        {
            zookeeper->set(replica_path + "/leader_priority", params.leader_priority);
            LOG_INFO(
                log,
                "Leader priority has changed, update in ZooKeeper: {} -> {}", leader_priority_from_zk, params.leader_priority);
        }
    }
}

void StorageHaKafka::assertNotReadonly() const
{
    if (is_readonly)
        throw Exception("Table is in readonly mode", ErrorCodes::TABLE_IS_READ_ONLY);
}

void StorageHaKafka::startStreamThreads()
{
    /// Need not lock here, enterLeaderElection() ensure thread safety
    LOG_TRACE(log, "Creating buffers and activating stream threads");

    for (size_t i = 0; i < settings.num_consumers; ++i)
    {
        consumer_contexts[i].task->activateAndSchedule();
    }
}

void StorageHaKafka::stopStreamThreads()
{
    /// Need not lock here, exitLeaderElection() ensure thread safety
    LOG_TRACE(log, "Deactivating stream threads and destroying buffers");

    auto stop_one = [](ConsumerContext & c) {
        c.task->deactivate();
        c.error_event = false;
    };

    if (settings.num_consumers > 1)
    {
        /// It may be costly to destroy consumers one by one. Parallel it.
        ThreadPool pool(std::min(settings.num_consumers.value, getContext()->getSettingsRef().max_threads.value));
        for (size_t i = 0; i < settings.num_consumers; ++i)
            pool.scheduleOrThrowOnError([&stop_one, &c = consumer_contexts[i]] { stop_one(c); });
        pool.wait();
    }
    else
    {
        stop_one(consumer_contexts[0]);
    }

    for (size_t i = 0; i < settings.num_consumers; ++i)
    {
        resetConsumerContext(i);
    }
}

void StorageHaKafka::enterLeaderElection()
{
    std::lock_guard election_lock(election_mutex);
    if (leader_election)
        return;

    try
    {
        createBuffers();
    }
    catch (...)
    {
        if (auto kafka_log = getContext()->getKafkaLog())
        {
            auto kafka_error_log = createKafkaLog(KafkaLogElement::EXCEPTION, 0);
            kafka_error_log.has_error = true;
            kafka_error_log.last_exception = getCurrentExceptionMessage(false);
            kafka_log->add(kafka_error_log);

            std::lock_guard lock(status_mutex);
            last_exception = kafka_error_log.last_exception;
        }
        throw;
    }

    auto callback = [this]() {
        CurrentMetrics::add(CurrentMetrics::LeaderReplica);
        LOG_INFO(log, "Became leader");

        /// Make sure is_leader is on before starting stream threads.
        is_leader = true;
        startStreamThreads();
    };

    try
    {
        leader_election = std::make_shared<zkutil::LeaderElection>(
            getContext()->getMessageBrokerSchedulePool(),
            zookeeper_path + "/leader_election",
            *current_zookeeper, /// current_zookeeper lives for the lifetime of leader_election,
            ///  since before changing `current_zookeeper`, `leader_election` object is destroyed in `partia
            callback,
            replica_name,
            false);
    }
    catch (...)
    {
        leader_election = nullptr;
        throw;
    }
}

void StorageHaKafka::exitLeaderElection()
{
    std::lock_guard election_lock(election_mutex);
    if (!leader_election)
        return;

    /// Shut down the leader election thread to avoid suddenly becoming the leader again after
    /// we have stopped the merge_selecting_thread, but before we have deleted the leader_election object.
    leader_election->shutdown();

    if (is_leader)
    {
        CurrentMetrics::sub(CurrentMetrics::LeaderReplica);
        LOG_INFO(log, "Stopped being leader");

        /// It's faster to stop stream threads if turn off is_leader first.
        is_leader = false;
        stopStreamThreads();
    }

    /// Delete the node in ZK only after we have stopped the merge_selecting_thread - so that only one
    /// replica assigns merges at any given time.
    leader_election = nullptr;
}

size_t StorageHaKafka::calcConsumerDistance([[maybe_unused]]KafkaConsumer & consumer)
{
    size_t distance = DC_UNKNOWN;

#if USE_BYTEDANCE_RDKAFKA
    if (auto rk = consumer.get_handle())
    {
        constexpr size_t max_buf_size = 16;
        String local_dc_buf(max_buf_size, '\0');
        String kafka_dc_buf(max_buf_size, '\0');
        rd_kafka_get_dc(rk, local_dc_buf.data(), local_dc_buf.size() - 1, kafka_dc_buf.data(), kafka_dc_buf.size() - 1);

        String local_dc(local_dc_buf.data());
        String kafka_dc(kafka_dc_buf.data());
        if (local_dc.length() == 0 || kafka_dc.length() == 0)
            distance = DC_UNKNOWN;
        else if (local_dc == kafka_dc)
            distance = DC_SAME;
        else
            distance = DC_DIFFERENT;

        LOG_DEBUG(log, "local_dc: {}, kafka_dc: {}; distance: {}", local_dc, kafka_dc, distance);
    }
#endif

    return distance;
}

void StorageHaKafka::updateLeaderPriority()
{
    std::lock_guard lock(context_mutex);

    /// DO NOT NEED update constant leader_priority
    if (String::npos == params.leader_priority_template.find(DISTANCE_MARK))
        return;

    size_t distance = DC_UNKNOWN;
    for (auto & consumer_context : consumer_contexts)
    {
        if (consumer_context.buffer && !consumer_context.error_event)
            distance = std::min(
                distance,
                calcConsumerDistance(
                    *consumer_context.buffer->subBufferAs<HaReadBufferFromKafkaConsumer>()->getConsumer()));
    }

    auto new_leader_priority = replaceConsumerDistance(params.leader_priority_template, distance);
    if (new_leader_priority != params.leader_priority)
    {
        auto zookeeper = getZooKeeper();
        zookeeper->set(replica_path + "/leader_priority", new_leader_priority);
        LOG_DEBUG(
            log,
            "Leader priority has changed, update in ZooKeeper: {} -> {}", params.leader_priority, new_leader_priority);
        params.leader_priority = new_leader_priority;
    }
}

void StorageHaKafka::updateAbsoluteDelayOfDependencies()
{
    std::function<time_t(const StorageID & table_id)> calc_max_delay_of_dependencies
        = [&](const StorageID & table_id) -> time_t {
        time_t max_absolute_delay = 0;
        for (auto db_table : DatabaseCatalog::instance().getDependencies(table_id))
        {
            auto storage = DatabaseCatalog::instance().tryGetTable(db_table, getContext());
            if (!storage)
                continue;
            do
            {
                auto mv = dynamic_cast<StorageMaterializedView *>(storage.get());
                if (!mv)
                    break;

                auto target_table = mv->tryGetTargetTable();
                if (!target_table)
                    break;

                if (auto ha = dynamic_cast<StorageHaMergeTree *>(target_table.get()); ha)
                {
                    max_absolute_delay = std::max(max_absolute_delay, ha->getAbsoluteDelay());
                }
                else if (auto unique = dynamic_cast<StorageHaUniqueMergeTree *>(target_table.get()); unique)
                {
                    max_absolute_delay = std::max(max_absolute_delay, unique->getAbsoluteDelay());
                }
                else
                {
                    break;
                }
            } while (false);

            /// Recursive call
            max_absolute_delay
                = std::max(max_absolute_delay, calc_max_delay_of_dependencies(db_table));
        }

        return max_absolute_delay;
    };

    auto max_absolute_delay = calc_max_delay_of_dependencies(getStorageID());

    if (max_absolute_delay > settings.max_delay_to_yield_leadership)
    {
        try
        {
            auto & zookeeper = current_zookeeper;

            if (!absolute_delay_node)
            {
                LOG_TRACE(
                    log,
                    "Creating absolute_delay_node: {}/absolute_delay {}", replica_path, max_absolute_delay);
                absolute_delay_node = zkutil::EphemeralNodeHolder::create(
                    replica_path + "/absolute_delay", *zookeeper, toString(max_absolute_delay));
            }
            else
            {
                zookeeper->set(replica_path + "/absolute_delay", toString(max_absolute_delay));
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
    else if (absolute_delay_node)
    {
        LOG_TRACE(log, "Removing absolute_delay_node: {}/absolute_delay", replica_path);
        absolute_delay_node = nullptr;
    }

    absolute_delay_of_dependencies = max_absolute_delay;
}

bool StorageHaKafka::checkYieldLeaderShip()
{
    const auto absolute_delay = absolute_delay_of_dependencies;

    auto zookeeper = getZooKeeper();
    auto live_replicas = zookeeper->getChildren(zookeeper_path + "/leader_election");

    for (auto & replica_election : live_replicas)
    {
        auto replica = zookeeper->get(zookeeper_path + "/leader_election/" + replica_election);
        if (replica == replica_name)
            continue;

        /// If there is no absolute_delay znode in peer replica, then the relative delay is zero.
        time_t replica_absolute_delay = 0;

        String replica_absolute_delay_str;
        if (zookeeper->tryGet(zookeeper_path + "/replicas/" + replica + "/absolute_delay", replica_absolute_delay_str))
            replica_absolute_delay = parse<Int64>(replica_absolute_delay_str);

        Int64 delta = Int64(absolute_delay) - Int64(replica_absolute_delay);
        if (delta < 0)
        {
            /// Skip this peer replica if it is stale
            continue;
        }
        else if (0 <= delta && delta <= settings.max_delay_to_yield_leadership)
        {
            /// Peer replica is similar to me, check leader_priority
            auto replica_leader_priority = zookeeper->get(zookeeper_path + "/replicas/" + replica + "/leader_priority");
            if (replica_leader_priority < params.leader_priority)
            {
                LOG_INFO(
                    log,
                    "replica {} has higher priority [{}], higher than current replica {} [{}]", replica, replica_leader_priority, replica_name, params.leader_priority);
                return true;
            }
        }
        else /// delta > max_delay_to_yield_leadership
        {
            LOG_INFO(
                log,
                "replica {} [delay {}] is newer then current replica {} [delay {}]", replica, replica_absolute_delay, replica_name, absolute_delay);
            return true;
        }
    }

    return false;
}

HaKafkaAddress StorageHaKafka::getHaKafkaAddress()
{
    HaKafkaAddress res;
    res.host = getContext()->getInterserverIOAddress().first;
    res.ha_port = getContext()->getHaTCPPort();
    res.database = database_name;
    res.table = table_name;
    return res;
}

HaKafkaAddress StorageHaKafka::getReplicaAddress(const String & target_replica_name)
{
    auto replica_host_path = zookeeper_path + "/replicas/" + target_replica_name + "/host";
    return HaKafkaAddress(getZooKeeper()->get(replica_host_path));
}

void StorageHaKafka::startConsume()
{
    removeStopMarkFile();
    enterLeaderElection();
}

void StorageHaKafka::stopConsume()
{
    createStopMarkFile();
    exitLeaderElection();
}

void StorageHaKafka::restartConsume()
{
    /// DO NOT need create mark
    exitLeaderElection();
    startConsume();
}

KafkaLogElement StorageHaKafka::createKafkaLog(KafkaLogElement::Type type, size_t consumer_index)
{
    KafkaLogElement elem;
    elem.event_type = type;
    elem.event_time = time(nullptr);
    elem.database = database_name;
    elem.table = table_name;
    elem.consumer = toString(consumer_index);
    return elem;
}

SettingsChanges StorageHaKafka::createSettingsAdjustments()
{
    SettingsChanges result;
    // Needed for backward compatibility
    if (settings.input_format_skip_unknown_fields.changed)
        // Always skip unknown fields regardless of the context (JSON or TSKV)
        result.emplace_back("input_format_skip_unknown_fields", settings.input_format_skip_unknown_fields.value);
    else
        result.emplace_back("input_format_skip_unknown_fields", 1u);

    if (settings.input_format_allow_errors_ratio.changed)
        result.emplace_back("input_format_allow_errors_ratio", settings.input_format_allow_errors_ratio.value);
    else
        result.emplace_back("input_format_allow_errors_ratio", 1.0);

    result.emplace_back("input_format_allow_errors_num", settings.skip_broken_messages.value);

    if (!settings.schema.value.empty())
        result.emplace_back("format_schema", settings.schema.value);

    result.emplace_back("input_format_json_aggregate_function_type_base64_encode", settings.json_aggregate_function_type_base64_encode.value);
    result.emplace_back("format_protobuf_enable_multiple_message", settings.protobuf_enable_multiple_message.value);
    result.emplace_back("format_protobuf_default_length_parser", settings.protobuf_default_length_parser.value);

    return result;
}

void StorageHaKafka::changeSettings(const AlterCommands & cur_params)
{
    auto new_settings = this->settings;

    for (auto & command : cur_params)
    {
        if (!command.settings_changes.empty())
            new_settings.applyKafkaSettingChanges(command.settings_changes);
    }
}

NamesAndTypesList StorageHaKafka::getVirtuals() const
{
    return NamesAndTypesList{
        {"_topic", std::make_shared<DataTypeString>()},
        {"_timestamp", std::make_shared<DataTypeUInt64>()},
        {"_key", std::make_shared<DataTypeString>()},
        {"_offset", std::make_shared<DataTypeUInt64>()},
        {"_partition", std::make_shared<DataTypeUInt64>()},
        {"_info", std::make_shared<DataTypeString>()},
        {"_content", std::make_shared<DataTypeString>()}
    };
}

Names StorageHaKafka::getVirtualColumnNames() const
{
    return Names {
        "_topic",
        "_timestamp",
        "_key",
        "_offset",
        "_partition",
        "_info",
        "_content"
    };
}

/*---------------------------------------------------------------------------*/

static StoragePtr createStorageHaKafka([[maybe_unused]] const StorageFactory::Arguments & args)
{
    ASTs & engine_args = args.engine_args;
    size_t args_count = engine_args.size();
    if (args_count < 2)
        throw Exception(
            "HaKafka requires 2 arguments (zookeeper path and replica name)",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// For Replicated.
    String zookeeper_path;
    String replica_name;

    if (auto ast = engine_args[0]->as<ASTLiteral>(); ast && ast->value.getType() == Field::Types::String)
        zookeeper_path = safeGet<String>(ast->value);
    else
        throw Exception("Path in ZooKeeper must be a string literal", ErrorCodes::BAD_ARGUMENTS);

    if (auto ast = engine_args[1]->as<ASTLiteral>(); ast && ast->value.getType() == Field::Types::String)
        replica_name = safeGet<String>(ast->value);
    else
        throw Exception("Replica name must be a string literal", ErrorCodes::BAD_ARGUMENTS);

    if (replica_name.empty())
        throw Exception("No replica name in config", ErrorCodes::NO_REPLICA_NAME_GIVEN);

    if (!args.storage_def->settings)
        throw Exception("HaKafka requires SETTINGS", ErrorCodes::BAD_ARGUMENTS);

    for (auto & change : args.storage_def->settings->as<ASTSetQuery &>().changes)
    {
        // removeKafkaPrefix(change.name);
        addKafkaPrefix(change.name);
    }
    sortKafkaSettings(*args.storage_def->settings);

    KafkaSettings kafka_settings;
    kafka_settings.loadFromQuery(*args.storage_def);

#define CHECK_HA_KAFKA_STORAGE_ARGUMENT(PAR_NAME) \
    if (!kafka_settings.PAR_NAME.changed) \
    { \
        throw Exception( \
            "Required parameter '" #PAR_NAME "' " \
            "for storage HaKafka not specified", \
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH); \
    }
    CHECK_HA_KAFKA_STORAGE_ARGUMENT(topic_list)
    CHECK_HA_KAFKA_STORAGE_ARGUMENT(group_name)
    CHECK_HA_KAFKA_STORAGE_ARGUMENT(format)
#undef CHECK_HA_KAFKA_STORAGE_ARGUMENT

    return StorageHaKafka::create(
        zookeeper_path,
        replica_name,
        args.attach,
        args.relative_data_path,
        args.table_id,
        args.getContext(),
        args.columns,
        args.constraints,
        kafka_settings);
}

void registerStorageHaKafka(StorageFactory & factory)
{
    factory.registerStorage("HaKafka", createStorageHaKafka, StorageFactory::StorageFeatures{ .supports_settings = true, });
}
}
