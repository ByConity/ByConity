#include <CloudServices/CnchServerClientPool.h>
#include <DaemonManager/DaemonJobAutoStatistics.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Statistics/AutoStatisticsHelper.h>
#include <Statistics/AutoStatisticsManager.h>
#include <Statistics/AutoStatisticsRpcUtils.h>
#include <Statistics/AutoStatsTaskLogHelper.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/CollectTarget.h>
#include <Statistics/OptimizerStatisticsClient.h>
#include <Statistics/StatisticsCollector.h>
#include <Statistics/SubqueryHelper.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageMergeTree.h>
#include <boost/algorithm/string.hpp>
#include <Poco/Logger.h>
#include "Common/time.h"
#include "common/logger_useful.h"
#include "Core/UUID.h"

#include <memory>
#include <mutex>
#include <regex>

namespace DB::Statistics::AutoStats
{

std::atomic_bool AutoStatisticsManager::is_initialized = false;
std::atomic_bool AutoStatisticsManager::xml_config_is_enable = false;

namespace XmlConfig
{
    String prefix = "optimizer.statistics.auto_statistics.";
    // this flag is to control whether AutoStatisticsManager should be running
    // if set to false, the whole function will be disabled
    String is_enabled = prefix + "enable";
}

// control if the whole AutoStatistics should be enabled
// including Manager and MemoryRecord
bool AutoStatisticsManager::xmlConfigIsEnable()
{
    return xml_config_is_enable.load();
}

std::pair<Time, Time> parseTimeWindow(const String & text)
{
    auto throw_error = [&] {
        auto err_msg = fmt::format(FMT_STRING("settings collect_window has invalid value: '{}'"), text);
        throw Exception(err_msg, ErrorCodes::BAD_ARGUMENTS);
    };

    // force HH:MM-HH:MM
    if (text.size() != 11 || text[5] != '-')
    {
        throw_error();
    }

    auto begin_text = text.substr(0, 5);
    auto begin_opt = getTimeFromString(begin_text);

    if (!begin_opt.has_value())
        throw_error();

    auto end_text = text.substr(6, 5);
    auto end_opt = getTimeFromString(end_text);

    if (!end_opt.has_value())
        throw_error();

    return std::make_pair(begin_opt.value(), end_opt.value());
}

// parse other config for execution
// refer to https://bytedance.feishu.cn/docx/LO75devznoUyKBxOHLKc4R9WnDb

void AutoStatisticsManager::prepareNewConfig(const Poco::Util::AbstractConfiguration & config)
{
    // in xml, optimizer.statistics.auto_statistics.enable will be TRUE
    // only useful to stop auto stats at all
    // bool is_enabled = config.getBool(XmlConfig::is_enabled, true);
    settings_manager.loadSettingsFromXml(config);
    xml_config_is_enable = settings_manager.getManagerSettings().enable_auto_stats();
}

AutoStatisticsManager::AutoStatisticsManager(ContextMutablePtr auth_context_)
    : auth_context(auth_context_) 
    , logger(getLogger("AutoStatisticsManager"))
    , task_queue(auth_context_, internal_config)
    , settings_manager(auth_context_)
    , schedule_lease(TimePoint{}) // just make compiler happy
{
    // do scanning and udi flush the next interval
    last_time_udi_flush = nowTimePoint();
    // immediately scan all tables if possiable
    last_time_scan_all_tables = TimePoint{};
    schedule_lease = nowTimePoint();
}

static String timeToString(Time time)
{
    auto & lut = DateLUT::serverTimezoneInstance();
    auto t = lut.toDateTimeComponents(time).time;
    return fmt::format("{:02d}:{:02d}", t.hour, t.minute);
}

void AutoStatisticsManager::run()
{
    auto context = getContext();
    auto config_checker = [&] {
        loadNewConfigIfNeeded();
        if (!xmlConfigIsEnable())
        {
            LOG_INFO(logger, "auto stats is disabled by config");
            return false;
        }
        else if (!internal_config.enable_auto_stats())
        {
            LOG_INFO(logger, "auto stats is disabled by system.auto_stats_manager_settings");
            return false;
        }
        else
        {
            LOG_TRACE(logger, "auto stats is enabled");
            return true;
        }
    };

    try
    {
        LOG_INFO(logger, "New Iteration");

        if (!information_is_valid)
        {
            config_checker();
            initializeInfo();
        }

        while (true)
        {
            auto auto_stats_enabled = config_checker();

            // even if auto stats is out of time range
            // we should record udi_counter to storage so that restarts won't lose too many data
            // so just check auto_stats_enabled
            if (!auto_stats_enabled)
            {
                LOG_INFO(logger, "skip iteration");
                break;
            }

            updateUdiInfo();

            auto init_min_event_time = nowTimePoint() - std::chrono::days(internal_config.task_expire_days());
            auto fetch_log_time_point = updateTaskQueueFromLog(next_min_event_time.value_or(init_min_event_time));
            next_min_event_time = fetch_log_time_point - std::chrono::seconds(internal_config.schedule_period_seconds());

            auto is_time_range = isNowValidTimeRange();

            auto mode = [&] {
                using Mode = TaskQueue::Mode;
                Mode res;
                res.enable_async_tasks = internal_config.enable_async_tasks();
                res.enable_normal_auto_stats = auto_stats_enabled && is_time_range;
                res.enable_immediate_auto_stats
                    = auto_stats_enabled && (is_time_range || internal_config.collect_empty_stats_immediately());
                return res;
            }();


            auto chosen_task = task_queue.chooseTask(mode);

            // all is done, just break;
            if (!chosen_task)
                break;

            executeOneTask(chosen_task);
        }

        if (task_queue.size() == 0)
        {
            this->scanAllTables();
        }
    }
    catch (...)
    {
        tryLogCurrentException(logger);
        clearOnException();
    }

    auto schedule_period = std::chrono::seconds(internal_config.schedule_period_seconds());
    auto info_msg = fmt::format(FMT_STRING("Finish Iteration, schedule after {} seconds"), schedule_period / std::chrono::seconds(1));
    LOG_INFO(logger, info_msg);
    schedule_lease = nowTimePoint() + schedule_period;
}

void AutoStatisticsManager::scanAllTables()
{
    auto context = getContext();
    if (!internal_config.enable_scan_all_tables())
    {
        LOG_TRACE(logger, "scanning all tables is disabled");
        return;
    }

    auto scan_all_tables_lease = last_time_scan_all_tables + std::chrono::seconds(internal_config.scan_all_tables_interval_seconds());
    if (nowTimePoint() < scan_all_tables_lease)
    {
        LOG_TRACE(logger, "scanning all tables is cooling down");
        return;
    }
    LOG_INFO(logger, "start scanning all tables");
    Stopwatch watch;
    try
    {
        // TODO: filter by storage host
        auto candidates = getTablesInScope(context, StatisticsScope{});
        markCollectableCandidates(candidates, false);
        LOG_INFO(logger, "finish scanning all tables in {} seconds", watch.elapsedSeconds());
    }
    catch (...)
    {
        tryLogCurrentException(logger);
        LOG_INFO(logger, "scanning all tables throw exception in {} seconds", watch.elapsedSeconds());
    }
    last_time_scan_all_tables = nowTimePoint();
}

void AutoStatisticsManager::markCollectableCandidates(
    const std::vector<StatsTableIdentifier> & candidates, bool force_collect_if_failed_to_query_row_count)
{
    auto context = getContextWithNewTransaction(getContext(), true, true);
    auto catalog = createCatalogAdaptor(context);
    UInt64 new_table_collecable = 0;
    UInt64 good_table = 0;
    for (const auto & identifier : candidates)
    {
        auto table_settings = settings_manager.getTableSettings(identifier);
        // check if identifier is valid
        if (!table_settings.enable_auto_stats)
        {
            continue;
        }

        auto storage = catalog->getStorageByTableId(identifier);
        auto host_opt = getRemoteTargetServerIfHas(context, storage.get());
        if (host_opt)
        {
            continue;
        }

        // check status in manager, if running, not add
        auto task = task_queue.tryGetTaskInfo(identifier.getUniqueKey());
        if (task)
        {
            auto status = task->getStatus();
            if (status == Status::Created || status == Status::Running || status == Status::Pending)
                continue;
        }

        // check stats is healthy, if yes, skip adding
        std::optional<double> priority;
        UInt64 stats_row_count = 0;
        UInt64 udi = 0;

        auto table_stats = getTableStatistics(context, identifier);
        if (!table_stats)
        {
            // table has no stats, must collect it
            priority = 1000;
        }
        else
        {
            // when table has stats
            stats_row_count = table_stats->getRowCount();
            auto real_count = catalog->queryRowCount(identifier);
            if (!real_count)
            {
                // we cannot determined real row count due to Storage limitation (no trivial count)
                // we just do add it to collect when it's triggered by [create auto stats]
                if (force_collect_if_failed_to_query_row_count)
                    priority = 1.0 / stats_row_count; // larger table has lower priority
                else
                    priority = std::nullopt;
            }
            else
            {
                // determined whether to collect it by calcPriority
                // nullopt means no need to collect
                udi = std::abs<UInt64>(real_count.value() - stats_row_count);
                priority = calcPriority(internal_config, udi, stats_row_count);
            }
        }

        if (priority.has_value())
        {
            LOG_INFO(logger, "table {} is marked as collectable", identifier.getNameForLogs());
            // add this task to task log
            TaskInfoCore core{
                .task_uuid = UUIDHelpers::generateV4(),
                .task_type = TaskType::Auto,
                .table = identifier,
                .stats_row_count = stats_row_count,
                .udi_count = udi,
                .priority = priority.value(),
                .retry_times = 0,
                .status = Status::Created,
            };
            ++new_table_collecable;
            writeTaskLog(context, core, "create auto stats");
        }
        else
        {
            ++good_table;
            LOG_TRACE(logger, "table {} is healthy, skip auto stats", identifier.getNameForLogs());
        }
    }
    LOG_INFO(
        logger,
        "{} table(s) are newly collected by recent scan/create_auto_stats_trigger, {} table(s) are skipped",
        new_table_collecable,
        good_table);
    context->getAutoStatsTaskLog()->flush();
}


AutoStatisticsManager::~AutoStatisticsManager() = default;

namespace
{
    template <typename T>
    String logElement(const T & ele)
    {
        return toString(ele);
    }

    template <typename T, typename U, typename X>
    String logElement(const std::tuple<T, U, X> & tp)
    {
        auto & [t, u, x] = tp;
        return fmt::format(FMT_STRING("<{},{},{}>"), t, u, x);
    }

    template <typename T, typename U>
    String logElement(const std::pair<T, U> & pr)
    {
        return fmt::format(FMT_STRING("<{},{}>"), pr.first, pr.second);
    }


    template <typename T>
    String logForUdiMap(const String & prefix, const std::unordered_map<StatsTableIdentifier, T> & mapping)
    {
        String body;
        for (const auto & [table, element] : mapping)
        {
            String element_str = logElement(element);
            body += fmt::format(FMT_STRING("{}:{},"), table.getDbTableName(), element_str);
        }
        auto msg = fmt::format(FMT_STRING("{}:{{ {} }}"), prefix, body);
        return msg;
    }
}

void AutoStatisticsManager::updateUdiInfo()
{
    auto current_time = nowTimePoint();
    // calculate lease on the fly to make sure settings is effective immediately
    auto udi_flush_lease = last_time_udi_flush + std::chrono::seconds(internal_config.schedule_period_seconds());
    if (current_time < udi_flush_lease)
    {
        return;
    }
    last_time_udi_flush = current_time;

    auto context = getContext();
    auto catalog = createCatalogAdaptor(context);
    auto & record = internal_memory_record;

    auto collected_data = record.getAndClearAll();

    if (collected_data.empty())
    {
        return;
    }

    for (auto & [uuid, delta_udi] : collected_data)
    {
        auto table_opt = catalog->getTableIdByUUID(uuid);
        if (!table_opt.has_value())
        {
            auto err_msg = fmt::format(FMT_STRING("uuid {} not exists, maybe table has been dropped"), UUIDHelpers::UUIDToString(uuid));
            LOG_WARNING(logger, err_msg);
            continue;
        }
        auto table = table_opt.value();
        // TODO: refactor this with batch api to reduce impact on catalog
        auto old_udi = catalog->fetchAddUdiCount(table, delta_udi);
        auto new_udi = old_udi + delta_udi;
        if (new_udi == 0)
            continue;

        auto [table_row_count, timestamp] = getTableStatsFromCatalog(catalog, table);

        LOG_INFO(
            logger,
            fmt::format(
                FMT_STRING("trying to add new task: table={}, rows={}, uid={}"), //
                table.getNameForLogs(),
                table_row_count,
                new_udi));

        logTaskIfNeeded(table, new_udi, table_row_count);
    }
}

std::optional<StatsTableIdentifier> normalizeTable(ContextPtr context, StatsTableIdentifier table)
{
    // in cnch, UUID is reliable
    auto catalog = createCatalogAdaptor(context);
    return catalog->getTableIdByUUID(table.getUUID());
}

ContextMutablePtr createContextWithAuth(ContextPtr global_context)
{
    // TODO: create context with auth
    auto context = Context::createCopy(global_context);
    auto [user, pass] = global_context->getCnchInterserverCredentials();
    context->setUser(user, pass, Poco::Net::SocketAddress{});
    return context;
}

bool AutoStatisticsManager::executeOneTask(const std::shared_ptr<TaskInfo> & chosen_task)
{
    auto context = getContext();
    auto catalog = createCatalogAdaptor(context);
    auto unique_key = chosen_task->getTable().getUniqueKey();

    try
    {
        auto table_opt = normalizeTable(context, chosen_task->getTable());

        if (!table_opt)
        {
            auto msg = fmt::format(FMT_STRING("Table {} is no longer valid, skip it"), chosen_task->getTable().getNameForLogs());
            LOG_WARNING(logger, msg);
            chosen_task->setStatus(Status::Failed);
            writeTaskLog(context, *chosen_task, msg);
            task_queue.erase(unique_key);
            return false;
        }

        auto table = table_opt.value();
        auto table_settings = settings_manager.getTableSettings(table);

        if (!table_settings.enable_auto_stats)
        {
            auto msg = fmt::format(
                FMT_STRING("Table {} is disable to auto collect by config, skip it"), chosen_task->getTable().getNameForLogs());
            LOG_WARNING(logger, msg);
            chosen_task->setStatus(Status::Cancelled);
            writeTaskLog(context, *chosen_task, msg);
            task_queue.erase(unique_key);
            return true;
        }

        LOG_INFO(logger, fmt::format(FMT_STRING("start auto stats on {}"), table.getNameForLogs()));

        chosen_task->setStatus(Status::Running);
        writeTaskLog(context, *chosen_task);

        auto columns_name = chosen_task->getColumnsName();

        CreateStatsSettings collector_settings;
        if (chosen_task->getTaskType() == TaskType::Manual)
        {
            if (!chosen_task->getSettingsJson().empty())
            {
                collector_settings.fromJsonStr(chosen_task->getSettingsJson());
            }
        }
        else
        {
            auto global_settings = context->getSettings();
            global_settings.applyChanges(table_settings.settings_changes);
            collector_settings.fromContextSettings(global_settings);
        }

        CollectTarget target(context, table, collector_settings, columns_name);
        // here we should create a context with auto stats info
        auto query_context = getContextWithNewTransaction(context, false, true);
        auto row_count_opt = collectStatsOnTarget(query_context, target);

        if (row_count_opt.has_value())
        {
            // normal execution finishes
            auto row_count = row_count_opt.value();
            LOG_INFO(logger, fmt::format(FMT_STRING("finish auto stats on {}"), table.getNameForLogs()));
            chosen_task->setStatus(Status::Success);
            chosen_task->setParams(0, row_count, 0);
            writeTaskLog(context, *chosen_task);
            task_queue.erase(unique_key);
        }
        else
        {
            // trigger if_not_exists and skip collection due to stats exists
            // task is still successful
            LOG_INFO(logger, fmt::format(FMT_STRING("skip auto stats on {} since stats exists"), table.getNameForLogs()));
            chosen_task->setStatus(Status::Success);
            chosen_task->setParams(0, 0, 0);
            writeTaskLog(context, *chosen_task, "skip since stats exists");
            task_queue.erase(unique_key);
        }

        return true;
    }
    catch (...)
    {
        tryLogCurrentException(logger);

        auto err_msg = getCurrentExceptionMessage(false);
        auto err_log = fmt::format(FMT_STRING("auto stats task {} fails, because {}"), chosen_task->getTable().getNameForLogs(), err_msg);
        LOG_ERROR(logger, err_log);

        chosen_task->setStatus(Status::Error);
        writeTaskLog(context, *chosen_task, err_msg);
        task_queue.erase(unique_key);
        return false;
    }
}

void AutoStatisticsManager::clearOnException()
{
    information_is_valid = false;
    task_queue.clear();
}


void AutoStatisticsManager::initializeInfo()
{
    auto context = getContext();
    loadNewConfigIfNeeded();
    LOG_INFO(logger, "init info since server restarts or has exception");

    // check health
    auto catalog = createCatalogAdaptor(context);
    // just check read health, because
    // write health checker works by inserting into distributed table
    // all servers inserts into a distributed table will cost O(N^2)
    catalog->checkHealth(false);
    task_queue.clear();

    next_min_event_time = std::nullopt;
    information_is_valid = true;
}

// we have to ensure only
void AutoStatisticsManager::initialize(ContextMutablePtr context_, const Poco::Util::AbstractConfiguration & config)
{
    if (context_->getServerType() != ServerType::cnch_server)
        return;

    if (auto task_log = context_->getAutoStatsTaskLog())
    {
        // use flush to create table if not exists
        task_log->flush(true);
    }
    else
    {
        LOG_WARNING(getLogger("AutoStatisticsManager::initialize"), "cnch_system.cnch_auto_stats_task_log is not initialized");
    }

    // zk helper will make only one manager runs at the whole cluster
    if (AutoStatisticsManager::is_initialized)
        throw Exception("AutoStatisticsManager should call initialize only once", ErrorCodes::LOGICAL_ERROR);

    {
        auto auth_context = createContextWithAuth(context_);
        auto the_instance = std::make_unique<AutoStatisticsManager>(std::move(auth_context));
        context_->setAutoStatisticsManager(std::move(the_instance));
    }
    auto * the_instance = context_->getAutoStatisticsManager();

    the_instance->prepareNewConfig(config);

    LOG_INFO(the_instance->logger, "Create Thread Pool with Setting (1, 0, 1)");
    the_instance->thread_pool = std::make_unique<ThreadPool>(1, 0, 1);
    AutoStatisticsManager::is_initialized = true; // ready for instance
}

bool AutoStatisticsManager::isNowValidTimeRange()
{
    auto current_time = time(nullptr);
    auto current_time_point = DateLUT::serverTimezoneInstance().toTime(current_time);

    auto [begin_time, end_time] = parseTimeWindow(internal_config.collect_window());
    if (betweenTime(current_time_point, begin_time, end_time))
    {
        LOG_DEBUG(
            logger,
            fmt::format(
                FMT_STRING("time in range, {} is between {} and {}"),
                timeToString(current_time_point),
                timeToString(begin_time),
                timeToString(end_time)));

        return true;
    }

    auto msg = fmt::format(
        FMT_STRING("skip collection due to time out of range, {} is not between {} and {}"),
        timeToString(current_time_point),
        timeToString(begin_time),
        timeToString(end_time));
    LOG_INFO(logger, msg);
    return false;
}

void AutoStatisticsManager::loadNewConfigIfNeeded()
{
    // TODO: reload settings needs reconsider,
    // TODO: since it should be placed into DaemonManager
    // since fetching from settings is cheap, always load
    internal_config = settings_manager.getManagerSettings();
}

void AutoStatisticsManager::scheduleDistributeUdiCount()
{
    auto context = getContext();
    auto catalog = createCatalogAdaptor(context);
    auto servers = DaemonManager::DaemonJobAutoStatistics::getServerList(context);
    using RecordType = std::unordered_map<UUID, UInt64>;
    RecordType all_record;
    for (auto & host_with_ports : servers)
    {
        auto server_cli = context->getCnchServerClientPool().get(host_with_ports);
        auto remote_record = server_cli->queryUdiCounter();
        for (const auto & [k, v] : remote_record)
        {
            all_record[k] += v;
        }
    }
    std::unordered_map<HostWithPorts, RecordType> remote_records;
    RecordType local_record;

    for (const auto & [k, v] : all_record)
    {
        auto storage = catalog->tryGetStorageByUUID(k);
        if (!storage)
            continue;

        auto host_opt = getRemoteTargetServerIfHas(context, storage.get());
        if (host_opt)
        {
            remote_records[host_opt.value()][k] = v;
        }
        else
        {
            local_record[k] = v;
        }
    }

    internal_memory_record.append(local_record);
    for (const auto & [host_with_ports, record] : remote_records)
    {
        auto server_cli = context->getCnchServerClientPool().get(host_with_ports);
        server_cli->redirectUdiCounter(record);
    }
}

void AutoStatisticsManager::scheduleCollect()
{
    std::unique_lock<std::mutex> lock(manager_mtx, std::defer_lock);
    if (!lock.try_lock())
    {
        LOG_INFO(logger, "last submission is not finished, skip iteration");
        return;
    }

    if (thread_pool->active() > 0)
    {
        LOG_INFO(logger, "last iteration is not finished, skip iteration");
        return;
    }

    auto now = nowTimePoint();
    if (now < schedule_lease.load())
    {
        LOG_INFO(logger, "last iteration is cooling down, skip iteration");
    }

    LOG_INFO(logger, "start iteration");
    thread_pool->scheduleOrThrowOnError([this]() { this->run(); });
}

void AutoStatisticsManager::writeMemoryRecord(const std::unordered_map<UUID, UInt64> & record)
{
    for (auto & [k, v] : record)
    {
        internal_memory_record.addUdiCount(k, v);
    }
}
// return max_event_time of current log
TimePoint AutoStatisticsManager::updateTaskQueueFromLog(TimePoint min_event_time)
{
    auto context = getContext();
    context->getAutoStatsTaskLog()->flush(true);

    /// record current time before read log
    /// in case reading log consume too much time
    auto current_time = nowTimePoint();

    auto task_log_infos = batchReadTaskLog(context, convertToDateTime64(min_event_time));
    UInt64 log_count = 0;
    for (const auto & task_log_info : task_log_infos)
    {
        ++log_count;
        task_queue.updateTasksFromLogIfNeeded(task_log_info);
    }
    auto task_count = this->task_queue.size();
    LOG_INFO(logger, "processed {} new log entry, now {} tasks is in list", log_count, task_count);
    return current_time;
}

void AutoStatisticsManager::logTaskIfNeeded(const StatsTableIdentifier & table, UInt64 udi_count, UInt64 stats_row_count)
{
    auto context = getContext();
    auto priority_opt = calcPriority(internal_config, udi_count, stats_row_count);
    if (!priority_opt)
        return;
    auto priority = priority_opt.value();
    auto info_log = fmt::format(
        FMT_STRING("auto task {} with priority={}, udi={}, stats_row_count={}"),
        table.getNameForLogs(),
        priority,
        udi_count,
        stats_row_count);

    auto task = task_queue.tryGetTaskInfo(table.getUniqueKey());
    if (task)
    {
        // task exists, just update priority
        task->setParams(udi_count, stats_row_count, priority);
        LOG_INFO(logger, fmt::format(FMT_STRING("update: {}"), info_log));
        writeTaskLog(context, *task, "update task info");
    }
    else
    {
        auto task_info_core = TaskInfoCore{
            .task_uuid = UUIDHelpers::generateV4(),
            .task_type = TaskType::Auto,
            .table = table,
            .settings_json = "",
            .stats_row_count = stats_row_count,
            .udi_count = udi_count,
            .priority = priority,
            .retry_times = 0,
            .status = Status::Created,
        };

        LOG_INFO(logger, fmt::format(FMT_STRING("create: {}"), info_log));
        writeTaskLog(context, task_info_core);
    }
}

}
