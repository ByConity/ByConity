#include <DaemonManager/DaemonJobAutoStatistics.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <CloudServices/CnchServerClientPool.h>
#include <Statistics/AutoStatisticsHelper.h>
#include <Statistics/AutoStatisticsManager.h>
#include <Statistics/AutoStatisticsRpcUtils.h>
#include <Statistics/AutoStatsTaskLogHelper.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/CollectTarget.h>
#include <Statistics/StatisticsCollector.h>
#include <Statistics/SubqueryHelper.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageMergeTree.h>
#include <boost/algorithm/string.hpp>

#include <memory>
#include <mutex>
#include <regex>

namespace DB::Statistics::AutoStats
{

std::atomic_bool AutoStatisticsManager::is_initialized = false;
std::atomic_bool AutoStatisticsManager::config_is_enabled = false;
std::unique_ptr<AutoStatisticsManager> AutoStatisticsManager::the_instance;

// refer to https://****
namespace Config
{
    String prefix = "optimizer.statistics.auto_statistics.";
    // enable auto statistics, including udi count service
    String is_enabled = prefix + "enable";
    String collect_window = prefix + "collect_window";
    String collect_empty_stats_immediately = prefix + "collect_empty_stats_immediately";

    String enable_sample = prefix + "enable_sample";
    String sample_ratio = prefix + "sample_ratio";
    String sample_row_count = prefix + "sample_row_count";

    String task_expire_days = prefix + "task_expire_days";

    String update_ratio_threshold = prefix + "update_ratio_threshold";
    String update_row_count_threshold = prefix + "update_row_count_threshold";
    String max_retry_times = prefix + "max_retry_times";

    String schedule_period_seconds = prefix + "schedule_period_seconds";
    String udi_flush_interval_seconds = prefix + "udi_flush_interval_seconds";
    String collect_interval_for_one_table_seconds = prefix + "collect_interval_for_one_table_seconds";

    String enable_async_tasks = prefix + "enable_async_tasks";
}


// control if the whole AutoStatistics should be enabled
// including Manager and MemoryRecord
bool AutoStatisticsManager::configIsEnabled()
{
    return config_is_enabled.load();
}

std::pair<Time, Time> parseTimeWindow(const String & text)
{
    auto throw_error = [&] {
        auto err_msg = fmt::format(FMT_STRING("option {} has invalid value: '{}'"), Config::collect_window, text);
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
// refer to https://****
void AutoStatisticsManager::prepareNewConfig(const Poco::Util::AbstractConfiguration & config)
{
    // TODO: change this to true after testing
    bool is_enabled = config.getBool(Config::is_enabled, false);
    InternalConfig new_config;

    #if 0
    auto collect_window_text = config.getString(Config::collect_window, "00:00-23:59");
    std::tie(new_config.begin_time, new_config.end_time) = parseTimeWindow(collect_window_text);

    new_config.collect_empty_stats_immediately = config.getBool(Config::collect_empty_stats_immediately, false);

    new_config.collector_settings.enable_sample = config.getBool(Config::enable_sample, true);
    new_config.collector_settings.sample_ratio = config.getDouble(Config::sample_ratio, 0.01);
    new_config.collector_settings.sample_row_count = config.getUInt64(Config::sample_row_count, 40'000'000);

    new_config.update_ratio_threshold = config.getDouble(Config::update_ratio_threshold, 0.2);
    new_config.update_row_count_threshold = config.getUInt64(Config::update_row_count_threshold, 1'000'000);
    new_config.max_retry_times = config.getUInt64(Config::max_retry_times, 3);

    new_config.schedule_period = std::chrono::seconds(config.getUInt64(Config::schedule_period_seconds, 60));
    new_config.udi_flush_interval = std::chrono::seconds(config.getUInt64(Config::udi_flush_interval_seconds, 60));
    new_config.collect_interval_for_one_table
        = std::chrono::seconds(config.getUInt64(Config::collect_interval_for_one_table_seconds, 1200));

    new_config.task_expire = std::chrono::days(config.getUInt64(Config::task_expire_days, 7));
    new_config.enable_async_tasks = config.getBool(Config::enable_async_tasks, true);
    #endif

    std::unique_lock lck(config_mtx);
    config_is_enabled = is_enabled;
    new_internal_config = std::move(new_config);
}

AutoStatisticsManager::AutoStatisticsManager(ContextPtr context_)
    : context(context_)
    , logger(&Poco::Logger::get("AutoStatisticsManager"))
    , task_queue(context_, internal_config)
    , schedule_lease(TimePoint{}) // std::chrono::time_point() is not nothrow at gcc 9.4, make compiler happy

{
    auto schedule_period = std::chrono::seconds(internal_config.schedule_period_seconds());
    schedule_lease = nowTimePoint() + schedule_period;
    udi_flush_lease = nowTimePoint() + schedule_period;
}

static String timeToString(Time time)
{
    auto & lut = DateLUT::instance();
    auto hr = lut.toHour(time);
    auto min = lut.toMinute(time);
    return fmt::format("{:02d}:{:02d}", hr, min);
}

void AutoStatisticsManager::run()
{
    auto config_checker = [&] {
        loadNewConfigIfNeeded();
        if (!configIsEnabled())
        {
            LOG_INFO(logger, "auto stats is disabled by config, skip iteration");
            return false;
        }
        else
        {
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
            updateUdiInfo();

            auto fetch_log_time_point = updateTaskQueueFromLog(next_min_event_time);
            next_min_event_time = fetch_log_time_point - std::chrono::seconds(internal_config.schedule_period_seconds());

            auto is_time_range = isNowValidTimeRange();

            auto mode = [&] {
                using Mode = TaskQueue::Mode;
                Mode mode_;
                mode_.enable_async_tasks = internal_config.enable_async_tasks();
                mode_.enable_normal_auto_stats = auto_stats_enabled && is_time_range;
                mode_.enable_immediate_auto_stats = auto_stats_enabled && (is_time_range || internal_config.collect_empty_stats_immediately());
                return mode_;
            }();


            auto chosen_task = task_queue.chooseTask(mode);

            // all is done, just break;
            if (!chosen_task)
                break;

            executeOneTask(chosen_task);
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

AutoStatisticsManager::~AutoStatisticsManager()
{
}

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
    if (current_time < udi_flush_lease)
    {
        return;
    }

    udi_flush_lease = current_time + std::chrono::seconds(internal_config.update_interval_seconds());

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
                FMT_STRING("trying to add new task: uuid={}, rows={}, uid={}"), //
                UUIDHelpers::UUIDToString(uuid),
                table_row_count,
                new_udi));

        logTaskIfNeeded(table, new_udi, table_row_count, timestamp);
    }
}

bool AutoStatisticsManager::executeOneTask(const std::shared_ptr<TaskInfo> & chosen_task)
{
    auto catalog = createCatalogAdaptor(context);

    auto uuid = chosen_task->getTable().getUniqueKey();
    auto table_opt = catalog->getTableIdByUUID(uuid);

    if (!table_opt)
    {
        // uuid is invalid, possibly due to table is dropped
        auto err_msg = fmt::format(FMT_STRING("uuid {} is no longer valid, skip auto stats task"), toString(uuid));
        LOG_WARNING(logger, err_msg);
        chosen_task->setStatus(Status::Failed);
        writeTaskLog(context, *chosen_task, err_msg);
        task_queue.erase(uuid);
        return true;
    }

    StatsTableIdentifier table = table_opt.value();
    try
    {
        LOG_INFO(logger, fmt::format(FMT_STRING("start auto stats on {}"), table.getNameForLogs()));

        chosen_task->setStatus(Status::Running);
        writeTaskLog(context, *chosen_task);

        CreateStatsSettings settings; // TODO
        auto columns_name = chosen_task->getColumnsName();

        // TODO: gouguilin
        // if (!chosen_task->getSettingsJson().empty())
        // {
        //     settings.fromJsonStr(chosen_task->getSettingsJson());
        // }

        CollectTarget target(context, table, settings, columns_name);
        auto row_count_opt = collectStatsOnTarget(context, target);

        if (row_count_opt.has_value())
        {
            // normal execution finishes
            auto row_count = row_count_opt.value();
            LOG_INFO(logger, fmt::format(FMT_STRING("finish auto stats on {}"), table.getNameForLogs()));
            chosen_task->setStatus(Status::Success);
            chosen_task->setParams(0, row_count, 0);
            writeTaskLog(context, *chosen_task);
            task_queue.erase(uuid);
        }
        else
        {
            // trigger if_not_exists and skip collection due to stats exists
            // task is still successful
            LOG_INFO(logger, fmt::format(FMT_STRING("skip auto stats on {} since stats exists"), table.getNameForLogs()));
            chosen_task->setStatus(Status::Success);
            chosen_task->setParams(0, 0, 0);
            writeTaskLog(context, *chosen_task, "skip since stats exists");
            task_queue.erase(uuid);
        }

        return true;
    }
    catch (...)
    {
        tryLogCurrentException(logger);

        auto err_msg = getCurrentExceptionMessage(false);
        auto err_log = fmt::format(FMT_STRING("auto stats task {} fails, because {}"), table.getNameForLogs(), err_msg);
        LOG_ERROR(logger, err_log);

        chosen_task->setStatus(Status::Error);
        writeTaskLog(context, *chosen_task, err_msg);
        task_queue.erase(uuid);
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
    loadNewConfigIfNeeded();
    LOG_INFO(logger, "init info since server restarts or has exception");

    // check health
    auto catalog = createCatalogAdaptor(context);
    catalog->checkHealth(true);
    task_queue.clear();

    auto init_min_event_time = nowTimePoint() - std::chrono::days(internal_config.task_expire_days());
    auto fetch_log_time_point = updateTaskQueueFromLog(init_min_event_time);
    next_min_event_time = fetch_log_time_point - std::chrono::seconds(internal_config.schedule_period_seconds());

    information_is_valid = true;
}

// we have to ensure only
void AutoStatisticsManager::initialize(ContextPtr context_, const Poco::Util::AbstractConfiguration & config)
{
    if (context_->getServerType() != ServerType::cnch_server)
        return;
    // use flush to create table if not exists
    context_->getAutoStatsTaskLog()->flush(true);

    // zk helper will make only one manager runs at the whole cluster
    if (AutoStatisticsManager::is_initialized)
        throw Exception("AutoStatisticsManager should call initialize only once", ErrorCodes::LOGICAL_ERROR);

    the_instance = std::make_unique<AutoStatisticsManager>(context_);
    the_instance->prepareNewConfig(config);

    LOG_INFO(the_instance->logger, "Create Thread Pool with Setting (1, 0, 1)");
    the_instance->thread_pool = std::make_unique<ThreadPool>(1, 0, 1);
    AutoStatisticsManager::is_initialized = true; // ready for instance
}

AutoStatisticsManager * AutoStatisticsManager::tryGetInstance()
{
    if (AutoStatisticsManager::is_initialized && the_instance.get())
    {
        return the_instance.get();
    }
    else
    {
        return nullptr;
    }
}

bool AutoStatisticsManager::isNowValidTimeRange()
{
    auto current_time = time(nullptr);
    auto current_time_point = DateLUT::instance().toTime(current_time);

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
    std::unique_lock lck(config_mtx);
    if (!new_internal_config.has_value())
        return;
    internal_config = std::move(new_internal_config.value());
    new_internal_config = std::nullopt;
}

void AutoStatisticsManager::scheduleDistributeUdiCount()
{
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
    context->getAutoStatsTaskLog()->flush(true);

    /// record current time before read log
    /// in case reading log consume too much time
    auto current_time = nowTimePoint();

    auto task_log_infos = batchReadTaskLog(context, convertToDateTime64(min_event_time));
    for (const auto & task_log_info : task_log_infos)
    {
        task_queue.updateTasksFromLogIfNeeded(task_log_info);
    }

    return current_time;
}

void AutoStatisticsManager::logTaskIfNeeded(
    const StatsTableIdentifier & table, UInt64 udi_count, UInt64 stats_row_count, DateTime64 timestamp)
{
    auto priority_opt = calcPriority(internal_config, udi_count, stats_row_count);
    if (!priority_opt)
        return;
    auto priority = priority_opt.value();
    auto table_uuid = table.getUUID();
    auto info_log = fmt::format(
        FMT_STRING("auto task {} with priority={}, udi={}, stats_row_count={}"),
        table.getNameForLogs(),
        priority,
        udi_count,
        stats_row_count);

    auto task = task_queue.tryGetTaskInfo(table_uuid);
    if (task)
    {
        // task exists, just update priority
        task->setParams(udi_count, stats_row_count, priority);
        LOG_INFO(logger, fmt::format(FMT_STRING("update: {}"), info_log));
        writeTaskLog(context, *task, "update task info");
    }
    else
    {
        // create new table
        auto lease = convertToTimePoint(timestamp) + std::chrono::seconds(internal_config.update_interval_seconds());
        // wait for a schedule period, to start the collection
        // make the system happy
        auto lease2 = nowTimePoint() + std::chrono::seconds(internal_config.schedule_period_seconds());
        lease = std::max(lease, lease2);

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

        auto new_task = std::make_shared<TaskInfo>(task_info_core, lease);

        LOG_INFO(logger, fmt::format(FMT_STRING("create: {}"), info_log));
        writeTaskLog(context, *new_task);
    }
}

}
