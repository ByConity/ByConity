#include <CloudServices/DedupDataChecker.h>

#include <common/types.h>
#include <Common/Exception.h>
#include <CloudServices/DedupGran.h>
#include <CloudServices/ICnchBGThread.h>
#include <DataStreams/BlockIO.h>
#include <Interpreters/CnchSystemLog.h>
#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Storages/StorageCnchMergeTree.h>

#include <fmt/core.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int LOGICAL_ERROR;
    extern const int UNIQUE_TABLE_DUPLICATE_KEY_FOUND;
}

Names DedupDataChecker::getCheckerQuery(StoragePtr & storage, StorageCnchMergeTree & cnch_table, bool force_check_at_once)
{
    Names ret;
    auto settings = cnch_table.getSettings();
    auto get_unique_key_str = [&] () -> String {
        auto unique_names = cnch_table.getInMemoryMetadataPtr()->getUniqueKeyColumns();
        WriteBufferFromOwnString unique_key_str;
        unique_key_str << "(";
        for (size_t i = 0; i < unique_names.size(); i++)
        {
            if (i != 0)
                unique_key_str << ",";

            unique_key_str << unique_names[i];
        }
        unique_key_str << ")";
        return unique_key_str.str();
    };

    auto get_check_predicate_str = [&] (const String & partition_id, Int64 bucket_number) -> String {
        WriteBufferFromOwnString check_predicate_str;
        String partition_predicate_str, bucket_predicate_str;

        if (partition_id != FAKE_DEDUP_GRAN_PARTITION_ID)
            partition_predicate_str = fmt::format("_partition_id='{}'", partition_id);
        if (bucket_number != -1)
            bucket_predicate_str = fmt::format("_bucket_number='{}'", bucket_number);

        if (!settings->check_predicate.value.empty() || !partition_predicate_str.empty() || !bucket_predicate_str.empty())
        {
            check_predicate_str << "where ";
            bool need_and = false;
            if (!settings->check_predicate.value.empty())
            {
                need_and = true;
                check_predicate_str << settings->check_predicate.value;
            }
            if (!partition_predicate_str.empty())
            {
                if (need_and)
                    check_predicate_str << " and ";
                need_and = true;
                check_predicate_str << partition_predicate_str;
            }
            if (!bucket_predicate_str.empty())
            {
                if (need_and)
                    check_predicate_str << " and ";
                need_and = true;
                check_predicate_str << bucket_predicate_str;
            }
        }
        return check_predicate_str.str();
    };

    String group_by_str;
    /// It is recommended to use this setting in the future without checkBucketParts()
    if (settings->enable_bucket_level_unique_keys)
        group_by_str = settings->partition_level_unique_keys ? "_partition_id, _bucket_number," : "_bucket_number,";
    else
        group_by_str = settings->partition_level_unique_keys ? "_partition_id," : "";

    if (settings->check_duplicate_for_big_table && !force_check_at_once)
    {
        bool is_bucket_valid = settings->enable_bucket_level_unique_keys && cnch_table.isBucketTable();
        if (is_bucket_valid)
        {
            if (settings->partition_level_unique_keys)  /// partition + bucket
            {
                Names partition_ids = getContext()->getCnchCatalog()->getPartitionIDs(storage, nullptr);
                auto max_bucket_num = cnch_table.getInMemoryMetadataPtr()->getBucketNumberFromClusterByKey();
                for (const auto & partition_id : partition_ids)
                {
                    for (auto bucket_index = 0; bucket_index < max_bucket_num; bucket_index++)
                        ret.emplace_back(fmt::format(DATA_CHECKER_QUERY, group_by_str, storage_id.getFullTableName(), get_check_predicate_str(partition_id, bucket_index), group_by_str, get_unique_key_str()));
                }
            }
            else    /// table + bucket
            {
                auto max_bucket_num = cnch_table.getInMemoryMetadataPtr()->getBucketNumberFromClusterByKey();
                for (auto bucket_index = 0; bucket_index < max_bucket_num; bucket_index++)
                    ret.emplace_back(fmt::format(DATA_CHECKER_QUERY, group_by_str, storage_id.getFullTableName(), get_check_predicate_str(FAKE_DEDUP_GRAN_PARTITION_ID, bucket_index), group_by_str, get_unique_key_str()));
            }
        }
        else
        {
            if (settings->partition_level_unique_keys) /// partition + non bucket
            {
                Names partition_ids = getContext()->getCnchCatalog()->getPartitionIDs(storage, nullptr);
                for (const auto & partition_id : partition_ids)
                    ret.emplace_back(fmt::format(DATA_CHECKER_QUERY, group_by_str, storage_id.getFullTableName(), get_check_predicate_str(partition_id, -1), group_by_str, get_unique_key_str()));
            }
            else /// table + non bucket
                ret.emplace_back(fmt::format(DATA_CHECKER_QUERY, group_by_str, storage_id.getFullTableName(), get_check_predicate_str(FAKE_DEDUP_GRAN_PARTITION_ID, -1), group_by_str, get_unique_key_str()));
        }
    }
    else
    {
        ret.emplace_back(fmt::format(DATA_CHECKER_QUERY, group_by_str, storage_id.getFullTableName(), get_check_predicate_str(FAKE_DEDUP_GRAN_PARTITION_ID, -1), group_by_str, get_unique_key_str()));
    }
    return ret;
}

String DedupDataChecker::getRepairGranQuery(const UInt64 & event_time_filter)
{
    auto ret = fmt::format(GET_REPAIR_GRAN_QUERY, storage_id.getDatabaseName(), storage_id.getTableName(), ErrorCodes::UNIQUE_TABLE_DUPLICATE_KEY_FOUND, event_time_filter);
    LOG_TRACE(log, "Try to get repair grans by query: {}", ret);
    return ret;
}

void DedupDataChecker::processCheckerResult(StorageCnchMergeTree & cnch_table, Block & input_block)
{
    auto settings = cnch_table.getSettings();
    NameSet data_checker_info_set;
    auto get_data_checker_task_info = [&](const String & partition_id, Int64 bucket_number) -> String
    {
        WriteBufferFromOwnString os;
        os << (!settings->partition_level_unique_keys ? "table" : "partition " + partition_id) << (settings->enable_bucket_level_unique_keys ? ("(bucket " + toString(bucket_number) + ")") : "");
        return os.str();
    };

    auto report_duplicate = [&](const String & partition_id, Int64 bucket_number) -> void
    {
        if (auto unique_table_log = getContext()->getCloudUniqueTableLog())
        {
            auto task_info = get_data_checker_task_info(partition_id, bucket_number);
            if (!data_checker_info_set.contains(task_info))
            {
                auto current_log = UniqueTable::createUniqueTableLog(UniqueTableLogElement::ERROR, cnch_table.getCnchStorageID());
                current_log.metric = ErrorCodes::UNIQUE_TABLE_DUPLICATE_KEY_FOUND;
                current_log.event_msg = "Data checker task " + task_info + " found duplication";
                TaskInfo log_entry_task_info;
                log_entry_task_info.task_type = TaskInfo::DATA_CHECKER_TASK;
                log_entry_task_info.dedup_gran.partition_id = !settings->partition_level_unique_keys ? ALL_DEDUP_GRAN_PARTITION_ID : partition_id;
                log_entry_task_info.dedup_gran.bucket_number = settings->enable_bucket_level_unique_keys ? bucket_number : -1;
                current_log.task_info = std::move(log_entry_task_info);
                unique_table_log->add(current_log);
                data_checker_info_set.emplace(task_info);
            }
        }
    };
    /// It is recommended to use this setting in the future without checkBucketParts()
    if (settings->enable_bucket_level_unique_keys)
    {
        if (settings->partition_level_unique_keys)
        {
            const auto * col_partition = checkAndGetColumn<ColumnString>(*input_block.getByName("_partition_id").column);
            const auto * col_bucket = checkAndGetColumn<ColumnInt64>(*input_block.getByName("_bucket_number").column);

            if (!col_partition || col_partition->empty())
            {
                LOG_TRACE(log, "Currently no duplicate data is detected for current table");
                return;
            }
            for (size_t i = 0; i < col_partition->size(); ++i)
                report_duplicate(col_partition->getDataAt(i).toString(), col_bucket->getInt(i));
        }
        else
        {
            const auto * col_bucket = checkAndGetColumn<ColumnInt64>(*input_block.getByName("_bucket_number").column);

            if (!col_bucket || col_bucket->empty())
            {
                LOG_TRACE(log, "Currently no duplicate data is detected for current table");
                return;
            }
            for (size_t i = 0; i < col_bucket->size(); ++i)
                report_duplicate("", col_bucket->getInt(i));
        }
    }
    else
    {
        if (settings->partition_level_unique_keys)
        {
            const auto * col_partition = checkAndGetColumn<ColumnString>(*input_block.getByName("_partition_id").column);

            if (!col_partition || col_partition->empty())
            {
                LOG_TRACE(log, "Currently no duplicate data is detected for current table");
                return;
            }
            for (size_t i = 0; i < col_partition->size(); ++i)
                report_duplicate(col_partition->getDataAt(i).toString(), -1);
        }
        else
        {
            const auto * cnt = checkAndGetColumn<ColumnUInt64>(*input_block.getByName("cnt").column);

            if (!cnt || cnt->empty())
            {
                LOG_TRACE(log, "Currently no duplicate data is detected for current table");
                return;
            }
            for (size_t i = 0; i < cnt->size(); ++i)
                report_duplicate("", -1);
        }
    }
}

BlockIO DedupDataChecker::tryToExecuteQuery(const String & query_to_execute)
{
    try
    {
        /// We need to create by global context as select queries use txn id to filter parts and bitmaps
        auto query_context = Context::createCopy(getContext()->getGlobalContext());

        return executeQuery(query_to_execute, query_context, true);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        throw;
    }
}

DedupDataChecker::DedupDataChecker(ContextPtr context_, String logger_name_, const MergeTreeMetaBase & storage_)
    : WithContext(context_)
    , log_name(logger_name_)
    , log(getLogger(log_name))
    , storage_id(storage_.getCnchStorageID())
{
    check_interval = storage_.getSettings()->check_duplicate_key_interval.totalMilliseconds();
    data_checker_task = getContext()->getUniqueTableSchedulePool().createTask(log_name, [this] { run(); });
    data_checker_task->deactivate();
}

DedupDataChecker::~DedupDataChecker()
{
    try
    {
        stop();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void DedupDataChecker::run()
{
    if (!isActive())
        return;

    try
    {
        iterate();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    if (isActive())
        data_checker_task->scheduleAfter(check_interval);
}

void DedupDataChecker::iterate()
{
    auto catalog = getContext()->getCnchCatalog();
    TxnTimestamp ts = getContext()->getTimestamp();
    auto table = catalog->tryGetTableByUUID(*getContext(), toString(storage_id.uuid), ts);
    if (!table)
        throw Exception("Table " + storage_id.getNameForLogs() + " has been dropped", ErrorCodes::ABORTED);
    auto & cnch_table = ICnchBGThread::checkAndGetCnchTable(table);

    auto check_queries = getCheckerQuery(table, cnch_table);
    for (const auto & check_query : check_queries)
    {
        auto block_io = tryToExecuteQuery(check_query);
        auto input_stream = block_io.getInputStream();

        while (true)
        {
            auto res = input_stream->read();
            if (!res)
                break;

            processCheckerResult(cnch_table, res);
        }
    }
}

DedupGranTimeMap DedupDataChecker::getNeedRepairGrans(StorageCnchMergeTree & cnch_table)
{
    TxnTimestamp ts = getContext()->getTimestamp();
    UInt64 current_repair_time = ts.toSecond();

    /// Double check on the server and worker side
    /// The server side checks the delivery time, and the worker side checks the execution completion time
    UInt64 duplicate_repair_interval_local = cnch_table.getSettings()->duplicate_repair_interval.totalSeconds();
    if (last_repair_time != 0 && current_repair_time - last_repair_time < duplicate_repair_interval_local)
        return {};

    /// Considering the delay in flushing cnch_unique_table_log, the server filter uses twice the duplicate_repair_interval
    UInt64 event_time_filter = last_repair_time - duplicate_repair_interval_local;
    if (last_repair_time == 0)
        event_time_filter = current_repair_time - 2 * duplicate_repair_interval_local;

    auto block_io = tryToExecuteQuery(getRepairGranQuery(event_time_filter));
    auto input_block = block_io.getInputStream();

    DedupGranTimeMap dedup_task_infos, other_task_infos;
    while (true)
    {
        auto res = input_block->read();
        if (!res)
            break;

        const auto * col_event_time = checkAndGetColumn<ColumnUInt32>(*res.getByName("event_time").column);
        const auto * col_task_type = checkAndGetColumn<ColumnString>(*res.getByName("task_type").column);
        const auto * col_partition_id = checkAndGetColumn<ColumnString>(*res.getByName("partition_id").column);
        const auto * col_bucket_num = checkAndGetColumn<ColumnString>(*res.getByName("bucket_num").column);

        if (!col_task_type || col_task_type->empty())
        {
            LOG_TRACE(log, "Currently no duplicate data is detected for current table");
            return {};
        }
        for (size_t i = 0; i < col_task_type->size(); ++i)
        {
            DedupGran tmp_repair_gran;
            UInt64 event_time_row = col_event_time->get64(i);
            auto task_type = col_task_type->getDataAt(i).toString();
            tmp_repair_gran.partition_id = col_partition_id->getDataAt(i).toString();
            tmp_repair_gran.bucket_number = std::stoi(col_bucket_num->getDataAt(i).toString());

            LOG_TRACE(log, "Duplication task type: {}, gran to be repaired: {}, event time: {}", task_type, tmp_repair_gran.getDedupGranDebugInfo(), event_time_row);

            if (task_type == "DEDUP_TASK")
            {
                auto max_time = dedup_task_infos.count(tmp_repair_gran) ? std::max(event_time_row, dedup_task_infos[tmp_repair_gran]) : event_time_row;
                dedup_task_infos[tmp_repair_gran] = max_time;
            }
            else
            {
                auto max_time = other_task_infos.count(tmp_repair_gran) ? std::max(event_time_row, other_task_infos[tmp_repair_gran]) : event_time_row;
                other_task_infos[tmp_repair_gran] = max_time;
            }
        }
    }

    last_repair_time = current_repair_time;
    /// Prioritize using the relevant information of the dedup task, because there may be meta checks and possible bucket locks usage during deduplication.
    return !dedup_task_infos.empty() ? dedup_task_infos : other_task_infos;
}

void DedupDataChecker::waitForNoDuplication(ContextPtr local_context)
{
    UInt64 wait_timeout_seconds = local_context->getSettingsRef().receive_timeout.value.totalSeconds();
    auto catalog = getContext()->getCnchCatalog();
    TxnTimestamp ts = getContext()->getTimestamp();
    auto table = catalog->tryGetTableByUUID(*getContext(), toString(storage_id.uuid), ts);
    if (!table)
        throw Exception("Table " + storage_id.getNameForLogs() + " has been dropped", ErrorCodes::ABORTED);
    auto & cnch_table = ICnchBGThread::checkAndGetCnchTable(table);

    Stopwatch timer;
    do
    {
        String check_query;
        auto check_queries = getCheckerQuery(table, cnch_table, /*force_check_at_once*/true);
        if (!check_queries.empty())
            check_query = check_queries[0];

        auto block_io = tryToExecuteQuery(check_query);
        auto input_stream = block_io.getInputStream();

        bool find_duplication = false;
        while (true)
        {
            auto res = input_stream->read();
            if (!res)
                break;

            find_duplication = true;
        }

        if (!find_duplication)
            return;

        /// XXX: Since it is only used for testing, we do not make this sleep time an independent setting
        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    } while (timer.elapsedSeconds() < wait_timeout_seconds);
    LOG_WARNING(
        log,
        "There are still duplication not repaired after " + toString(wait_timeout_seconds) + "s.");
}
}
