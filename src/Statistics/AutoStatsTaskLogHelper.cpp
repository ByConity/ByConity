#include <string>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/now64.h>
#include <Protos/auto_statistics.pb.h>
#include <Statistics/AutoStatsTaskLogHelper.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/StatsTableIdentifier.h>
#include <Statistics/SubqueryHelper.h>

namespace DB::Statistics::AutoStats
{

static inline UUID getUUID(const ColumnPtr & col_base, UInt64 row)
{
    auto col = static_cast<const ColumnUUID *>(col_base.get());
    return col->getElement(row);
}

static inline std::vector<String> getVectorString(const ColumnPtr & col_base, UInt64 row)
{
    auto col = static_cast<const ColumnArray *>(col_base.get());
    auto data_col = static_cast<const ColumnString *>(col->getDataPtr().get());
    auto offsets_col = static_cast<const ColumnArray::ColumnOffsets *>(col->getOffsetsPtr().get());

    auto offset = offsets_col->getElement(row);
    auto prev_offset = row == 0 ? 0 : offsets_col->getElement(row - 1);
    auto size = offset - prev_offset;

    std::vector<String> result;
    result.reserve(size);
    for (auto i = prev_offset; i < offset; ++i)
        result.emplace_back(data_col->getDataAt(i));

    return result;
}

std::vector<TaskInfoLog> batchReadTaskLog(ContextPtr context, DateTime64 min_event_time)
{
    std::vector<TaskInfoLog> result;

    auto event_time_str = serializeToText(min_event_time);

    auto event_date = convertToDate(min_event_time);
    auto event_date_str = serializeToText(event_date);

    String table_sql = "system.auto_stats_task_log";
    auto select_sql = fmt::format(
        FMT_STRING(
            " select event_time, task_uuid, task_type, table_uuid, stats_row_count, udi, priority, retry, status, settings_json, columns, database, table"
            " from ("
            "    select"
            "    *,"
            "    row_number() over ("
            "        partition by task_uuid"
            "        order by event_time desc"
            "        ) as _tmp__rn"
            "    from {}"
            "    where event_date >= toDate('{}') and event_time >= toDateTime64('{}', 3)"
            " ) "
            " where _tmp__rn=1 and status not in ('Success', 'Failed', 'Cancelled')"),
        table_sql,
        event_date_str,
        event_time_str);

    auto proc_block = [&result](Block & block) {
        auto index_base = result.size();
        result.resize(result.size() + block.rows());
        for (size_t i = 0; i < block.columns(); ++i)
        {
            auto & column = block.safeGetByPosition(i).column;
            for (size_t row = 0; row < block.rows(); ++row)
            {
                auto & task = result[index_base + row];
                switch (i)
                {
                    case 0:
                        task.event_time = column->get64(row);
                        break;
                    case 1:
                        task.task_uuid = getUUID(column, row);
                        break;
                    case 2:
                        task.task_type = static_cast<TaskType>(column->get64(row));
                        break;
                    case 3:
                        task.table.getMutableStorageID().uuid = getUUID(column, row);
                        break;
                    case 4:
                        task.stats_row_count = column->get64(row);
                        break;
                    case 5:
                        task.udi_count = column->get64(row);
                        break;
                    case 6:
                        task.priority = column->getFloat64(row);
                        break;
                    case 7:
                        task.retry_times = column->get64(row);
                        break;
                    case 8:
                        task.status = static_cast<Status>(column->get64(row));
                        break;
                    case 9:
                        task.settings_json = column->getDataAt(row).toString();
                        break;
                    case 10:
                        task.columns_name = getVectorString(column, row);
                        break;
                    case 11:
                        task.table.getMutableStorageID().database_name = column->getDataAt(row).toString();
                        break;
                    case 12:
                        task.table.getMutableStorageID().table_name = column->getDataAt(row).toString();
                        break;
                }
            }
        }
    };
    auto query_context = SubqueryHelper::createQueryContext(context);
    executeSubQuery(select_sql, query_context, proc_block, true);

    LOG_DEBUG(
        getLogger("AutoStatsTaskLogHelper"),
        fmt::format(FMT_STRING("batchReadTaskLog: read {} useful entries with min_event_time='{}'"), result.size(), event_time_str));

    return result;
}

void writeTaskLog(ContextPtr context, const TaskInfo & task, const String & extra_message)
{
    writeTaskLog(context, task.getCore(), extra_message);
}


void writeTaskLog(ContextPtr context, const TaskInfoCore & core, const String & extra_message)
{
    auto task_log = context->getAutoStatsTaskLog();
    if (!task_log)
        return;

    auto catalog = createCatalogAdaptor(context);

    AutoStatsTaskLogElement element;

    auto table = core.table;
    element.database_name = table.getDatabaseName();
    element.table_name = table.getTableName();
    element.table_uuid = core.table.getStorageID().uuid; // not UniqueKey
    element.columns_name = core.columns_name;
    element.retry = core.retry_times;
    element.task_uuid = core.task_uuid;
    element.event_time = nowSubsecondDt64(DataTypeDateTime64::default_scale);
    element.udi = core.udi_count;
    element.stats_row_count = core.stats_row_count;
    element.priority = core.priority;
    element.status = core.status;
    element.task_type = core.task_type;
    element.settings_json = core.settings_json;

    element.message = extra_message; // usually exception
    task_log->add(element);
}
}
