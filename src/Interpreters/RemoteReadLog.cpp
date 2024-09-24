#include <Interpreters/RemoteReadLog.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/time.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

NamesAndTypesList RemoteReadLogElement::getNamesAndTypes()
{
    return {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"request_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
        {"context", std::make_shared<DataTypeString>()},
        {"query_id", std::make_shared<DataTypeString>()},
        {"txn_id", std::make_shared<DataTypeUInt64>()},
        {"thread_name", std::make_shared<DataTypeString>()},
        {"thread_id", std::make_shared<DataTypeUInt64>()},
        {"path", std::make_shared<DataTypeString>()},
        {"offset", std::make_shared<DataTypeUInt64>()},
        {"size", std::make_shared<DataTypeInt64>()},
        {"duration_us", std::make_shared<DataTypeUInt64>()},
    };
}

void RemoteReadLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(DateLUT::sessionInstance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(request_time_microseconds);
    columns[i++]->insert(context);
    columns[i++]->insert(query_id);
    columns[i++]->insert(txn_id);
    columns[i++]->insert(thread_name);
    columns[i++]->insert(thread_id);
    columns[i++]->insert(path);
    columns[i++]->insert(offset);
    columns[i++]->insert(size);
    columns[i++]->insert(duration_us);
}

void RemoteReadLog::insert(std::chrono::time_point<std::chrono::system_clock> request_time, const String & path, UInt64 offset, Int64 size, UInt64 duration_us, const String & read_context)
{
    RemoteReadLogElement elem;
    elem.event_time = time(nullptr);
    elem.request_time_microseconds = time_in_microseconds(request_time);
    elem.context = read_context;
    elem.query_id = CurrentThread::getQueryId().toString();
    elem.txn_id = CurrentThread::getTransactionId();
    elem.thread_name = getThreadName();
    elem.thread_id = CurrentThread::getThreadId();
    elem.path = path;
    elem.offset = offset;
    elem.size = size;
    elem.duration_us = duration_us;

    this->add(elem);
}

}

