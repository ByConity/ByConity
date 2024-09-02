#include <Storages/MaterializedView/RefreshSchedule.h>
#include <Common/thread_local_rng.h>
#include <string>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <regex>
#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

RefreshSchedule::RefreshSchedule(const ASTRefreshStrategy * strategy)
{
    if (!strategy)
        kind = RefreshScheduleKind::SYNC;
    else 
    {
        if (strategy->schedule_kind == RefreshScheduleKind::UNKNOWN)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Materialized view async refersh strategy is not allowed to set to UNKNOWN");

        kind = strategy->schedule_kind;
        if (strategy->start)
        {
            String time_start_string = strategy->start->value.safeGet<String>();
            start_time_string = time_start_string;
            std::smatch match;
            std::regex pattern(R"((\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}))");
            if (!std::regex_match(time_start_string, match, pattern))
            {
                throw Exception(
                    "Materialized view async refersh start time only accept datetime format YYYY-mm-dd HH:MM:SS, can not parse "
                        + time_start_string,
                    ErrorCodes::BAD_ARGUMENTS);
            }
            std::tm time_date = {};
            std::istringstream ss(time_start_string);
            ss >> std::get_time(&time_date, "%Y-%m-%d %H:%M:%S");
            start_time = std::chrono::system_clock::from_time_t(mktime(&time_date));
        }

        if (strategy->time_interval)
            time_interval = strategy->time_interval->interval;
    }
}

bool RefreshSchedule::operator!=(const RefreshSchedule & rhs) const
{
    return std::tie(kind, time_interval, start_time) != std::tie(rhs.kind, rhs.time_interval, rhs.start_time);
}

static std::chrono::sys_seconds advanceEvery(std::chrono::system_clock::time_point previous, CalendarTimeInterval period)
{
    auto period_start = period.floor(previous);
    auto t = period.advance(period_start);
    return t;
}

UInt64 RefreshSchedule::prescribeNextElaps() const
{
    auto now = std::chrono::system_clock::now();
    auto next_time = start_time;
    while (next_time <= now)
    {
        next_time = advanceEvery(next_time, time_interval);
    }
    auto now_time_seconds = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch());
    auto next_time_seconds = std::chrono::duration_cast<std::chrono::seconds>(next_time.time_since_epoch());

    return next_time_seconds.count() - now_time_seconds.count();
}

String RefreshSchedule::getStartTime() const
{
    return start_time_string;
}

std::chrono::sys_seconds RefreshSchedule::prescribeNext(std::chrono::system_clock::time_point last_prescribed, std::chrono::system_clock::time_point now) const
{
    auto res = advanceEvery(last_prescribed, time_interval);
    if (res < now)
        res = advanceEvery(now, time_interval); 
    return res;
}

}
