#pragma once

#include <Common/CalendarTimeInterval.h>
#include <Parsers/ASTRefreshStrategy.h>
#include <chrono>

namespace DB
{

struct RefreshSchedule
{
    RefreshScheduleKind kind {RefreshScheduleKind::SYNC};
    CalendarTimeInterval time_interval;
    std::chrono::system_clock::time_point start_time;
    String start_time_string;

    explicit RefreshSchedule(const ASTRefreshStrategy * strategy);
    bool operator!=(const RefreshSchedule & rhs) const;

    bool sync() const {return kind == RefreshScheduleKind::SYNC;}
    bool async() const {return kind == RefreshScheduleKind::ASYNC;}
    bool manual() const {return kind == RefreshScheduleKind::MANUAL;}

    String getStartTime() const;

    UInt64 prescribeNextElaps() const;

    std::chrono::sys_seconds prescribeNext(
        std::chrono::system_clock::time_point last_prescribed, std::chrono::system_clock::time_point now) const;
};

}
