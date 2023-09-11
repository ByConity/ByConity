#pragma once

#include <Core/Types.h>

namespace DB::IO::Scheduler {

enum class LogLevel {
    NONE = 0,
    DEBUG = 1,
    TRACE = 2,
};

LogLevel str2LogLevel(const String& str);

}
