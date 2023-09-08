#include <IO/Scheduler/Common.h>
#include <Common/Exception.h>

namespace DB {

namespace ErrorCodes {
    extern const int BAD_ARGUMENTS;
}

namespace IO::Scheduler {

LogLevel str2LogLevel(const String& str) {
    if (str == "none") {
        return LogLevel::NONE;
    } else if (str == "debug") {
        return LogLevel::DEBUG;
    } else if (str == "trace") {
        return LogLevel::TRACE;
    } else {
        throw Exception("Can't parse log level from str " + str,
            ErrorCodes::BAD_ARGUMENTS);
    }
}

}

}
