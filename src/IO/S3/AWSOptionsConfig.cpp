#include "AWSOptionsConfig.h"
#include <fmt/core.h>
#include <common/logger_useful.h>

Aws::Utils::Logging::LogLevel DB::S3::AWSOptionsConfig::convertStringToLogLevel(std::string log_level_str) {
    if (log_level_str.compare("Off") == 0)
        return Aws::Utils::Logging::LogLevel::Off;
    else if (log_level_str.compare("Fatal") == 0)
        return Aws::Utils::Logging::LogLevel::Fatal;
    else if (log_level_str.compare("Error") == 0)
        return Aws::Utils::Logging::LogLevel::Error;
    else if (log_level_str.compare("Warn") == 0)
        return Aws::Utils::Logging::LogLevel::Warn;
    else if (log_level_str.compare("Info") == 0)
        return Aws::Utils::Logging::LogLevel::Info;
    else if (log_level_str.compare("Debug") == 0)
        return Aws::Utils::Logging::LogLevel::Debug;
    else if (log_level_str.compare("Trace") == 0)
        return Aws::Utils::Logging::LogLevel::Trace;
    else {
        LOG_WARNING(getLogger("AWSOptionsConfig"), fmt::format("Illegal aws log level {}, please check.", log_level_str));
        return Aws::Utils::Logging::LogLevel::Off;
    }
}

DB::S3::AWSOptionsConfig & DB::S3::AWSOptionsConfig::instance()
{
    static DB::S3::AWSOptionsConfig ret;
    return ret;
}
