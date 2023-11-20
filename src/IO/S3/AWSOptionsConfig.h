#pragma once

#include <Common/getNumberOfPhysicalCPUCores.h>
#include <aws/core/utils/logging/LogLevel.h>

namespace DB::S3
{

class AWSOptionsConfig {
public:
    Aws::Utils::Logging::LogLevel convertStringToLogLevel(std::string log_level);

    bool use_crt_http_client = false;
    int aws_event_loop_size = getNumberOfPhysicalCPUCores();
    int max_hosts = 32;
    int max_TTL = 300;
    std::string log_level = "Off";

    static AWSOptionsConfig & instance();
};
}
