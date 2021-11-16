#pragma once

/**
 * Consul is maintained by Data-inf guys.
 * They use cpputil/log to output logs which ClickHouse doesn't need.
 * Create a fake log.h to make them happy.
 */

#define LOGF_TRACE(fmt, ...)
#define LOGF_DEBUG(fmt, ...)
#define LOGF_INFO( fmt, ...)
#define LOGF_WARN( fmt, ...)
#define LOGF_ERROR(fmt, ...)
#define LOGF_FATAL(fmt, ...)
