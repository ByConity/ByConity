/**
 * @brief MetricsData exports the global data to save system status (for DM)
 * so that it can be used for system status monitoring.
 * Server can write to these data.
 */
#pragma once

#include <atomic>

/// Record the seconds from the epoch for the oldest transaction we observed.
extern std::atomic<int64_t> oldest_transaction;
