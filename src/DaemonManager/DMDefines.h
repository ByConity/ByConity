#pragma once

namespace DB::DaemonManager
{
/// check liveness for every N iteration ~ each iteration 10 sec => total 4 minute
constexpr size_t LIVENESS_CHECK_INTERVAL = 24;
constexpr uint32_t BYTEKV_BATCH_SCAN = 100;
constexpr int32_t SLOW_EXECUTION_THRESHOLD_MS = 200;

using UUIDs = std::unordered_set<UUID>;

}
