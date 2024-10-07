#pragma once
#include <Common/Logger.h>
#include <string>
#include <Coordination/KeeperStorage.h>
#include <common/logger_useful.h>

namespace DB
{

void deserializeKeeperStorageFromSnapshot(KeeperStorage & storage, const std::string & snapshot_path, LoggerPtr log);

void deserializeKeeperStorageFromSnapshotsDir(KeeperStorage & storage, const std::string & path, LoggerPtr log);

void deserializeLogAndApplyToStorage(KeeperStorage & storage, const std::string & log_path, LoggerPtr log);

void deserializeLogsAndApplyToStorage(KeeperStorage & storage, const std::string & path, LoggerPtr log);

}
