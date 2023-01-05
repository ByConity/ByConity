#pragma once

#include <Storages/StorageCnchMergeTree.h>
#include <Poco/Logger.h>

namespace DB::DaemonManager
{
    inline bool isCnchTable(const StoragePtr & storage)
    {
        auto * cnch_table = dynamic_cast<StorageCnchMergeTree *>(storage.get());
        return cnch_table != nullptr;
    }

    void printConfig(std::map<std::string, unsigned int> & config, Poco::Logger * log);

    std::map<std::string, unsigned int> updateConfig(
        std::map<std::string, unsigned int> && default_config,
        const Poco::Util::AbstractConfiguration & app_config);
}
