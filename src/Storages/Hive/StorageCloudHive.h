#pragma once

#include <Common/Logger.h>
#include "Common/config.h"
#if USE_HIVE

#include <Storages/IStorage.h>
#include "Storages/Hive/HiveFile/IHiveFile.h"
#include "Storages/StorageInMemoryMetadata.h"
#include <common/shared_ptr_helper.h>
#include <Processors/IntermediateResult/CacheManager.h>

namespace DB
{
struct CnchHiveSettings;

class StorageCloudHive : public shared_ptr_helper<StorageCloudHive>, public IStorage, WithContext
{
public:
    StorageCloudHive(
        StorageID table_id_,
        const StorageInMemoryMetadata & metadata,
        ContextPtr context_,
        const std::shared_ptr<CnchHiveSettings> & settings_);

    ~StorageCloudHive() override = default;

    std::string getName() const override { return "CloudHive"; }

    HiveFiles filterHiveFilesByIntermediateResultCache(SelectQueryInfo & query_info, ContextPtr query_context, HiveFiles & hive_files);

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    NamesAndTypesList getVirtuals() const override;

    void loadHiveFiles(const HiveFiles & files);
    HiveFiles getHiveFiles() const { return files; }
    std::shared_ptr<CnchHiveSettings> getSettings() const { return storage_settings; }
    bool supportIntermedicateResultCache() const override { return true; }
    bool supportsPrewhere() const override { return true; }
private:

    HiveFiles files;
    std::shared_ptr<CnchHiveSettings> storage_settings;
    LoggerPtr log {getLogger("CloudHive")};
    CacheHolderPtr cache_holder;
};

}

#endif
