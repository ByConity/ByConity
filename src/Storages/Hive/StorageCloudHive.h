#pragma once

#include "Common/config.h"
#if USE_HIVE

#include <Storages/IStorage.h>
#include "Storages/Hive/HiveFile/IHiveFile.h"
#include "Storages/StorageInMemoryMetadata.h"
#include <common/shared_ptr_helper.h>

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

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    NamesAndTypesList getVirtuals() const override;

    void loadHiveFiles(const HiveFiles & files);
    HiveFiles getHiveFiles() const { return files; }

private:
    struct MinMaxDescription
    {
        NamesAndTypesList columns;
        ExpressionActionsPtr expression;
    };
    static MinMaxDescription getMinMaxIndex(const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context);

    void selectFiles(
        ContextPtr local_context,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        HiveFiles & hive_files,
        unsigned num_streams);

    HiveFiles files;
    std::shared_ptr<CnchHiveSettings> storage_settings;
    Poco::Logger * log {&Poco::Logger::get("CloudHive")};
};

}

#endif
