#pragma once

#include <Common/Logger.h>
#include <Common/config.h>
#if USE_HIVE and USE_JAVA_EXTENSIONS

#include <Interpreters/StorageID.h>
#include <MergeTreeCommon/CnchStorageCommon.h>
#include <Storages/DataLakes/PaimonCommon.h>
#include <Storages/DataLakes/StorageCnchLakeBase.h>
#include <Storages/Hive/CnchHiveSettings.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/Exception.h>

namespace DB
{
class StorageCnchPaimon : public shared_ptr_helper<StorageCnchPaimon>, public StorageCnchLakeBase
{
    friend struct shared_ptr_helper<StorageCnchPaimon>;

public:
    std::string getName() const override { return "PaimonCnch"; }
    bool supportsPrewhere() const override { return false; }

    std::optional<TableStatistics> getTableStats(const Strings & /*columns*/, ContextPtr /*local_context*/) override
    {
        return std::nullopt;
    }

    StorageCnchPaimon(
        const ContextPtr & context_,
        const StorageID & table_id_,
        const String & database_,
        const String & table_,
        CnchHiveSettingsPtr storage_settings_,
        StorageInMemoryMetadata metadata_,
        PaimonCatalogClientPtr catalog_client_);

protected:
    PrepareContextResult prepareReadContext(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr & local_context,
        unsigned num_streams) override;

private:
    LoggerPtr log{getLogger("StoragePaimonCluster")};

    PaimonCatalogClientPtr catalog_client;
};
}

#endif
