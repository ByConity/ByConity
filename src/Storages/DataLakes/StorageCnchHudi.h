#pragma once
#include "Common/config.h"
#if USE_HIVE

#include "Storages/Hive/DirectoryLister.h"
#include "Storages/Hive/StorageCnchHive.h"

namespace DB
{
class HudiMorDirectoryLister;
class StorageCnchHudi : public shared_ptr_helper<StorageCnchHudi>, public StorageCnchHive
{
public:
    friend class HudiMorDirectoryLister;
    std::string getName() const override { return "CnchHudi"; }

    StorageCnchHudi(
        const StorageID & table_id_,
        const String & hive_metastore_url_,
        const String & hive_db_name_,
        const String & hive_table_name_,
        StorageInMemoryMetadata metadata_,
        ContextPtr context_,
        std::shared_ptr<CnchHiveSettings> settings_,
        IMetaClientPtr client_from_catalog = nullptr);

    virtual std::shared_ptr<IDirectoryLister> getDirectoryLister(ContextPtr local_context) override;

    Strings getHiveColumnTypes() const;
    Strings getHiveColumnNames() const;
};

}
#endif
