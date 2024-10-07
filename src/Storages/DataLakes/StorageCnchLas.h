#pragma once
#include <Common/Logger.h>
#include "Common/config.h"
#if USE_HIVE and USE_JAVA_EXTENSIONS

#include "Storages/Hive/StorageCnchHive.h"

namespace DB
{
class JNIHiveMetastoreClient;

class StorageCnchLas : public StorageCnchHive
{
public:
    StorageCnchLas(
        const StorageID & table_id_,
        const String & hive_metastore_url_,
        const String & hive_db_name_,
        const String & hive_table_name_,
        StorageInMemoryMetadata metadata_,
        ContextPtr context_,
        IMetaClientPtr client,
        std::shared_ptr<CnchHiveSettings> settings_);

    PrepareContextResult prepareReadContext(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr & local_context,
        unsigned num_streams) override;

    std::optional<TableStatistics> getTableStats(const Strings & columns, ContextPtr local_context) override;

    void serializeHiveFiles(Protos::ProtoHiveFiles & proto, const HiveFiles & hive_files) override;

private:
    Strings getHiveColumnNames() const;
    Strings getHiveColumnTypes() const;

    JNIHiveMetastoreClient * jni_meta_client = nullptr;
    LoggerPtr log {getLogger("CnchLas")};
};

}
#endif
