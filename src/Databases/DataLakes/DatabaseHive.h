#pragma once

#include <Common/Logger.h>
#include <Common/config.h>

#if USE_HIVE

#include <Core/MultiEnum.h>
#include <Core/NamesAndTypes.h>
#include <Databases/DataLakes/DatabaseLakeBase.h>
#include <Databases/DatabasesCommon.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Hive/CnchHiveSettings.h>
#include <Storages/Hive/Metastore/HiveMetastore.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <Poco/Logger.h>
#include <Common/ThreadPool.h>

namespace DB
{

class Context;


class DatabaseHive final : public DatabaseLakeBase
{
public:
    ~DatabaseHive() override = default;

    DatabaseHive(ContextPtr context, const String & database_name, const String & metadata_path, const ASTStorage * database_engine_define);

    String getEngineName() const override { return "Hive"; }

    bool empty() const override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name) override;

    bool isTableExist(const String & name, ContextPtr context) const override;

    time_t getObjectMetadataModificationTime(const String & name) const override;

protected:
    ASTPtr getCreateTableQueryImpl(const String & name, ContextPtr context, bool throw_on_error) const override;

private:
    String hive_metastore_url;
    String database_name_in_hive;
    HiveMetastoreClientPtr metastore_client;

    LoggerPtr log{getLogger("DatabaseHive")};
};

}
#endif
