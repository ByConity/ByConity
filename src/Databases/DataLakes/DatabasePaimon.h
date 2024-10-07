#pragma once

#include <Common/Logger.h>
#include <Common/config.h>
#if USE_HIVE and USE_JAVA_EXTENSIONS

#include <Databases/DataLakes/DatabaseLakeBase.h>
#include <Storages/DataLakes/PaimonCommon.h>

namespace DB
{

class DatabasePaimon final : public DatabaseLakeBase

{
public:
    ~DatabasePaimon() override = default;

    DatabasePaimon(
        ContextPtr context, const String & database_name, const String & metadata_path, const ASTStorage * database_engine_define);

    String getEngineName() const override { return "Paimon"; }

    bool empty() const override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name) override;

    bool isTableExist(const String & table_name, ContextPtr context) const override;

    time_t getObjectMetadataModificationTime(const String & table_name) const override;

protected:
    ASTPtr getCreateTableQueryImpl(const String & table_name, ContextPtr context, bool throw_on_error) const override;

private:
    String database_name_in_paimon;
    PaimonCatalogClientPtr catalog_client;


    LoggerPtr log{getLogger("DatabasePaimon")};
};

}

#endif
