#pragma once

#include <Common/Logger.h>
#include "DataTypes/IDataType.h"
#include "Interpreters/Context_fwd.h"
#include "Parsers/ASTCreateQuery.h"
#include "Storages/ColumnsDescription.h"
#include "Storages/StorageInMemoryMetadata.h"

namespace Apache::Hadoop::Hive
{
class Table;
}

namespace DB
{
class HiveSchemaConverter : WithContext
{
public:
    static DataTypePtr hiveTypeToCHType(const String & hive_type, bool make_columns_nullable);

    explicit HiveSchemaConverter(ContextPtr context_, std::shared_ptr<Apache::Hadoop::Hive::Table> hive_table_);

    /// schema inference
    void convert(StorageInMemoryMetadata & metadata) const;

    /// check schema
    void check(const StorageInMemoryMetadata & metadata) const;

    ASTCreateQuery createQueryAST(const std::string & catalog_name) const;

private:
    std::shared_ptr<Apache::Hadoop::Hive::Table> hive_table;
    LoggerPtr log {getLogger("HiveSchemaConverter")};
};


struct CloudTableBuilder
{
    CloudTableBuilder();
    CloudTableBuilder & setMetadata(const StorageMetadataPtr & metadata);
    CloudTableBuilder & setCloudEngine(const String & cloudEngineName);
    CloudTableBuilder & setStorageID(const StorageID & storage_id);

    String build() const;
    const String & cloudTableName() const;

private:
    std::shared_ptr<ASTCreateQuery> create_query;
};

}
