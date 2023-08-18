#pragma once

#include "DataTypes/IDataType.h"
#include "Interpreters/Context_fwd.h"
<<<<<<< HEAD
=======
#include "Parsers/ASTCreateQuery.h"
>>>>>>> e22f20f6c2 (Merge branch 'pick-hive' into 'cnch-ce-merge')
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
    StorageInMemoryMetadata convert() const;

    /// check schema
<<<<<<< HEAD
    void check(const ColumnsDescription & columns) const;
=======
    void check(const StorageInMemoryMetadata & metadata) const;
>>>>>>> e22f20f6c2 (Merge branch 'pick-hive' into 'cnch-ce-merge')

private:
    std::shared_ptr<Apache::Hadoop::Hive::Table> hive_table;
    Poco::Logger * log {&Poco::Logger::get("HiveSchemaConverter")};
};

<<<<<<< HEAD
=======
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

>>>>>>> e22f20f6c2 (Merge branch 'pick-hive' into 'cnch-ce-merge')
}
