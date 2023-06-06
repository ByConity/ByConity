#pragma once

#include "DataTypes/IDataType.h"
#include "Interpreters/Context_fwd.h"
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
    void check(const ColumnsDescription & columns) const;

private:
    std::shared_ptr<Apache::Hadoop::Hive::Table> hive_table;
    Poco::Logger * log {&Poco::Logger::get("HiveSchemaConverter")};
};

}
