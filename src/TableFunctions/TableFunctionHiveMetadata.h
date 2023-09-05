#pragma once

#include "Common/config.h"
#if USE_HIVE

#include "TableFunctions/ITableFunction.h"

namespace DB
{
class TableFunctionHiveMetadata : public ITableFunction
{
public:
    static constexpr auto name = "hiveMetadata";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, ContextPtr context,
            const std::string & table_name, ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "CnchHive"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    void fillData(MutableColumns & columns) const;

    String hive_metastore_url;
    String hive_database_name;
    String hive_table_name;
};

}
#endif
