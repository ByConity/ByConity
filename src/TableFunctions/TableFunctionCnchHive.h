#pragma once
#include <Common/Logger.h>
#include "Common/config.h"

#if USE_HIVE
#include <TableFunctions/ITableFunction.h>
#include <Poco/Logger.h>
namespace DB
{
class Context;
class TableFunctionCnchHive : public ITableFunction
{
public:
    static constexpr auto name = "cnchHive";
    static constexpr auto storage_type_name = "CnchHive";
    std::string getName() const override { return name; }

    bool hasStaticStructure() const override { return true; }

    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return storage_type_name; }
    ColumnsDescription getActualTableStructure(ContextPtr) const override;
    void parseArguments(const ASTPtr & ast_function_, ContextPtr context_) override;

private:
    LoggerPtr logger = getLogger("TableFunctionHive");

    String cluster_name;
    String hive_metastore_url;
    String hive_database_name;
    String hive_table_name;

    StoragePtr storage;
};
}
#endif
