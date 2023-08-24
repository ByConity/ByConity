#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Interpreters/Context.h>
#include <Storages/RemoteFile/CnchFileCommon.h>
#include <Core/Block.h>

namespace DB
{
/*
 * function(source, format, structure) - creates a temporary storage from formated source
 */
class ITableFunctionFileLike : public ITableFunction
{
private:
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    virtual StoragePtr getStorage(const ColumnsDescription & columns, ContextPtr global_context, const std::string & table_name) const = 0;
        
public:
    CnchFileArguments arguments;
};
}
