#include <Common/config.h>

#if USE_HDFS
#    include <Storages/RemoteFile/CnchFileCommon.h>
#    include <TableFunctions/TableFunctionFactory.h>
#    include <TableFunctions/TableFunctionHDFS.h>
#    include <Storages/RemoteFile/StorageCnchHDFS.h>
#    include <TableFunctions/parseColumnsListForTableFunction.h>

namespace DB
{
StoragePtr TableFunctionHDFS::getStorage(const ColumnsDescription & columns, ContextPtr global_context, const std::string & table_name) const
{
    return StorageCnchHDFS::create(
        std::const_pointer_cast<Context>(global_context),
        StorageID(global_context->getCurrentDatabase(), table_name),
        columns,
        ConstraintsDescription{},
        nullptr,
        arguments,
        global_context->getCnchFileSettings());
}

ColumnsDescription TableFunctionHDFS::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(arguments.structure, context);
}


void registerTableFunctionHDFS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHDFS>();
}
}
#endif
