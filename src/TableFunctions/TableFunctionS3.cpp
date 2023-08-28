#include <Common/config.h>

#if USE_AWS_S3
#    include <Storages/RemoteFile/CnchFileCommon.h>
#    include <TableFunctions/TableFunctionFactory.h>
#    include <TableFunctions/TableFunctionS3.h>
#    include <Storages/RemoteFile/StorageCnchS3.h>
#    include <TableFunctions/parseColumnsListForTableFunction.h>

namespace DB
{
StoragePtr TableFunctionS3::getStorage(const ColumnsDescription & columns, ContextPtr global_context, const std::string & table_name) const
{
   
    return StorageCnchS3::create(
        std::const_pointer_cast<Context>(global_context),
        StorageID(global_context->getCurrentDatabase(), table_name),
        columns,
        ConstraintsDescription{},
        nullptr,
        arguments,
        global_context->getCnchFileSettings());// todo(jiashuo): just pass context is enough
}

ColumnsDescription TableFunctionS3::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(arguments.structure, context);
}

void registerTableFunctionS3(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3>();
}

void registerTableFunctionCOS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionCOS>();
}

}
#endif
