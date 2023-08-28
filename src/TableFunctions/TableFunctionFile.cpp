#include <Storages/StorageFile.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionFile.h>

namespace DB
{
StoragePtr TableFunctionFile::getStorage(const ColumnsDescription & columns, ContextPtr global_context, const std::string & table_name) const
{
    StorageFile::CommonArguments args{
        WithContext(global_context),
        StorageID(getDatabaseName(), table_name),
        arguments.format_name,
        std::nullopt /*format settings*/,
        arguments.compression_method,
        columns,
        ConstraintsDescription{},
        String{},
    };

    return StorageFile::create(arguments.url, global_context->getUserFilesPath(), args);
}

void registerTableFunctionFile(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFile>();
}
}
