#include <Storages/StorageURL.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionURL.h>
#include <Poco/URI.h>
#include <Storages/StorageExternalDistributed.h>

namespace DB
{
StoragePtr TableFunctionURL::getStorage(const ColumnsDescription & columns, ContextPtr global_context, const std::string & table_name) const
{
     /// If url contains {1..k} or failover options with separator `|`, use a separate storage
    if ((arguments.url.find('{') == std::string::npos || arguments.url.find('}') == std::string::npos) && arguments.url.find('|') == std::string::npos)
    {
        Poco::URI uri(arguments.url);
        return StorageURL::create(
            uri,
            StorageID(getDatabaseName(), table_name),
            arguments.format_name,
            std::nullopt /*format settings*/,
            columns,
            ConstraintsDescription{},
            String{},
            global_context,
            arguments.compression_method);
    }
    else
    {
        return StorageExternalDistributed::create(
            arguments.url,
            StorageID(getDatabaseName(), table_name),
            arguments.format_name,
            std::nullopt,
            arguments.compression_method,
            columns,
            ConstraintsDescription{},
            global_context);
    }
}

void registerTableFunctionURL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionURL>();
}
}
