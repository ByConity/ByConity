#include <optional>
#include <Catalog/Catalog.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemExternalDatabases.h>
#include "Common/Exception.h"
#include <Common/Status.h>
#include "ExternalCatalog/IExternalCatalogMgr.h"
#include "Parsers/ASTFunction.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTLiteral.h"
#include "Parsers/ASTSelectQuery.h"
#include <Storages/System/CollectWhereClausePredicate.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

NamesAndTypesList StorageSystemExternalDatabases::getNamesAndTypes()
{
    return {
        {"catalog_name", std::make_shared<DataTypeString>()},
        {"database", std::make_shared<DataTypeString>()},
    };
}




void StorageSystemExternalDatabases::fillData(MutableColumns & res_columns, [[maybe_unused]] ContextPtr context, const SelectQueryInfo & query_info) const
{
    const auto & select = query_info.query->as<ASTSelectQuery>();
    if (!select->where() && !select->prewhere())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'catalog_name must be specified in where conditions");
    }
    String catalog_name;
    bool extracted = extractNameFromWhereClause(select->where(), "catalog_name", catalog_name);
    if (!extracted)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'catalog_name must be specified in where conditions");
    }
    const String tenant_id = context->getTenantId();
    if(!tenant_id.empty() && !startsWith(catalog_name, tenant_id + "."))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "could only view catalog infos within the same tenant");
    }
    auto catalog_ptr = ExternalCatalog::Mgr::instance().tryGetCatalog(catalog_name);
    if (!catalog_ptr)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "catalog {} does not exist", catalog_name);
    }
    auto all_dbs = catalog_ptr->listDbNames();
    for(const auto & database_name: all_dbs){
        res_columns[0]->insert(catalog_name);
        res_columns[1]->insert(database_name);
    }
}

}
