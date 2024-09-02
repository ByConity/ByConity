#include <optional>
#include <Catalog/Catalog.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/CollectWhereClausePredicate.h>
#include <Storages/System/StorageSystemExternalTables.h>
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

NamesAndTypesList StorageSystemExternalTables::getNamesAndTypes()
{
    return {
        {"catalog_name", std::make_shared<DataTypeString>()},
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemExternalTables::fillData(MutableColumns & res_columns, [[maybe_unused]] ContextPtr context, const SelectQueryInfo & query_info) const
{
    const auto & select = query_info.query->as<ASTSelectQuery>();
    if (!select->where() && !select->prewhere())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`catalog_name = some_catalog` must be specified in where conditions");
    }
    String catalog_name;
    bool extracted = extractNameFromWhereClause(select->where(), "catalog_name", catalog_name);
    if (!extracted)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`catalog_name = some_catalog` must be specified in where conditions");
    }

    String database_name;
    extracted = extractNameFromWhereClause(select->where(), "database", database_name);

    auto catalog_ptr = ExternalCatalog::Mgr::instance().tryGetCatalog(catalog_name);
    if (!catalog_ptr)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "catalog {} does not exist", catalog_name);
    }
    auto all_tbls = catalog_ptr->listTableNames(database_name);
    for(const auto & table_name: all_tbls){
        res_columns[0]->insert(catalog_name);
        res_columns[1]->insert(database_name);
        res_columns[2]->insert(table_name);
    }
}

}
