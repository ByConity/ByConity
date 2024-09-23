#include <memory>
#include <optional>
#include <Catalog/Catalog.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/CollectWhereClausePredicate.h>
#include <Storages/System/StorageSystemExternalCatalogs.h>
#include "Common/Exception.h"
#include <Common/Status.h>
#include "ExternalCatalog/IExternalCatalogMgr.h"
#include "Parsers/ASTFunction.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTLiteral.h"
#include "Parsers/ASTSelectQuery.h"
namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

NamesAndTypesList StorageSystemExternalCatalogs::getNamesAndTypes()
{
    return {
        {"catalog_name", std::make_shared<DataTypeString>()},
        {"create_time", std::make_shared<DataTypeDateTime>()}};
}


void StorageSystemExternalCatalogs::fillData(
    MutableColumns & res_columns, [[maybe_unused]] ContextPtr context, const SelectQueryInfo &) const
{
    const String tenant_id = context->getTenantId();
    auto catalogs = ExternalCatalog::Mgr::instance().listCatalog();
    for (const auto & c : catalogs)
    {
        if (!tenant_id.empty() && !startsWith(c.first, tenant_id + "."))
            continue;
        res_columns[0]->insert(c.first);
        if (c.second.create_time.has_value())
        {
            res_columns[1]->insert(c.second.create_time.value());
        }
        else
        {
            res_columns[1]->insert(0);
        }
    }

}

}
