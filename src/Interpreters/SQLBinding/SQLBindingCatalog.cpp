#include <Catalog/Catalog.h>
#include <Statistics/SubqueryHelper.h>
#include <Interpreters/SQLBinding/SQLBindingCatalog.h>
#include <Interpreters/SQLBinding/SQLBindingCache.h>

namespace DB
{
void BindingCatalogManager::updateSQLBinding(const SQLBindingItemPtr & data)
{
    catalog->updateSQLBinding(data);
}

SQLBindings BindingCatalogManager::getSQLBindings()
{
    return catalog->getSQLBindings();
}

SQLBindings BindingCatalogManager::getReSQLBindings()
{
    return catalog->getReSQLBindings(true);
}

SQLBindingItemPtr BindingCatalogManager::getSQLBinding(const UUID & uuid, const String & tenant_id, const bool & is_re_expression)
{
    return catalog->getSQLBinding(UUIDHelpers::UUIDToString(uuid), tenant_id, is_re_expression);
}

void BindingCatalogManager::removeSQLBinding(const UUID & uuid, const String & tenant_id, const bool & is_re_expression)
{
    catalog->removeSQLBinding(UUIDHelpers::UUIDToString(uuid), tenant_id, is_re_expression);
}

void BindingCatalogManager::updateGlobalBindingCache(const ContextPtr & context)
{
    auto sql = fmt::format(
        FMT_STRING("select host(), updateBindingCache() from cnch(server, system.one) SETTINGS enable_optimizer=0"));
    DB::Statistics::executeSubQuery(context, sql);
}

}
