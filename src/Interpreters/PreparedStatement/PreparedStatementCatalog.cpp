#include <Catalog/Catalog.h>
#include <Interpreters/PreparedStatement/PreparedStatementCatalog.h>

namespace DB
{
void PreparedStatementCatalogManager::updatePreparedStatement(PreparedStatementItemPtr data)
{
    catalog->updatePreparedStatement(data);
}

PreparedStatements PreparedStatementCatalogManager::getPreparedStatements()
{
    return catalog->getPreparedStatements();
}

PreparedStatementItemPtr PreparedStatementCatalogManager::getPreparedStatement(const String & name)
{
    return catalog->getPreparedStatement(name);
}

void PreparedStatementCatalogManager::removePreparedStatement(const String & name)
{
    catalog->removePreparedStatement(name);
}


}
