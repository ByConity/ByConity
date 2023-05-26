#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Interpreters/StorageID.h>

namespace DB
{

void ASTQueryWithTableAndOutput::setTableInfo(const StorageID &storage_id)
{
    catalog = storage_id.catalog_name;
    database =storage_id.database_name;
    table = storage_id.table_name;
    uuid = storage_id.uuid;
}

StorageID ASTQueryWithTableAndOutput::getTableInfo() const 
{
    return StorageID(catalog, database, table , uuid);
}

void ASTQueryWithTableAndOutput::formatHelper(const FormatSettings & settings, const char * name) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << name << " " << (settings.hilite ? hilite_none : "");
    settings.ostr << (!catalog.empty()? backQuoteIfNeed(catalog)+ "." : ""); 
    settings.ostr << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
}

}

