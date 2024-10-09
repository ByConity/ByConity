#include <IO/Operators.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>

namespace DB
{

String ASTQueryWithTableAndOutput::getDatabase() const
{
    return database;
}

String ASTQueryWithTableAndOutput::getTable() const
{
    return table;
}

void ASTQueryWithTableAndOutput::setDatabase(const String & name)
{
    database = name;
}

void ASTQueryWithTableAndOutput::setTable(const String & name)
{
    table = name;
}

void ASTQueryWithTableAndOutput::setTableInfo(const StorageID &storage_id)
{
    database = storage_id.database_name;
    table = storage_id.table_name;
    uuid = storage_id.uuid;
}

StorageID ASTQueryWithTableAndOutput::getTableInfo() const
{
    return StorageID(database, table, uuid);
}

void ASTQueryWithTableAndOutput::formatHelper(const FormatSettings & settings, const char * name) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << name << " " << (settings.hilite ? hilite_none : "");
    settings.ostr << (!catalog.empty() ? backQuoteIfNeed(catalog) + "." : "");
    settings.ostr << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
}

}

