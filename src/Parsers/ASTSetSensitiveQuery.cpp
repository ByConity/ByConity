#include <Parsers/ASTSetSensitiveQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/FieldVisitorToString.h>
#include <IO/Operators.h>


namespace DB
{

void ASTSetSensitiveQuery::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    format.ostr << (format.hilite ? hilite_keyword : "") << "SET SENSITIVE " << (format.hilite ? hilite_none : "");
    format.ostr << target << " ";

    if (!database.empty())
        format.ostr << database;

    if (!table.empty())
        format.ostr << "." << table;

    if (!column.empty())
        format.ostr << "(" << column << ")";

    format.ostr << " = ";
    format.ostr << (value ? "1" : "0");
}


}
