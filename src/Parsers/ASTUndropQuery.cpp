#include <Parsers/ASTUndropQuery.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


String ASTUndropQuery::getID(char delim) const
{
    return "UndropQuery" + (delim + database) + delim + table;
}

ASTPtr ASTUndropQuery::clone() const
{
    auto res = std::make_shared<ASTUndropQuery>(*this);
    cloneOutputOptions(*res);
    return res;
}

void ASTUndropQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");
        settings.ostr << "UNDROP ";

    settings.ostr << ((table.empty() && !database.empty()) ? "DATABASE " : "TABLE ");

    settings.ostr << (settings.hilite ? hilite_none : "");

    if (table.empty() && !database.empty())
        settings.ostr << backQuoteIfNeed(database);
    else
        settings.ostr << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
}

}
