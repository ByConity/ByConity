#include <Parsers/ASTDropFunctionQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropFunctionQuery::clone() const
{
    return std::make_shared<ASTDropFunctionQuery>(*this);
}

void ASTDropFunctionQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "DROP ";
    if (cnch_local)
        settings.ostr << "LOCAL ";
    settings.ostr << "FUNCTION " << (settings.hilite ? hilite_none : "");
    
    if (if_exists)
        settings.ostr << "IF EXISTS ";

    if (!database_name.empty())
    {
        settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(database_name) << (settings.hilite ? hilite_none : "");
        settings.ostr << (settings.hilite ? hilite_identifier : "") << "." << (settings.hilite ? hilite_none : "");
    }

    settings.ostr << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(function_name) << (settings.hilite ? hilite_none : "");
    formatOnCluster(settings);
}


}
