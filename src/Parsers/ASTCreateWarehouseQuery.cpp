#include <IO/Operators.h>
#include <Parsers/ASTCreateWarehouseQuery.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{

ASTPtr ASTCreateWarehouseQuery::clone() const
{
    auto res = std::make_shared<ASTCreateWarehouseQuery>(*this);
    res->children.clear();

    if (settings)
        res->set(res->settings, settings->clone());

    return res;

}

void ASTCreateWarehouseQuery::formatImpl(const FormatSettings &s, FormatState &state, FormatStateStacked frame) const
{
    s.ostr << (s.hilite ? hilite_keyword : "")
           << "CREATE WAREHOUSE "
           << (if_not_exists ? "IF NOT EXISTS " : "")
           << (s.hilite ? hilite_none : "");

    s.ostr << (s.hilite ? hilite_identifier : "") << name << (s.hilite ? hilite_none : "");

    if (settings)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") 
        << s.nl_or_ws << "SETTINGS " 
        << (s.hilite ? hilite_none : "");
        
        settings->formatImpl(s, state, frame);
    }
}

}
