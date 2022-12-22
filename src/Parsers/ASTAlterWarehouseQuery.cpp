#include <IO/Operators.h>
#include <Parsers/ASTAlterWarehouseQuery.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{

ASTPtr ASTAlterWarehouseQuery::clone() const
{
    auto res = std::make_shared<ASTAlterWarehouseQuery>(*this);
    res->children.clear();

    if (settings)
        res->set(res->settings, settings->clone());

    return res;
}

void ASTAlterWarehouseQuery::formatImpl(const FormatSettings &s, FormatState &state, FormatStateStacked frame) const
{

    s.ostr << (s.hilite ? hilite_keyword : "") 
           << "ALTER WAREHOUSE " 
           << (s.hilite ? hilite_none : "");

    s.ostr << (s.hilite ? hilite_identifier : "") << name << (s.hilite ? hilite_none : "");

    if (!rename_to.empty())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "RENAME TO " << rename_to
                      << (s.hilite ? hilite_none : "");
    }

    if (settings)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "SETTINGS " << (s.hilite ? hilite_none : "");
        settings->formatImpl(s, state, frame);
    }
}

}
