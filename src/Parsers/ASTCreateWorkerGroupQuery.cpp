#include <IO/Operators.h>
#include <Parsers/ASTCreateWorkerGroupQuery.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{
ASTPtr ASTCreateWorkerGroupQuery::clone() const
{
    auto res = std::make_shared<ASTCreateWorkerGroupQuery>(*this);
    res->children.clear();

    if (settings)
        res->set(res->settings, settings->clone());

    return res;
}

void ASTCreateWorkerGroupQuery::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    s.ostr << (s.hilite ? hilite_keyword : "") 
           << "CREATE WORKER GROUP " 
           << (if_not_exists ? "IF NOT EXISTS " : "")
           << (s.hilite ? hilite_none : "");

    s.ostr << (s.hilite ? hilite_identifier : "") 
           << worker_group_id 
           << (s.hilite ? hilite_none : "");

    if (!vw_name.empty())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") 
               << " IN " 
               << (s.hilite ? hilite_identifier : "") 
               << (s.hilite ? hilite_none : "");

        s.ostr << (s.hilite ? hilite_identifier : "") << vw_name << (s.hilite ? hilite_none : "");
    }

    if (settings)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") 
               << s.nl_or_ws 
               << "SETTINGS " 
               << (s.hilite ? hilite_none : "");

        settings->formatImpl(s, state, frame);
    }
}

}
