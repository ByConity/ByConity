#include <IO/Operators.h>
#include <Parsers/ASTDropWorkerGroupQuery.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{
ASTPtr ASTDropWorkerGroupQuery::clone() const
{
    auto res = std::make_shared<ASTDropWorkerGroupQuery>(*this);
    res->children.clear();

    if (settings)
        res->set(res->settings, settings->clone());

    return res;
}

void ASTDropWorkerGroupQuery::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    s.ostr << (s.hilite ? hilite_keyword : "") << "DROP WORKER GROUP " << (if_exists ? "IF EXISTS " : "") << (s.hilite ? hilite_none : "");

    s.ostr << (s.hilite ? hilite_identifier : "") << worker_group_id << (s.hilite ? hilite_none : "");

    if (settings)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "SETTINGS " << (s.hilite ? hilite_none : "");
        settings->formatImpl(s, state, frame);
    }
}

}
