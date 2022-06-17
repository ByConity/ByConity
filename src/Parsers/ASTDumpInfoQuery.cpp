#include <Parsers/ASTDumpInfoQuery.h>
namespace DB
{
ASTPtr ASTDumpInfoQuery::clone() const
{
    auto res = std::make_shared<ASTDumpInfoQuery>(*this);
    res->children.clear();
    res->dump_string = dump_string;
    if (dump_query)
    {
        res->dump_query = dump_query->clone();
        res->children.push_back(res->dump_query);
    }
    return res;
}
void ASTDumpInfoQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "DUMP" << (settings.hilite ? hilite_none : "");
    settings.ostr << settings.nl_or_ws;
    if (dump_query)
        dump_query->formatImpl(settings, state, frame);
}
}
