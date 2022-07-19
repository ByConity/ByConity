#include <Parsers/ASTReproduceQuery.h>
namespace DB
{
ASTPtr ASTReproduceQuery::clone() const
{
    auto res = std::make_shared<ASTReproduceQuery>(*this);
    res->children.clear();
    res->reproduce_path = reproduce_path;
    return res;
}
void ASTReproduceQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "REPRODUCE" << (settings.hilite ? hilite_none : "");
    settings.ostr << settings.nl_or_ws;
    settings.ostr << reproduce_path;
}
}
