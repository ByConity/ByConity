#include <IO/Operators.h>
#include <Parsers/ASTDropWarehouseQuery.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{

ASTPtr ASTDropWarehouseQuery::clone() const
{
    auto res = std::make_shared<ASTDropWarehouseQuery>(*this);
    res->children.clear();

    return res;

}

void ASTDropWarehouseQuery::formatImpl(const FormatSettings &s, FormatState &/*state*/, FormatStateStacked /*frame*/) const
{
    s.ostr << (s.hilite ? hilite_keyword : "") 
           << "DROP WAREHOUSE " 
           << (if_exists ? "IF EXISTS " : "")
           << (s.hilite ? hilite_none : "");

    s.ostr << (s.hilite ? hilite_identifier : "") << name << (s.hilite ? hilite_none : "");

}

}
