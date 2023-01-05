#include <IO/Operators.h>
#include <Parsers/ASTShowWarehousesQuery.h>
#include <Parsers/ASTSetQuery.h>

#include <sstream>

namespace DB
{

ASTPtr ASTShowWarehousesQuery::clone() const
{
    auto res = std::make_shared<ASTShowWarehousesQuery>(*this);
    res->children.clear();

    if (!like.empty())
        res->like = like;

    return res;

}
void ASTShowWarehousesQuery::formatImpl(const FormatSettings & s, FormatState &/*state*/, FormatStateStacked /*frame*/) const
{
    s.ostr << (s.hilite ? hilite_keyword : "") 
           << "SHOW WAREHOUSES " 
           << (s.hilite ? hilite_none : "");

    if (!like.empty())
    {
        std::stringstream ss;
        ss << std::quoted(like, '\'');
        s.ostr << (s.hilite ? hilite_keyword : "") 
               << "LIKE " 
               << (s.hilite ? hilite_none : "") 
               << ss.str();
    }

}

}
