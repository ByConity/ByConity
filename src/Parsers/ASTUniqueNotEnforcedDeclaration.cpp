#include <Core/ColumnsWithTypeAndName.h>
#include <IO/Operators.h>
#include <Parsers/ASTUniqueNotEnforcedDeclaration.h>
#include <Common/quoteString.h>


namespace DB
{

ASTPtr ASTUniqueNotEnforcedDeclaration::clone() const
{
    auto res = std::make_shared<ASTUniqueNotEnforcedDeclaration>();

    res->name = name;
    if (column_names)
        res->column_names = column_names->clone();
    return res;
}

void ASTUniqueNotEnforcedDeclaration::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    if (name.empty())
        throw Exception("BUG: unique not enforced name is empty", ErrorCodes::INCORRECT_DATA);

    s.ostr << backQuoteIfNeed(name);

    s.ostr << (s.hilite ? hilite_keyword : "") << " UNIQUE " << (s.hilite ? hilite_none : "");

    s.ostr << (s.hilite ? hilite_identifier : "") << "(";
    column_names->formatImpl(s, state, frame);
    s.ostr << ")" << (s.hilite ? hilite_none : "");
}

}
