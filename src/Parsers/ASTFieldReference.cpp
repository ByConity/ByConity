#include <Parsers/ASTFieldReference.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTFieldReference::clone() const
{
    return std::make_shared<ASTFieldReference>(*this);
}

void ASTFieldReference::formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_identifier : "");
    settings.writeIdentifier("@" + std::to_string(field_index));
    settings.ostr << (settings.hilite ? hilite_none : "");
}

void ASTFieldReference::appendColumnNameImpl(WriteBuffer & ostr) const
{
    writeString("@" + std::to_string(field_index), ostr);
}

}
