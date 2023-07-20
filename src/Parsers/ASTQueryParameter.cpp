#include <Parsers/ASTQueryParameter.h>
#include <IO/WriteHelpers.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

void ASTQueryParameter::formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr
        << (settings.hilite ? hilite_substitution : "") << '{'
        << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(name)
        << (settings.hilite ? hilite_substitution : "") << ':'
        << (settings.hilite ? hilite_identifier : "") << type
        << (settings.hilite ? hilite_substitution : "") << '}'
        << (settings.hilite ? hilite_none : "");
}

void ASTQueryParameter::appendColumnNameImpl(WriteBuffer & ostr) const
{
    writeString(name, ostr);
}

void ASTQueryParameter::serialize(WriteBuffer & buf) const
{
    writeBinary(name, buf);
}

ASTPtr ASTQueryParameter::deserialize(ReadBuffer & buf)
{
    String name;
    readBinary(name, buf);
    String type;
    readBinary(type, buf);
    auto ast = std::make_shared<ASTQueryParameter>(name, type);
    ast->deserializeImpl(buf);
    return ast;
}

}
