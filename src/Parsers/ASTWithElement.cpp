#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTSerDerHelper.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTWithElement::clone() const
{
    const auto res = std::make_shared<ASTWithElement>(*this);
    res->children.clear();
    res->subquery = subquery->clone();
    res->children.emplace_back(res->subquery);
    return res;
}

void ASTWithElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    settings.writeIdentifier(name);
    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS" << (settings.hilite ? hilite_none : "");
    settings.ostr << settings.nl_or_ws << indent_str;
    dynamic_cast<const ASTWithAlias &>(*subquery).formatImplWithoutAlias(settings, state, frame);
}

void ASTWithElement::serialize(WriteBuffer & buf) const
{
    writeBinary(name, buf);
    serializeAST(subquery, buf);
}

void ASTWithElement::deserializeImpl(ReadBuffer & buf)
{
    readBinary(name, buf);
    subquery = deserializeASTWithChildren(children, buf);
}

ASTPtr ASTWithElement::deserialize(ReadBuffer & buf)
{
    auto with = std::make_shared<ASTWithElement>();
    with->deserializeImpl(buf);
    return with;
}

}
