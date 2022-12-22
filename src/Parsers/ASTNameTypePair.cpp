#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTSerDerHelper.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTNameTypePair::clone() const
{
    auto res = std::make_shared<ASTNameTypePair>(*this);
    res->children.clear();

    if (type)
    {
        res->type = type->clone();
        res->children.push_back(res->type);
    }

    return res;
}


void ASTNameTypePair::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    settings.ostr << indent_str << backQuoteIfNeed(name) << ' ';
    type->formatImpl(settings, state, frame);
}

void ASTNameTypePair::serialize(WriteBuffer & buf) const
{
    writeBinary(name, buf);
    serializeAST(type, buf);
}

void ASTNameTypePair::deserializeImpl(ReadBuffer & buf)
{
    readBinary(name, buf);
    type = deserializeASTWithChildren(children, buf);
}

ASTPtr ASTNameTypePair::deserialize(ReadBuffer & buf)
{
    auto name_type = std::make_shared<ASTNameTypePair>();
    name_type->deserializeImpl(buf);
    return name_type;
}

}


