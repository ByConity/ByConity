#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSerDerHelper.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

void ASTQualifiedAsterisk::appendColumnName(WriteBuffer & ostr) const
{
    const auto & qualifier = children.at(0);
    qualifier->appendColumnName(ostr);
    writeCString(".*", ostr);
}

void ASTQualifiedAsterisk::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    const auto & qualifier = children.at(0);
    qualifier->formatImpl(settings, state, frame);
    settings.ostr << ".*";
    for (ASTs::const_iterator it = children.begin() + 1; it != children.end(); ++it)
    {
        settings.ostr << ' ';
        (*it)->formatImpl(settings, state, frame);
    }
}

void ASTQualifiedAsterisk::serialize(WriteBuffer & buf) const
{
    serializeASTs(children, buf);
}

void ASTQualifiedAsterisk::deserializeImpl(ReadBuffer & buf)
{
    children = deserializeASTs(buf);
}

ASTPtr ASTQualifiedAsterisk::deserialize(ReadBuffer & buf)
{
    auto asterisk = std::make_shared<ASTQualifiedAsterisk>();
    asterisk->deserializeImpl(buf);
    return asterisk;
}

}
