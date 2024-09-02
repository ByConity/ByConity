#include <Parsers/ASTPreparedParameter.h>

#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
};

void ASTPreparedParameter::formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_substitution : "") << '[' << (settings.hilite ? hilite_identifier : "")
                  << backQuoteIfNeed(name) << (settings.hilite ? hilite_substitution : "") << ':'
                  << (settings.hilite ? hilite_function : "") << type << (settings.hilite ? hilite_substitution : "") << ']'
                  << (settings.hilite ? hilite_none : "");
}

void ASTPreparedParameter::appendColumnNameImpl(WriteBuffer & ostr) const
{
    writeString("PreparedParam_" + name, ostr);
}

void ASTPreparedParameter::serialize(WriteBuffer & buf) const
{
    ASTWithAlias::serialize(buf);
    writeBinary(name, buf);
    writeBinary(type, buf);
}

ASTPtr ASTPreparedParameter::deserialize(ReadBuffer & buf)
{
    auto prepared_param = std::make_shared<ASTPreparedParameter>();
    prepared_param->deserializeImpl(buf);
    return prepared_param;
}

void ASTPreparedParameter::deserializeImpl(ReadBuffer & buf)
{
    ASTWithAlias::deserializeImpl(buf);
    readBinary(name, buf);
    readBinary(type, buf);
}
}
