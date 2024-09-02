#include <IO/Operators.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTSerDerHelper.h>


namespace DB
{

ASTDataType::ASTDataType(const ASTPtr & dt, bool nullable_)
    : data_type(dt), nullable(nullable_)
{
    children.push_back(dt);
}

ASTPtr ASTDataType::clone() const
{
    return std::make_shared<ASTDataType>(this->data_type->clone(), nullable);
}

void ASTDataType::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (settings.dialect_type != DialectType::CLICKHOUSE)
    {
        data_type->formatImpl(settings, state, frame);
        settings.ostr << ' ' << (settings.hilite ? hilite_keyword : "")
                      << (nullable ? "NULL" : "NOT NULL")
                      << (settings.hilite ? hilite_none : "");
    }
    else
    {
        if (nullable)
            settings.ostr << (settings.hilite ? hilite_function : "")
                          << "Nullable" << (settings.hilite ? hilite_none : "")
                          << "(";
        data_type->formatImpl(settings, state, frame);
        if (nullable)
            settings.ostr << ')';
    }
}

void ASTDataType::serialize(WriteBuffer & buf) const
{
    writeChar(nullable, buf);
    serializeASTs(children, buf);
}

void ASTDataType::deserializeImpl(ReadBuffer & buf)
{
    char nullable_char;
    readChar(nullable_char, buf);
    nullable = static_cast<bool>(nullable_char);
    children = deserializeASTs(buf);
    data_type = children[0];
}

ASTPtr ASTDataType::deserialize(ReadBuffer & buf)
{
    auto data_type = std::make_shared<ASTDataType>();
    data_type->deserializeImpl(buf);
    return data_type;
}

}
