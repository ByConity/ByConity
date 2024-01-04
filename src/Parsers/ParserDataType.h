#pragma once
#include <Parsers/IParserBase.h>


namespace DB
{

/// Parses data type as ASTFunction
/// Examples: Int8, Array(Nullable(FixedString(16))), DOUBLE PRECISION, Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
class ParserDataType : public IParserDialectBase
{
protected:
    const char * getName() const override { return "data type"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
private:
    const bool is_root_type;
    const bool parent_nullable;
public:
    explicit ParserDataType(ParserSettingsImpl t = ParserSettings::CLICKHOUSE,
                            bool root_type = true,
                            bool parent_null = false)
            : IParserDialectBase(t), is_root_type(root_type), parent_nullable(parent_null) {}
};

}

