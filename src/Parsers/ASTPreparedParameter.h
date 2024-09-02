#pragma once

#include <Parsers/ASTWithAlias.h>


namespace DB
{

/// Parameter in prepared query with name and type of substitution ([name:type]).
/// Example: PREPARE SELECT * FROM table WHERE id = [pid:UInt16].
class ASTPreparedParameter : public ASTWithAlias
{
public:
    String name;
    String type;

    ASTPreparedParameter() = default;
    ASTPreparedParameter(const String & name_, const String & type_) : name(name_), type(type_)
    {
    }

    /** Get the text that identifies this element. */
    String getID(char delim) const override
    {
        return String("PreparedParameter") + delim + name;
    }

    ASTType getType() const override
    {
        return ASTType::ASTPreparedParameter;
    }

    ASTPtr clone() const override
    {
        return std::make_shared<ASTPreparedParameter>(*this);
    }

    void serialize(WriteBuffer & buf) const override;
    static ASTPtr deserialize(ReadBuffer & buf);

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;

private:
    void deserializeImpl(ReadBuffer & buf) override;
};

}
