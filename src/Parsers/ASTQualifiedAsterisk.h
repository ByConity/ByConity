#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** Something like t.*
  * It will have qualifier as its child ASTIdentifier.
  * Optional transformers can be attached to further manipulate these expanded columns.
  */
class ASTQualifiedAsterisk : public IAST
{
public:
    String getID(char) const override { return "QualifiedAsterisk"; }

    ASTType getType() const override { return ASTType::ASTQualifiedAsterisk; }

    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTQualifiedAsterisk>(*this);
        clone->cloneChildren();
        return clone;
    }
    void appendColumnName(WriteBuffer & ostr) const override;

    void serialize(WriteBuffer & buf) const override;
    void deserializeImpl(ReadBuffer & buf) override;
    static ASTPtr deserialize(ReadBuffer & buf);

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
