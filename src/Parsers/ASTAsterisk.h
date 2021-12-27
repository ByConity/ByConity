#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** SELECT * is expanded to all visible columns of the source table.
  * Optional transformers can be attached to further manipulate these expanded columns.
  */
class ASTAsterisk : public IAST
{
public:
    String getID(char) const override { return "Asterisk"; }
    ASTType getType() const override { return ASTType::ASTAsterisk; }
    ASTPtr clone() const override;
    void appendColumnName(WriteBuffer & ostr) const override;

    void serialize(WriteBuffer & buf) const override;
    void deserializeImpl(ReadBuffer & buf) override;
    static ASTPtr deserialize(ReadBuffer & buf);

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
