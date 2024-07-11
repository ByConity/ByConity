#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** SET SENSITIVE query
  */
class ASTSetSensitiveQuery : public IAST
{
public:

    String database;
    String table;
    String column;
    String target; // NOTE: change to ENUM?
    bool value;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Set Sensitive"; }

    ASTType getType() const override { return ASTType::ASTSetSensitiveQuery; }

    ASTPtr clone() const override { return std::make_shared<ASTSetSensitiveQuery>(*this); }

    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
};

}
