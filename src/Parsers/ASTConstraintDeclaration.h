#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/** name CHECK logical_expr
 */
class ASTConstraintDeclaration : public IAST
{
public:
    String name;
    IAST * expr;

    String getID(char) const override { return "Constraint"; }

    ASTType getType() const override { return ASTType::ASTConstraintDeclaration; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};
}
