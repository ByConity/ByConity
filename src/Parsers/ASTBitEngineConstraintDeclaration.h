#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTConstraintDeclaration.h>

namespace DB
{

/** name CHECK logical_expr
 */
class ASTBitEngineConstraintDeclaration : public ASTConstraintDeclaration
{
public:

    String getID(char) const override { return "BitEngineConstraint"; }
    ASTType getType() const override { return ASTType::ASTBitEngineConstraintDeclaration; }
    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};
}
