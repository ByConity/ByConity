#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/** UNIQUE (column_names...)
 */
class ASTUniqueNotEnforcedDeclaration : public IAST
{
public:
    String name; // constraint name
    ASTPtr column_names; // type is ASTExpressionList.

    String getID(char) const override
    {
        return "Unique Not Enforced";
    }

    ASTType getType() const override
    {
        return ASTType::ASTUniqueNotEnforcedDeclaration;
    }

    ASTPtr clone() const override;

    void toLowerCase() override { boost::to_lower(name); }

    void toUpperCase() override { boost::to_upper(name); }

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};
}
