#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/** FOREIGN KEY (column_name [, ... ] ) REFERENCES reftable ( refcolumn, [, ... ] )
 */
class ASTForeignKeyDeclaration : public IAST
{
public:
    String fk_name;
    ASTPtr column_names; // type is ASTExpressionList.
    String ref_table_name;
    ASTPtr ref_column_names;

    String getID(char) const override
    {
        return "ForeignKey";
    }

    ASTType getType() const override
    {
        return ASTType::ASTForeignKeyDeclaration;
    }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};
}
