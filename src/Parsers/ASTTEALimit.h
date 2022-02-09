#pragma once

#include <Parsers/IAST.h>
#include <Core/Field.h>

namespace DB
{
/**
 * TEALIMIT N GROUP (g_0, ... , g_n) ORDER EXPR(cnt_0, ... cnt_n) ASC|DESC
 */
class ASTTEALimit : public IAST
{
public:
    ASTPtr limit_value;            // ASTLiteral
    ASTPtr limit_offset;            // ASTLiteral
    ASTPtr group_expr_list;  // ASTExpressionList
    ASTPtr order_expr_list;  // ASTOrderByElement

    String getID(char) const override;
    ASTPtr clone() const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
