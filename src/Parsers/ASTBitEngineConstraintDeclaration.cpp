#include <Parsers/ASTBitEngineConstraintDeclaration.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB {

ASTPtr ASTBitEngineConstraintDeclaration::clone() const
{
    auto res = std::make_shared<ASTBitEngineConstraintDeclaration>();

    res->name = name;

    if (expr)
        res->set(res->expr, expr->clone());

    return res;
}

void ASTBitEngineConstraintDeclaration::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;
    std::string indent_str = s.one_line ? "" : std::string(4 * frame.indent, ' ');

    s.ostr << s.nl_or_ws << indent_str;
    s.ostr << backQuoteIfNeed(name);
    s.ostr << (s.hilite ? hilite_keyword : "") << " CHECK " << (s.hilite ? hilite_none : "");
    expr->formatImpl(s, state, frame);
}

}
