#include <Parsers/ASTTEALimit.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{
#define CLONE(member) if (member) { res->member = member->clone(); res->children.push_back(res->member); }

    ASTPtr ASTTEALimit::clone() const
    {
        auto res = std::make_shared<ASTTEALimit>(*this);
        res->children.clear(); 
        CLONE(limit_value)
        CLONE(limit_offset)
        CLONE(group_expr_list)
        CLONE(order_expr_list)
        return res;
    }
#undef CLONE

    String ASTTEALimit::getID(char) const
    {
        return "TEALIMIT";
    }

    void ASTTEALimit::formatImpl(const FormatSettings & s, 
                                 FormatState & state, 
                                 FormatStateStacked frame) const
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << "TEALIMIT ";
        if (limit_value)
        {
            if (limit_offset)
            {
                limit_offset->formatImpl(s, state, frame);
                s.ostr << ", ";
            }

            limit_value->formatImpl(s, state, frame);
        }

        if (group_expr_list)
        {
            s.ostr << (s.hilite ? hilite_keyword : "") << " GROUP ";
            group_expr_list->formatImpl(s, state, frame);
        }

        if (order_expr_list)
        {
            s.ostr << (s.hilite ? hilite_keyword : "") << " ORDER ";
            order_expr_list->formatImpl(s, state, frame);
        }
    }
}
//  namespace DB
