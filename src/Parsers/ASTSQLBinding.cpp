#include <Parsers/ASTSQLBinding.h>

namespace DB
{
ASTPtr ASTCreateBinding::clone() const
{
    auto res = std::make_shared<ASTCreateBinding>(*this);
    res->children.clear();
    res->positions.clear();

#define CLONE(expr) res->setExpression(expr, getExpression(expr, true))

    CLONE(Expression::QUERY_PATTERN);
    CLONE(Expression::TARGET);
    CLONE(Expression::SETTINGS);

#undef CLONE

    return res;
}

void ASTCreateBinding::setExpression(Expression expr, ASTPtr && ast)
{
    if (ast)
    {
        auto it = positions.find(expr);
        if (it == positions.end())
        {
            positions[expr] = children.size();
            children.emplace_back(ast);
        }
        else
            children[it->second] = ast;
    }
    else if (positions.count(expr))
    {
        size_t pos = positions[expr];
        children.erase(children.begin() + pos);
        positions.erase(expr);
        for (auto & pr : positions)
            if (pr.second > pos)
                --pr.second;
    }
}

ASTPtr ASTCreateBinding::getExpression(Expression expr, bool clone) const
{
    auto it = positions.find(expr);
    if (it != positions.end())
        return clone ? children[it->second]->clone() : children[it->second];
    return {};
}

void ASTCreateBinding::formatImpl(const FormatSettings & format, FormatState & state, FormatStateStacked frame) const
{
    format.ostr << (format.hilite ? hilite_keyword : "") << "CREATE";
    switch (level)
    {
        case BindingLevel::SESSION:
            format.ostr << (format.hilite ? hilite_keyword : "") << " SESSION";
            break;
        case BindingLevel::GLOBAL:
            format.ostr << (format.hilite ? hilite_keyword : "") << " GLOBAL";
            break;
    }
    format.ostr << (format.hilite ? hilite_keyword : "") << " BINDING";

    if (!re_expression.empty())
    {
        format.ostr << format.nl_or_ws << (format.hilite ? hilite_none : "") << re_expression;
        if (settings())
        {
            format.ostr << format.nl_or_ws << (format.hilite ? hilite_keyword : "") << "SETTINGS " << (format.hilite ? hilite_none : "");
            settings()->formatImpl(format, state, frame);
        }
    }
    else
    {
        if (pattern())
        {
            format.ostr << format.nl_or_ws;
            ++frame.indent;
            pattern()->formatImpl(format, state, frame);
            --frame.indent;
        }

        if (target())
        {
            format.ostr << format.nl_or_ws << (format.hilite ? hilite_keyword : "") << "USING" << (format.hilite ? hilite_none : "");

            format.ostr << format.nl_or_ws;
            ++frame.indent;
            target()->formatImpl(format, state, frame);
            --frame.indent;
        }
    }
}

ASTPtr ASTShowBindings::clone() const
{
    auto res = std::make_shared<ASTShowBindings>(*this);
    return res;
}

void ASTShowBindings::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    format.ostr << (format.hilite ? hilite_keyword : "") << "SHOW BINDINGS" << (format.hilite ? hilite_none : "");
}

ASTPtr ASTDropBinding::clone() const
{
    return std::make_shared<ASTDropBinding>(*this);
}

void ASTDropBinding::formatImpl(const FormatSettings & format, FormatState & state, FormatStateStacked frame) const
{
    format.ostr << (format.hilite ? hilite_keyword : "") << "DROP";
    switch (level)
    {
        case BindingLevel::SESSION:
            format.ostr  << (format.hilite ? hilite_keyword : "") << " SESSION";
            break;
        case BindingLevel::GLOBAL:
            format.ostr  << (format.hilite ? hilite_keyword : "") << " GLOBAL";
            break;
    }
    format.ostr  << (format.hilite ? hilite_keyword : "") << " BINDING";

    if (!re_expression.empty())
    {
        format.ostr << format.nl_or_ws << (format.hilite ? hilite_none : "") << re_expression;
    }
    else if (!uuid.empty())
    {
        format.ostr  << (format.hilite ? hilite_keyword : "") << " UUID ";
        format.ostr << format.nl_or_ws << (format.hilite ? hilite_none : "") << uuid ;
    }
    else if (pattern_ast)
    {
        format.ostr << format.nl_or_ws;
        ++frame.indent;
        pattern_ast->formatImpl(format, state, frame);
        --frame.indent;
    }
}


}