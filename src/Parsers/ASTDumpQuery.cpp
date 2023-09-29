#include <Parsers/ASTDumpQuery.h>


namespace DB
{

String ASTDumpQuery::getID(char) const
{
    switch (kind)
    {
        case ASTDumpQuery::Kind::DDL:
            return "DumpDDLQuery";
        case ASTDumpQuery::Kind::Query:
            return "DumpQueryInfoQuery";
        case ASTDumpQuery::Kind::Workload:
            return "DumpWorkloadQuery";
    }
}

ASTPtr ASTDumpQuery::clone() const
{
    auto res = std::make_shared<ASTDumpQuery>(*this);
    res->children.clear();
    res->positions.clear();

#define CLONE(expr) res->setExpression(expr, getExpression(expr, true))

    CLONE(Expression::DATABASES);
    CLONE(Expression::SUBQUERY);
    CLONE(Expression::QUERY_IDS);
    CLONE(Expression::BEGIN_TIME);
    CLONE(Expression::END_TIME);
    CLONE(Expression::WHERE);
    CLONE(Expression::LIMIT_LENGTH);
    CLONE(Expression::CLUSTER);
    CLONE(Expression::DUMP_PATH);

#undef CLONE

    return res;
}

void ASTDumpQuery::setExpression(Expression expr, ASTPtr && ast)
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

void ASTDumpQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "DUMP";
    switch (kind)
    {
        case ASTDumpQuery::Kind::DDL:
            settings.ostr << " DDL" << (settings.hilite ? hilite_none : "");
            if (databases())
            {
                settings.ostr << settings.nl_or_ws
                              << (settings.hilite ? hilite_keyword : "") << "FROM " << (settings.hilite ? hilite_none : "");
                databases()->formatImpl(settings, state, frame);
            }
            break;
        case ASTDumpQuery::Kind::Query:
            settings.ostr << " QUERY" << (settings.hilite ? hilite_none : "");
            if (without_ddl)
                settings.ostr << " WITHOUT DDL";
            if (subquery())
            {
                settings.ostr << settings.nl_or_ws;
                ++frame.indent;
                subquery()->formatImpl(settings, state, frame);
                --frame.indent;
            } else if (queryIds())
            {
                settings.ostr << " IDS ";
                queryIds()->formatImpl(settings, state, frame);
            } else {
                throw Exception("Dump query info query must have either subquery or query ids", ErrorCodes::LOGICAL_ERROR);
            }
            break;
        case ASTDumpQuery::Kind::Workload:
            settings.ostr << " WORKLOAD" << (settings.hilite ? hilite_none : "");
            if (without_ddl)
                settings.ostr << " WITHOUT DDL";
            if (cluster())
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " ON CLUSTER " << (settings.hilite ? hilite_none : "");
                cluster()->formatImpl(settings, state, frame);
            }
            if (where())
            {
                settings.ostr << settings.nl_or_ws
                              << (settings.hilite ? hilite_keyword : "") << "WHERE " << (settings.hilite ? hilite_none : "");
                where()->formatImpl(settings, state, frame);
            }
            else
            {
                if (beginTime())
                {
                    settings.ostr << settings.nl_or_ws
                                  << (settings.hilite ? hilite_keyword : "") << "BEGIN " << (settings.hilite ? hilite_none : "");
                    beginTime()->formatImpl(settings, state, frame);
                }
                if (endTime())
                {
                    settings.ostr << settings.nl_or_ws
                                  << "END ";
                    endTime()->formatImpl(settings, state, frame);
                }
            }
            if (limitLength())
            {
                settings.ostr << settings.nl_or_ws
                              << (settings.hilite ? hilite_keyword : "")  << "LIMIT " << (settings.hilite ? hilite_none : "");
                limitLength()->formatImpl(settings, state, frame);
            }
            break;
    }
    if (dumpPath())
    {
        settings.ostr << settings.nl_or_ws
                      << (settings.hilite ? hilite_keyword : "") << "INTO " << (settings.hilite ? hilite_none : "");
        dumpPath()->formatImpl(settings, state, frame);
    }
}

ASTPtr & ASTDumpQuery::getExpression(Expression expr)
{
    if (!positions.count(expr))
        throw Exception("Get expression before set", ErrorCodes::LOGICAL_ERROR);
    return children[positions[expr]];
}

}
