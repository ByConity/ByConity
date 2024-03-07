#include <Parsers/ASTPreparedStatement.h>

namespace DB
{

ASTPtr ASTCreatePreparedStatementQuery::clone() const
{
    auto res = std::make_shared<ASTCreatePreparedStatementQuery>(*this);
    res->query = query->clone();
    res->children.clear();
    res->children.push_back(res->query);
    return res;
}

void ASTCreatePreparedStatementQuery::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE " << (settings.hilite ? hilite_none : "");
    if (is_permanent)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "PERMANENT " << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_keyword : "") << "PREPARED STATEMENT " << (settings.hilite ? hilite_none : "");
    if (if_not_exists)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "IF NOT EXISTS " << (settings.hilite ? hilite_none : "");
    else if (or_replace)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "OR REPLACE " << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_identifier : "") << name << (settings.hilite ? hilite_none : "");

    formatOnCluster(settings);
    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS" << (settings.hilite ? hilite_none : "");
    if (getQuery())
    {
        settings.ostr << settings.nl_or_ws;
        getQuery()->formatImpl(settings, state, frame);
    }
}

ASTPtr ASTExecutePreparedStatementQuery::clone() const
{
    auto res = std::make_shared<ASTExecutePreparedStatementQuery>(*this);
    if (values)
        res->values = values->clone();

    cloneOutputOptions(*res);

    return res;
}


void ASTExecutePreparedStatementQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "EXECUTE PREPARED STATEMENT " << (settings.hilite ? hilite_identifier : "")
                << name << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " USING" << (settings.hilite ? hilite_none : "");

    if (getValues())
    {
        settings.ostr << settings.nl_or_ws;
        getValues()->formatImpl(settings, state, frame);
    }
}

ASTPtr ASTShowPreparedStatementQuery::clone() const
{
    auto res = std::make_shared<ASTShowPreparedStatementQuery>(*this);
    return res;
}

void ASTShowPreparedStatementQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (show_explain)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "EXPLAIN " << (settings.hilite ? hilite_none : "");
    else
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW " << (settings.hilite ? hilite_none : "");

    if (show_create)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE " << (settings.hilite ? hilite_none : "");

    if (show_explain || show_create)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "PREPARED STATEMENT " << (settings.hilite ? hilite_none : "");
        settings.ostr << (settings.hilite ? hilite_identifier : "") << name << (settings.hilite ? hilite_none : "");
    }
    else
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "PREPARED STATEMENTS" << (settings.hilite ? hilite_none : "");
}

ASTPtr ASTDropPreparedStatementQuery::clone() const
{
    auto res = std::make_shared<ASTDropPreparedStatementQuery>(*this);
    return res;
}

void ASTDropPreparedStatementQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "DROP PREPARED STATEMENT ";
    if (if_exists)
        settings.ostr << "IF EXISTS ";

    settings.ostr << (settings.hilite ? hilite_identifier : "") << name << (settings.hilite ? hilite_none : "");
    formatOnCluster(settings);
}


}
