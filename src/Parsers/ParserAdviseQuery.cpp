#include <Parsers/ParserAdviseQuery.h>

#include <Parsers/ASTAdviseQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{
/**
 * ADVISE
 *   [ALL | ORDER_BY | CLUSTER_BY | MATERIALIZED_VIEW | PROJECTION]
 *   [TABLES db1.* [,db2.* ,...]]
 *   [QUERIES '.../queries.sql']
 *   [OUTPUT DDL [INTO '.../optimized.sql']]
 */
bool ParserAdviseQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_advise("ADVISE");

    ParserKeyword s_all("ALL");
    ParserKeyword s_orderby("ORDER_BY");
    ParserKeyword s_clusterby("CLUSTER_BY");
    ParserKeyword s_datatype("DATA_TYPE");
    ParserKeyword s_materialized_view("MATERIALIZED_VIEW");
    ParserKeyword s_projection("PROJECTION");

    ParserKeyword s_tables("TABLES");
    ParserKeyword s_queries("QUERIES");
    ParserKeyword s_separator("SEPARATOR");

    ParserKeyword s_output("OUTPUT");
    ParserKeyword s_ddl("DDL");
    ParserKeyword s_into("INTO");

    ParserNotEmptyExpressionList exp_list(false, dt);
    ParserStringLiteral string_literal;

    ASTAdviseQuery::AdvisorType type = ASTAdviseQuery::AdvisorType::ALL;
    ASTPtr tables;
    ASTPtr queries_file;
    ASTPtr separator;
    bool output_ddl = false;
    ASTPtr optimized_file;

    if (!s_advise.ignore(pos, expected))
        return false;

    if (s_all.ignore(pos, expected))
        type = ASTAdviseQuery::AdvisorType::ALL;
    else if (s_orderby.ignore(pos, expected))
        type = ASTAdviseQuery::AdvisorType::ORDER_BY;
    else if (s_clusterby.ignore(pos, expected))
        type = ASTAdviseQuery::AdvisorType::CLUSTER_BY;
    else if (s_datatype.ignore(pos, expected))
        type = ASTAdviseQuery::AdvisorType::DATA_TYPE;
    else if (s_materialized_view.ignore(pos, expected))
        type = ASTAdviseQuery::AdvisorType::MATERIALIZED_VIEW;
    else if (s_projection.ignore(pos, expected))
        type = ASTAdviseQuery::AdvisorType::PROJECTION;

    if (s_tables.ignore(pos, expected))
        if (!exp_list.parse(pos, tables, expected))
            return false;

    if (s_queries.ignore(pos, expected))
    {
        if (!string_literal.parse(pos, queries_file, expected))
            return false;
        if (s_separator.ignore(pos, expected))
            if (!string_literal.parse(pos, separator, expected))
                return false;
    }

    if (s_output.ignore(pos, expected))
    {
        if (!s_ddl.ignore(pos, expected))
            return false;
        output_ddl = true;
        if (s_into.ignore(pos, expected))
        {
            if (!string_literal.parse(pos, optimized_file, expected))
                return false;
        }
    }

    auto query = std::make_shared<ASTAdviseQuery>();
    query->type = type;
    query->tables = tables;
    if (queries_file)
        query->queries_file = queries_file->as<ASTLiteral>()->value.get<String>();
    if (separator)
        query->separator = separator->as<ASTLiteral>()->value.get<String>();
    query->output_ddl = output_ddl;
    if (optimized_file)
        query->optimized_file = optimized_file->as<ASTLiteral>()->value.get<String>();
    node = query;
    return true;
}

}
