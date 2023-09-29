#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserDumpQuery.h>

namespace DB
{

bool ParserDumpQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_dump("DUMP");

    ParserKeyword s_ddl("DDL");
    ParserKeyword s_from("FROM");

    ParserKeyword s_query("QUERY");
    ParserKeyword s_ids("IDS");
    ParserKeyword s_without("WITHOUT");

    ParserKeyword s_workload("WORKLOAD");
    ParserKeyword s_on("ON");
    ParserKeyword s_cluster("CLUSTER");
    ParserKeyword s_begin("BEGIN");
    ParserKeyword s_end("END");
    ParserKeyword s_where("WHERE");
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserKeyword s_limit("LIMIT");
    ParserKeyword s_into("INTO");

    ParserList exp_list_for_databases(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma));
    ParserArrayOfLiterals array_p;
    ParserLiteral literal_p(dt);
    ParserIdentifier identifier_p;
    ParserExpressionWithOptionalAlias exp_elem(false, dt);
    ParserSelectWithUnionQuery select_p(dt);

    ASTDumpQuery::Kind kind;
    bool without_ddl = false;
    ASTPtr cluster_name;
    ASTPtr databases;
    ASTPtr query_ids;
    ASTPtr subquery;
    ASTPtr limit_length;
    ASTPtr begin_time;
    ASTPtr end_time;
    ASTPtr where_expression;
    ASTPtr dump_path;

    /// DUMP
    if (!s_dump.ignore(pos, expected))
        return false;

    /// DDL
    if (s_ddl.ignore(pos, expected))
    {
        kind = ASTDumpQuery::Kind::DDL;
        without_ddl = false;
        /// FROM db1, db2, ...
        if (s_from.ignore(pos, expected))
        {
            if (!exp_list_for_databases.parse(pos, databases, expected))
                return false;
        }
    }
    /// QUERY
    else if (s_query.ignore(pos, expected))
    {
        kind = ASTDumpQuery::Kind::Query;

        /// WITHOUT DDL
        if (s_without.ignore(pos, expected))
        {
            if (!s_ddl.ignore(pos, expected))
                return false;

            without_ddl = true;
        }

        /// IDS['query_id1', 'query_id2', ...]
        if (s_ids.ignore(pos, expected))
        {
            if (!array_p.parse(pos, query_ids, expected))
                return false;
        }
        /// subquery
        else if (!select_p.parse(pos, subquery, expected))
            return false;
    }
    /// WORKLOAD
    else if (s_workload.ignore(pos, expected))
    {
        kind = ASTDumpQuery::Kind::Workload;
        /// WITHOUT DDL
        if (s_without.ignore(pos, expected))
        {
            if (!s_ddl.ignore(pos, expected))
                return false;

            without_ddl = true;
        }

        /// ON CLUSTER cluster_name
        if (s_on.ignore(pos, expected))
        {
            if (!s_cluster.ignore(pos, expected))
                return false;

            if (!identifier_p.parse(pos, cluster_name, expected))
                return false;
        }

        /// WHERE expr
        if (s_where.ignore(pos, expected))
        {
            if (!exp_elem.parse(pos, where_expression, expected))
                return false;
        } else {
            /// BEGIN expr
            if (s_begin.ignore(pos, expected))
                if (!exp_elem.parse(pos, begin_time, expected))
                    return false;

            /// END expr
            if (s_end.ignore(pos, expected))
                if (!exp_elem.parse(pos, end_time, expected))
                    return false;
        }

        /// LIMIT length
        if (s_limit.ignore(pos, expected))
        {
            if (!literal_p.parse(pos, limit_length, expected))
                return false;
        }
    }
    else
    {
        return false;
    }

    if (s_into.ignore(pos, expected))
    {
        if (!literal_p.parse(pos, dump_path, expected))
            return false;
    }

    auto dump_query = std::make_shared<ASTDumpQuery>();
    dump_query->kind = kind;
    dump_query->without_ddl = without_ddl;
    dump_query->setExpression(ASTDumpQuery::Expression::DATABASES, std::move(databases));
    dump_query->setExpression(ASTDumpQuery::Expression::SUBQUERY, std::move(subquery));
    dump_query->setExpression(ASTDumpQuery::Expression::QUERY_IDS, std::move(query_ids));
    dump_query->setExpression(ASTDumpQuery::Expression::BEGIN_TIME, std::move(begin_time));
    dump_query->setExpression(ASTDumpQuery::Expression::END_TIME, std::move(end_time));
    dump_query->setExpression(ASTDumpQuery::Expression::WHERE, std::move(where_expression));
    dump_query->setExpression(ASTDumpQuery::Expression::LIMIT_LENGTH, std::move(limit_length));
    dump_query->setExpression(ASTDumpQuery::Expression::CLUSTER, std::move(cluster_name));
    dump_query->setExpression(ASTDumpQuery::Expression::DUMP_PATH, std::move(dump_path));

    node = dump_query;
    return true;
}

}
