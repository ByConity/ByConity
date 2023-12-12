#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTCreateQuery.h>
#include <Core/Defines.h>
#include <gtest/gtest.h>

using namespace DB;

namespace
{
String getEngineName(const ASTPtr & query_ptr)
{
    auto & create = query_ptr->as<ASTCreateQuery &>();
    ASTStorage * storage = create.storage;
    if (!storage)
        return "";
    ASTFunction * engine = storage->engine;
    if (!engine)
        return "";
    return engine->name;
}

TEST(convertMergeTreeToCnchEngineTest, normal_test)
{
    ParserCreateQuery create_parser;
    {
        const String create_query = "CREATE TABLE t.test (d Date, i Int32) ENGINE = MergeTree ORDER BY t PARTITION BY d";
        ASTPtr create_ast = parseQuery(create_parser, create_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        ASTPtr new_create_ast = convertMergeTreeToCnchEngine(create_ast);
        const String new_create_query = queryToString(new_create_ast);
        EXPECT_EQ(getEngineName(new_create_ast), "CnchMergeTree");
        const String expected_create_query{"CREATE TABLE t.test (`d` Date, `i` Int32) ENGINE = CnchMergeTree PARTITION BY d ORDER BY t"};
        EXPECT_EQ(new_create_query, expected_create_query);
    }

    {
        const String create_query = "CREATE TABLE t.test (d Date, i Int32) ENGINE = AggregatingMergeTree ORDER BY t PARTITION BY d";
        ASTPtr create_ast = parseQuery(create_parser, create_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        ASTPtr new_create_ast = convertMergeTreeToCnchEngine(create_ast);
        const String new_create_query = queryToString(new_create_ast);
        EXPECT_EQ(getEngineName(new_create_ast), "CnchAggregatingMergeTree");
        const String expected_create_query{"CREATE TABLE t.test (`d` Date, `i` Int32) ENGINE = CnchAggregatingMergeTree PARTITION BY d ORDER BY t"};
        EXPECT_EQ(new_create_query, expected_create_query);
    }

    {
        const String create_query = "CREATE TABLE t.test2 ENGINE = MergeTree AS SELECT * FROM t.test";
        ASTPtr create_ast = parseQuery(create_parser, create_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        ASTPtr new_create_ast = convertMergeTreeToCnchEngine(create_ast);
        const String new_create_query = queryToString(new_create_ast);
        EXPECT_EQ(getEngineName(new_create_ast), "CnchMergeTree");
        const String expected_create_query{"CREATE TABLE t.test2 ENGINE = CnchMergeTree AS SELECT * FROM t.test"};
        EXPECT_EQ(new_create_query, expected_create_query);
    }

    {
        const String create_query = "CREATE VIEW test_view_00740 AS SELECT * FROM test_00740";
        ASTPtr create_ast = parseQuery(create_parser, create_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        ASTPtr new_create_ast = convertMergeTreeToCnchEngine(create_ast);
        const String new_create_query = queryToString(new_create_ast);
        EXPECT_EQ(getEngineName(new_create_ast), "");
        EXPECT_EQ(new_create_query, create_query);
    }

    {
        const String create_query = "ATTACH TABLE test_view_00740";
        ASTPtr create_ast = parseQuery(create_parser, create_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        ASTPtr new_create_ast = convertMergeTreeToCnchEngine(create_ast);
        const String new_create_query = queryToString(new_create_ast);
        EXPECT_EQ(getEngineName(new_create_ast), "");
        EXPECT_EQ(new_create_query, create_query);
    }
}

}
