#include <Optimizer/tests/gtest_optimizer_test_utils.h>

#include <Optimizer/ExpressionInterpreter.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ExpressionListParsers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <gtest/gtest.h>

using namespace DB;

using TestFunc = std::function<ASTPtr(const ConstASTPtr & node, ContextMutablePtr context, const NamesAndTypes & column_types)>;

void runASTTest(const String & expr_in, const String & expr_out, const TestFunc & test_func, ContextMutablePtr context)
{
    ParserExpression parser(ParserSettings::ANSI);
    auto ast = parseQuery(parser, expr_in, 0, 99999);
    NamesAndTypes column_types{
        {"unbound_uint8", std::make_shared<DataTypeUInt8>()},
        {"unbound_uint8_1", std::make_shared<DataTypeUInt8>()},
        {"unbound_uint32", std::make_shared<DataTypeUInt32>()},
        {"unbound_uint32_1", std::make_shared<DataTypeUInt32>()},
        {"unbound_uint32_2", std::make_shared<DataTypeUInt32>()},
        {"unbound_nullable_uint8", makeNullable(std::make_shared<DataTypeUInt8>())},
        {"unbound_nullable_uint32", makeNullable(std::make_shared<DataTypeUInt32>())},
        {"unbound_int8", std::make_shared<DataTypeInt8>()},
        {"unbound_int16", std::make_shared<DataTypeInt16>()},
        {"unbound_int32", std::make_shared<DataTypeInt32>()},
        {"unbound_int64", std::make_shared<DataTypeInt64>()},
        {"unbound_nullable_int16", makeNullable(std::make_shared<DataTypeInt16>())},
        {"unbound_nullable_int64", makeNullable(std::make_shared<DataTypeInt64>())},
        {"unbound_string", std::make_shared<DataTypeString>()},
        {"unbound_string_2", std::make_shared<DataTypeString>()},
        {"unbound_nullable_string", makeNullable(std::make_shared<DataTypeString>())},
        {"unbound_float32", std::make_shared<DataTypeFloat32>()},
        {"unbound_nullable_float32", makeNullable(std::make_shared<DataTypeFloat32>())},
        {"unbound_float64", std::make_shared<DataTypeFloat64>()},
        {"unbound_nullable_float64", makeNullable(std::make_shared<DataTypeFloat64>())},
        {"unbound_decimal_6_2", createDecimal<DataTypeDecimal>(6, 2)},
        {"unbound_decimal_12_5", createDecimal<DataTypeDecimal>(12, 5)},
        {"unbound_nullable_decimal_6_2", makeNullable(createDecimal<DataTypeDecimal>(6, 2))},
        {"unbound_date", std::make_shared<DataTypeDate>()},
        {"unbound_datetime", std::make_shared<DataTypeDateTime>()},
        {"unbound_array", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>())},
        {"unbound_tuple",
         std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()})},
        {"bound_uint8_0", std::make_shared<DataTypeUInt8>()},
        {"bound_uint8_1", std::make_shared<DataTypeUInt8>()},
        {"bound_nullable_uint8", makeNullable(std::make_shared<DataTypeUInt8>())},
        {"bound_uint32", std::make_shared<DataTypeUInt32>()},
        {"bound_array", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>())},
    };
    auto result = test_func(ast, context, column_types);

#ifdef DEBUG
    std::cout << "expression: `" << expr_in << "` is evaluated to `" << serializeAST(*result) << "`" << std::endl;
#endif
    if (serializeAST(*result) != expr_out)
        GTEST_FAIL() << "AST Test fails: expression: `" << expr_in << "` , expect: `" << expr_out << "` , actual: `"
                     << serializeAST(*result) << "`" << std::endl;
}

TEST(OptimizerExpressionInterpreter, DISABLED_Predicate)
{
    auto context = Context::createCopy(getContext().context);

    IdentifierResolver identifier_resolver = [&](const ASTIdentifier & node, const IDataType &) -> std::optional<Field>
    {
        if (node.name() == "bound_uint8_0")
            return UInt64(0);
        else if (node.name() == "bound_uint8_1")
            return UInt64(1);
        else if (node.name() == "bound_nullable_uint8")
            return Null();
        else if (node.name() == "bound_uint32")
            return UInt64(100);
        else if (node.name() == "bound_array")
            return Array{UInt64(1), UInt64(2), UInt64(3)};
        else
            return std::nullopt;
    };

    TestFunc test_default_optimize = [&](const ConstASTPtr & node, ContextMutablePtr ctx, const NamesAndTypes & column_types)
    {
        ExpressionInterpreterSettings settings {.identifier_resolver = ExpressionInterpreter::no_op_resolver};
        auto type_analyzer = TypeAnalyzer::create(context, column_types);
        auto res = ExpressionInterpreter::evaluate(node, ctx, type_analyzer, settings);
        if (std::holds_alternative<ASTPtr>(res.second))
            return std::get<ASTPtr>(res.second);
        else
            return LiteralEncoder::encode(std::get<Field>(res.second), res.first, ctx);
    };

    TestFunc test_identifier_resolution = [&](const ConstASTPtr & node, ContextMutablePtr ctx, const NamesAndTypes & column_types)
    {
        ExpressionInterpreterSettings settings {.identifier_resolver = identifier_resolver};
        auto type_analyzer = TypeAnalyzer::create(context, column_types);
        auto res = ExpressionInterpreter::evaluate(node, ctx, type_analyzer, settings);
        if (std::holds_alternative<ASTPtr>(res.second))
            return std::get<ASTPtr>(res.second);
        else
            return LiteralEncoder::encode(std::get<Field>(res.second), res.first, ctx);
    };

    TestFunc test_optimize_predicate = [](const ConstASTPtr & node, ContextMutablePtr ctx, const NamesAndTypes & column_types)
    {
        return ExpressionInterpreter::optimizePredicate(node, ctx, column_types);
    };

    /** constant folding **/
    runASTTest("1 + 2", "cast(3, 'UInt16')", test_default_optimize, context);
    runASTTest("1 + NULL", "NULL", test_default_optimize, context);
    runASTTest("1 + 5 * 0.6 - 2", "2.", test_default_optimize, context);
    runASTTest("-1 + (1000000 - 1000000)", "cast(-1, 'Int64')", test_default_optimize, context);
    runASTTest("toTypeName(-1 + (1000000 - 1000000))", "'Int64'", test_default_optimize, context);
    runASTTest("toDecimal32(3, 2) + toDecimal32(4.5, 4)", "'7.5000'", test_default_optimize, context);
    runASTTest("10 and (0 or not 1)", "0", test_default_optimize, context);
    runASTTest("substring(concat('Hello, ', 'World!'), 3, 8)", "'llo, Wor'", test_default_optimize, context);
    runASTTest("lower(rpad('ABC', 7, '*')) = 'abc****'", "1", test_default_optimize, context);
    runASTTest("toTimeZone(toDateTime('2020-12-31 22:00:00', 'UTC'), 'Asia/Shanghai')", "cast(1609452000, 'DateTime(\\'Asia/Shanghai\\')')", test_default_optimize, context);
    runASTTest("toYear(toDate('1994-01-01') + INTERVAL '1' YEAR)", "1995", test_default_optimize, context);
    runASTTest("CAST(geohashDecode('ypzpgxczbzur').1, 'UInt32')", "cast(99, 'UInt32')", test_default_optimize, context);
    runASTTest("has(arrayMap(x -> (x + 2), array(1,2,3)), 5)", "has(arrayMap(x -> (x + 2), [1, 2, 3]), 5)", test_default_optimize, context);
    runASTTest("multiIf(1 = 0, 'a', 1 > 0, 'b', 'c')", "'b'", test_default_optimize, context);


    /** identifier resolution **/
    runASTTest("bound_uint8_0 AND 1", "0", test_identifier_resolution, context);
    runASTTest("bound_uint8_0 + bound_uint8_1", "cast(1, 'UInt16')", test_identifier_resolution, context);
    runASTTest("bound_nullable_uint8 + 3 = 100", "cast(NULL, 'Nullable(UInt8)')", test_identifier_resolution, context);
    runASTTest("unbound_uint32 + (bound_uint32 + 200)", "unbound_uint32 + cast(300, 'UInt64')", test_identifier_resolution, context);
    runASTTest("unbound_array[bound_array[2]]", "unbound_array[cast(2, 'UInt32')]", test_identifier_resolution, context);
    runASTTest("unbound_date + INTERVAL 5 DAY", "unbound_date + toIntervalDay(5)", test_identifier_resolution, context);


    /** unevaluate functions **/
    runASTTest("unbound_uint32 IN (1, 2, 3)", "unbound_uint32 IN (1, 2, 3)", test_default_optimize, context);

    // undeterministic functions
    runASTTest("now()", "now()", test_default_optimize, context);
    runASTTest("currentDatabase()", "currentDatabase()", test_default_optimize, context);

    // agg functions
    runASTTest("sum(unbound_uint32)", "sum(unbound_uint32)", test_default_optimize, context);
    runASTTest("count(*)", "count(*)", test_default_optimize, context);

    // window functions
    runASTTest("row_number() OVER (ORDER BY unbound_uint32)", "(row_number() OVER (\n         ORDER BY unbound_uint32 ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS\n ))", test_default_optimize, context);
    runASTTest("sum(unbound_uint32) over (partition by unbound_int16 order by unbound_int16 desc)", "(sum(unbound_uint32) OVER (\n         PARTITION BY unbound_int16 ORDER BY unbound_int16 DESC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS\n ))", test_default_optimize, context);

    /** null simplify **/
    runASTTest("unbound_uint8 + NULL", "NULL", test_default_optimize, context);
    runASTTest("coalesce(NULL, unbound_uint8)", "coalesce(NULL, unbound_uint8)", test_default_optimize, context);

    /** simplify isNull & isNotNull **/
    runASTTest("unbound_uint8 IS NULL", "0", test_default_optimize, context);
    runASTTest("unbound_uint8 IS NOT NULL", "1", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 IS NULL", "isNull(unbound_nullable_uint8)", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 IS NOT NULL", "isNotNull(unbound_nullable_uint8)", test_default_optimize, context);

    /** simplify AND **/
    runASTTest("unbound_uint8 AND 0", "0", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 1", "unbound_uint8 AND 1", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 2", "unbound_uint8 AND 1", test_default_optimize, context);
    runASTTest("unbound_uint8 AND NULL", "unbound_uint8 AND NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 AND CAST(0, 'Nullable(UInt8)')", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND CAST(1, 'Nullable(UInt8)')", "unbound_uint8 AND cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND CAST(2, 'Nullable(UInt8)')", "unbound_uint8 AND cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 0 AND 1", "0", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 0 AND CAST(1, 'Nullable(UInt8)')", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 0 AND NULL", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 1 AND NULL", "unbound_uint8 AND NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 0 AND 1 AND NULL", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);

    runASTTest("unbound_nullable_uint8 AND 0", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 AND 1", "unbound_nullable_uint8 AND 1", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 AND 2", "unbound_nullable_uint8 AND 1", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 AND NULL", "unbound_nullable_uint8 AND NULL", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 AND CAST(0, 'Nullable(UInt8)')", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 AND CAST(1, 'Nullable(UInt8)')", "unbound_nullable_uint8 AND cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 AND CAST(2, 'Nullable(UInt8)')", "unbound_nullable_uint8 AND cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 AND 0 AND 1", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 AND 0 AND CAST(1, 'Nullable(UInt8)')", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 AND 0 AND NULL", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 AND 1 AND NULL", "unbound_nullable_uint8 AND NULL", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 AND 0 AND 1 AND NULL", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);

    runASTTest("unbound_uint32 AND 0", "0", test_default_optimize, context);
    runASTTest("unbound_uint32 AND 1", "unbound_uint32 AND 1", test_default_optimize, context);
    runASTTest("unbound_uint32 AND 2", "unbound_uint32 AND 1", test_default_optimize, context);
    runASTTest("unbound_uint32 AND NULL", "unbound_uint32 AND NULL", test_default_optimize, context);
    runASTTest("unbound_uint32 AND CAST(0, 'Nullable(UInt8)')", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint32 AND CAST(1, 'Nullable(UInt8)')", "unbound_uint32 AND cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint32 AND CAST(2, 'Nullable(UInt8)')", "unbound_uint32 AND cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint32 AND 0 AND 1", "0", test_default_optimize, context);
    runASTTest("unbound_uint32 AND 0 AND CAST(1, 'Nullable(UInt8)')", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint32 AND 0 AND NULL", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint32 AND 1 AND NULL", "unbound_uint32 AND NULL", test_default_optimize, context);
    runASTTest("unbound_uint32 AND 0 AND 1 AND NULL", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);

    runASTTest("unbound_uint8 AND unbound_uint8_1", "unbound_uint8 AND unbound_uint8_1", test_default_optimize, context);
    runASTTest("unbound_uint8 AND unbound_uint8_1 AND 0", "0", test_default_optimize, context);
    runASTTest("unbound_uint8 AND unbound_uint8_1 AND 1", "unbound_uint8 AND unbound_uint8_1", test_default_optimize, context);
    runASTTest("unbound_uint8 AND unbound_uint8_1 AND NULL", "unbound_uint8 AND unbound_uint8_1 AND NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 AND unbound_uint8_1 AND CAST(0, 'Nullable(UInt8)')", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND unbound_uint8_1 AND CAST(1, 'Nullable(UInt8)')", "cast(unbound_uint8 AND unbound_uint8_1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND unbound_nullable_uint8", "unbound_uint8 AND unbound_nullable_uint8", test_default_optimize, context);
    runASTTest("unbound_uint8 AND unbound_nullable_uint8 AND 0", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND unbound_nullable_uint8 AND 1", "unbound_uint8 AND unbound_nullable_uint8", test_default_optimize, context);
    runASTTest("unbound_uint8 AND unbound_nullable_uint8 AND NULL", "unbound_uint8 AND unbound_nullable_uint8 AND NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 AND unbound_nullable_uint8 AND CAST(0, 'Nullable(UInt8)')", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND unbound_nullable_uint8 AND CAST(1, 'Nullable(UInt8)')", "unbound_uint8 AND unbound_nullable_uint8", test_default_optimize, context);

    /** simplify OR **/
    runASTTest("unbound_uint8 OR 0", "unbound_uint8 OR 0", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 1", "1", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 2", "1", test_default_optimize, context);
    runASTTest("unbound_uint8 OR NULL", "unbound_uint8 OR NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 OR CAST(0, 'Nullable(UInt8)')", "unbound_uint8 OR cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR CAST(1, 'Nullable(UInt8)')", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR CAST(2, 'Nullable(UInt8)')", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 0 OR 1", "1", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 0 OR CAST(1, 'Nullable(UInt8)')", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 0 OR NULL", "unbound_uint8 OR NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 1 OR NULL", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 0 OR 1 OR NULL", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);

    runASTTest("unbound_nullable_uint8 OR 0", "unbound_nullable_uint8 OR 0", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 OR 1", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 OR 2", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 OR NULL", "unbound_nullable_uint8 OR NULL", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 OR CAST(0, 'Nullable(UInt8)')", "unbound_nullable_uint8 OR cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 OR CAST(1, 'Nullable(UInt8)')", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 OR CAST(2, 'Nullable(UInt8)')", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 OR 0 OR 1", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 OR 0 OR CAST(1, 'Nullable(UInt8)')", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 OR 0 OR NULL", "unbound_nullable_uint8 OR NULL", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 OR 1 OR NULL", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_nullable_uint8 OR 0 OR 1 OR NULL", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);

    runASTTest("unbound_uint32 OR 0", "unbound_uint32 OR 0", test_default_optimize, context);
    runASTTest("unbound_uint32 OR 1", "1", test_default_optimize, context);
    runASTTest("unbound_uint32 OR 2", "1", test_default_optimize, context);
    runASTTest("unbound_uint32 OR NULL", "unbound_uint32 OR NULL", test_default_optimize, context);
    runASTTest("unbound_uint32 OR CAST(0, 'Nullable(UInt8)')", "unbound_uint32 OR cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint32 OR CAST(1, 'Nullable(UInt8)')", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint32 OR CAST(2, 'Nullable(UInt8)')", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint32 OR 0 OR 1", "1", test_default_optimize, context);
    runASTTest("unbound_uint32 OR 0 OR CAST(1, 'Nullable(UInt8)')", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint32 OR 0 OR NULL", "unbound_uint32 OR NULL", test_default_optimize, context);
    runASTTest("unbound_uint32 OR 1 OR NULL", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint32 OR 0 OR 1 OR NULL", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);

    runASTTest("unbound_uint8 OR unbound_uint8_1", "unbound_uint8 OR unbound_uint8_1", test_default_optimize, context);
    runASTTest("unbound_uint8 OR unbound_uint8_1 OR 0", "unbound_uint8 OR unbound_uint8_1", test_default_optimize, context);
    runASTTest("unbound_uint8 OR unbound_uint8_1 OR 1", "1", test_default_optimize, context);
    runASTTest("unbound_uint8 OR unbound_uint8_1 OR NULL", "unbound_uint8 OR unbound_uint8_1 OR NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 OR unbound_uint8_1 OR CAST(0, 'Nullable(UInt8)')", "cast(unbound_uint8 OR unbound_uint8_1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR unbound_uint8_1 OR CAST(1, 'Nullable(UInt8)')", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR unbound_nullable_uint8", "unbound_uint8 OR unbound_nullable_uint8", test_default_optimize, context);
    runASTTest("unbound_uint8 OR unbound_nullable_uint8 OR 0", "unbound_uint8 OR unbound_nullable_uint8", test_default_optimize, context);
    runASTTest("unbound_uint8 OR unbound_nullable_uint8 OR 1", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR unbound_nullable_uint8 OR NULL", "unbound_uint8 OR unbound_nullable_uint8 OR NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 OR unbound_nullable_uint8 OR CAST(0, 'Nullable(UInt8)')", "unbound_uint8 OR unbound_nullable_uint8", test_default_optimize, context);
    runASTTest("unbound_uint8 OR unbound_nullable_uint8 OR CAST(1, 'Nullable(UInt8)')", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);

    /** mix AND & ORs **/
    runASTTest("unbound_uint8 AND 0 AND (unbound_uint8 OR 0)", "0", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_uint8 OR 1)", "0", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_uint8 OR NULL)", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_uint8 OR CAST(0, 'Nullable(UInt8)'))", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_uint8 OR CAST(1, 'Nullable(UInt8)'))", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_nullable_uint8 OR 0)", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_nullable_uint8 OR 1)", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_nullable_uint8 OR NULL)", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_nullable_uint8 OR CAST(0, 'Nullable(UInt8)'))", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_nullable_uint8 OR CAST(1, 'Nullable(UInt8)'))", "cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_uint8 OR 0)", "unbound_uint8 AND unbound_uint8", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_uint8 OR 1)", "unbound_uint8 AND 1", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_uint8 OR NULL)", "unbound_uint8 AND (unbound_uint8 OR NULL)", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_uint8 OR CAST(0, 'Nullable(UInt8)'))", "unbound_uint8 AND cast(unbound_uint8, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_uint8 OR CAST(1, 'Nullable(UInt8)'))", "unbound_uint8 AND cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_nullable_uint8 OR 0)", "unbound_uint8 AND unbound_nullable_uint8", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_nullable_uint8 OR 1)", "unbound_uint8 AND cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_nullable_uint8 OR NULL)", "unbound_uint8 AND (unbound_nullable_uint8 OR NULL)", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_nullable_uint8 OR CAST(0, 'Nullable(UInt8)'))", "unbound_uint8 AND unbound_nullable_uint8", test_default_optimize, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_nullable_uint8 OR CAST(1, 'Nullable(UInt8)'))", "unbound_uint8 AND cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_uint8 OR 0)", "unbound_uint8 AND unbound_uint8 AND NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_uint8 OR 1)", "unbound_uint8 AND NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_uint8 OR NULL)", "unbound_uint8 AND (unbound_uint8 OR NULL) AND NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_uint8 OR CAST(0, 'Nullable(UInt8)'))", "unbound_uint8 AND cast(unbound_uint8, 'Nullable(UInt8)') AND NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_uint8 OR CAST(1, 'Nullable(UInt8)'))", "unbound_uint8 AND NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_nullable_uint8 OR 0)", "unbound_uint8 AND unbound_nullable_uint8 AND NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_nullable_uint8 OR 1)", "unbound_uint8 AND NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_nullable_uint8 OR NULL)", "unbound_uint8 AND (unbound_nullable_uint8 OR NULL) AND NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_nullable_uint8 OR CAST(0, 'Nullable(UInt8)'))", "unbound_uint8 AND unbound_nullable_uint8 AND NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_nullable_uint8 OR CAST(1, 'Nullable(UInt8)'))", "unbound_uint8 AND NULL", test_default_optimize, context);

    runASTTest("unbound_uint8 OR 0 OR (unbound_uint8 AND 0)", "unbound_uint8 OR 0", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_uint8 AND 1)", "unbound_uint8 OR unbound_uint8", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_uint8 AND NULL)", "unbound_uint8 OR (unbound_uint8 AND NULL)", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_uint8 AND CAST(0, 'Nullable(UInt8)'))", "unbound_uint8 OR cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_uint8 AND CAST(1, 'Nullable(UInt8)'))", "unbound_uint8 OR cast(unbound_uint8, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_nullable_uint8 AND 0)", "unbound_uint8 OR cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_nullable_uint8 AND 1)", "unbound_uint8 OR unbound_nullable_uint8", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_nullable_uint8 AND NULL)", "unbound_uint8 OR (unbound_nullable_uint8 AND NULL)", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_nullable_uint8 AND CAST(0, 'Nullable(UInt8)'))", "unbound_uint8 OR cast(0, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_nullable_uint8 AND CAST(1, 'Nullable(UInt8)'))", "unbound_uint8 OR unbound_nullable_uint8", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_uint8 AND 0)", "1", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_uint8 AND 1)", "1", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_uint8 AND NULL)", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_uint8 AND CAST(0, 'Nullable(UInt8)'))", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_uint8 AND CAST(1, 'Nullable(UInt8)'))", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_nullable_uint8 AND 0)", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_nullable_uint8 AND 1)", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_nullable_uint8 AND NULL)", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_nullable_uint8 AND CAST(0, 'Nullable(UInt8)'))", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_nullable_uint8 AND CAST(1, 'Nullable(UInt8)'))", "cast(1, 'Nullable(UInt8)')", test_default_optimize, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_uint8 AND 0)", "unbound_uint8 OR NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_uint8 AND 1)", "unbound_uint8 OR unbound_uint8 OR NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_uint8 AND NULL)", "unbound_uint8 OR (unbound_uint8 AND NULL) OR NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_uint8 AND CAST(0, 'Nullable(UInt8)'))", "unbound_uint8 OR NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_uint8 AND CAST(1, 'Nullable(UInt8)'))", "unbound_uint8 OR cast(unbound_uint8, 'Nullable(UInt8)') OR NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_nullable_uint8 AND 0)", "unbound_uint8 OR NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_nullable_uint8 AND 1)", "unbound_uint8 OR unbound_nullable_uint8 OR NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_nullable_uint8 AND NULL)", "unbound_uint8 OR (unbound_nullable_uint8 AND NULL) OR NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_nullable_uint8 AND CAST(0, 'Nullable(UInt8)'))", "unbound_uint8 OR NULL", test_default_optimize, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_nullable_uint8 AND CAST(1, 'Nullable(UInt8)'))", "unbound_uint8 OR unbound_nullable_uint8 OR NULL", test_default_optimize, context);

    /** test optimize predicate **/
    runASTTest("unbound_uint8 AND 0", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 1", "unbound_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 2", "unbound_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND NULL", "unbound_uint8 AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND CAST(0, 'Nullable(UInt8)')", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND CAST(1, 'Nullable(UInt8)')", "unbound_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND CAST(2, 'Nullable(UInt8)')", "unbound_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 0 AND 1", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 0 AND CAST(1, 'Nullable(UInt8)')", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 0 AND NULL", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 1 AND NULL", "unbound_uint8 AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 0 AND 1 AND NULL", "0", test_optimize_predicate, context);

    runASTTest("unbound_nullable_uint8 AND 0", "0", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 AND 1", "unbound_nullable_uint8", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 AND 2", "unbound_nullable_uint8", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 AND NULL", "unbound_nullable_uint8 AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 AND CAST(0, 'Nullable(UInt8)')", "0", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 AND CAST(1, 'Nullable(UInt8)')", "unbound_nullable_uint8", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 AND CAST(2, 'Nullable(UInt8)')", "unbound_nullable_uint8", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 AND 0 AND 1", "0", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 AND 0 AND CAST(1, 'Nullable(UInt8)')", "0", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 AND 0 AND NULL", "0", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 AND 1 AND NULL", "unbound_nullable_uint8 AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 AND 0 AND 1 AND NULL", "0", test_optimize_predicate, context);

    runASTTest("unbound_uint32 AND 0", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint32 AND 1", "unbound_uint32", test_optimize_predicate, context);
    runASTTest("unbound_uint32 AND 2", "unbound_uint32", test_optimize_predicate, context);
    runASTTest("unbound_uint32 AND NULL", "unbound_uint32 AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint32 AND CAST(0, 'Nullable(UInt8)')", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint32 AND CAST(1, 'Nullable(UInt8)')", "unbound_uint32", test_optimize_predicate, context);
    runASTTest("unbound_uint32 AND CAST(2, 'Nullable(UInt8)')", "unbound_uint32", test_optimize_predicate, context);
    runASTTest("unbound_uint32 AND 0 AND 1", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint32 AND 0 AND CAST(1, 'Nullable(UInt8)')", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint32 AND 0 AND NULL", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint32 AND 1 AND NULL", "unbound_uint32 AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint32 AND 0 AND 1 AND NULL", "0", test_optimize_predicate, context);

    runASTTest("unbound_uint8 AND unbound_uint8_1", "unbound_uint8 AND unbound_uint8_1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND unbound_uint8_1 AND 0", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND unbound_uint8_1 AND 1", "unbound_uint8 AND unbound_uint8_1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND unbound_uint8_1 AND NULL", "unbound_uint8 AND unbound_uint8_1 AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND unbound_uint8_1 AND CAST(0, 'Nullable(UInt8)')", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND unbound_uint8_1 AND CAST(1, 'Nullable(UInt8)')", "cast(unbound_uint8 AND unbound_uint8_1, 'Nullable(UInt8)')", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND unbound_nullable_uint8", "unbound_uint8 AND unbound_nullable_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND unbound_nullable_uint8 AND 0", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND unbound_nullable_uint8 AND 1", "unbound_uint8 AND unbound_nullable_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND unbound_nullable_uint8 AND NULL", "unbound_uint8 AND unbound_nullable_uint8 AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND unbound_nullable_uint8 AND CAST(0, 'Nullable(UInt8)')", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND unbound_nullable_uint8 AND CAST(1, 'Nullable(UInt8)')", "unbound_uint8 AND unbound_nullable_uint8", test_optimize_predicate, context);

    runASTTest("unbound_uint8 OR 0", "unbound_uint8 OR 0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 1", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 2", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR NULL", "unbound_uint8 OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR CAST(0, 'Nullable(UInt8)')", "unbound_uint8 OR cast(0, 'Nullable(UInt8)')", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR CAST(1, 'Nullable(UInt8)')", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR CAST(2, 'Nullable(UInt8)')", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 0 OR 1", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 0 OR CAST(1, 'Nullable(UInt8)')", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 0 OR NULL", "unbound_uint8 OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 1 OR NULL", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 0 OR 1 OR NULL", "1", test_optimize_predicate, context);

    runASTTest("unbound_nullable_uint8 OR 0", "unbound_nullable_uint8 OR 0", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 OR 1", "1", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 OR 2", "1", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 OR NULL", "unbound_nullable_uint8 OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 OR CAST(0, 'Nullable(UInt8)')", "unbound_nullable_uint8 OR cast(0, 'Nullable(UInt8)')", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 OR CAST(1, 'Nullable(UInt8)')", "1", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 OR CAST(2, 'Nullable(UInt8)')", "1", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 OR 0 OR 1", "1", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 OR 0 OR CAST(1, 'Nullable(UInt8)')", "1", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 OR 0 OR NULL", "unbound_nullable_uint8 OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 OR 1 OR NULL", "1", test_optimize_predicate, context);
    runASTTest("unbound_nullable_uint8 OR 0 OR 1 OR NULL", "1", test_optimize_predicate, context);

    runASTTest("unbound_uint32 OR 0", "unbound_uint32 OR 0", test_optimize_predicate, context);
    runASTTest("unbound_uint32 OR 1", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint32 OR 2", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint32 OR NULL", "unbound_uint32 OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint32 OR CAST(0, 'Nullable(UInt8)')", "unbound_uint32 OR cast(0, 'Nullable(UInt8)')", test_optimize_predicate, context);
    runASTTest("unbound_uint32 OR CAST(1, 'Nullable(UInt8)')", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint32 OR CAST(2, 'Nullable(UInt8)')", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint32 OR 0 OR 1", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint32 OR 0 OR CAST(1, 'Nullable(UInt8)')", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint32 OR 0 OR NULL", "unbound_uint32 OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint32 OR 1 OR NULL", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint32 OR 0 OR 1 OR NULL", "1", test_optimize_predicate, context);

    runASTTest("unbound_uint8 OR unbound_uint8_1", "unbound_uint8 OR unbound_uint8_1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR unbound_uint8_1 OR 0", "unbound_uint8 OR unbound_uint8_1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR unbound_uint8_1 OR 1", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR unbound_uint8_1 OR NULL", "unbound_uint8 OR unbound_uint8_1 OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR unbound_uint8_1 OR CAST(0, 'Nullable(UInt8)')", "cast(unbound_uint8 OR unbound_uint8_1, 'Nullable(UInt8)')", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR unbound_uint8_1 OR CAST(1, 'Nullable(UInt8)')", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR unbound_nullable_uint8", "unbound_uint8 OR unbound_nullable_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR unbound_nullable_uint8 OR 0", "unbound_uint8 OR unbound_nullable_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR unbound_nullable_uint8 OR 1", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR unbound_nullable_uint8 OR NULL", "unbound_uint8 OR unbound_nullable_uint8 OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR unbound_nullable_uint8 OR CAST(0, 'Nullable(UInt8)')", "unbound_uint8 OR unbound_nullable_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR unbound_nullable_uint8 OR CAST(1, 'Nullable(UInt8)')", "1", test_optimize_predicate, context);

    runASTTest("unbound_uint8 AND 0 AND (unbound_uint8 OR 0)", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_uint8 OR 1)", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_uint8 OR NULL)", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_uint8 OR CAST(0, 'Nullable(UInt8)'))", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_uint8 OR CAST(1, 'Nullable(UInt8)'))", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_nullable_uint8 OR 0)", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_nullable_uint8 OR 1)", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_nullable_uint8 OR NULL)", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_nullable_uint8 OR CAST(0, 'Nullable(UInt8)'))", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 0 AND (unbound_nullable_uint8 OR CAST(1, 'Nullable(UInt8)'))", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_uint8 OR 0)", "unbound_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_uint8 OR 1)", "unbound_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_uint8 OR NULL)", "unbound_uint8 AND (unbound_uint8 OR NULL)", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_uint8 OR CAST(0, 'Nullable(UInt8)'))", "unbound_uint8 AND cast(unbound_uint8, 'Nullable(UInt8)')", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_uint8 OR CAST(1, 'Nullable(UInt8)'))", "unbound_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_nullable_uint8 OR 0)", "unbound_uint8 AND unbound_nullable_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_nullable_uint8 OR 1)", "unbound_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_nullable_uint8 OR NULL)", "unbound_uint8 AND (unbound_nullable_uint8 OR NULL)", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_nullable_uint8 OR CAST(0, 'Nullable(UInt8)'))", "unbound_uint8 AND unbound_nullable_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND 1 AND (unbound_nullable_uint8 OR CAST(1, 'Nullable(UInt8)'))", "unbound_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_uint8 OR 0)", "unbound_uint8 AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_uint8 OR 1)", "unbound_uint8 AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_uint8 OR NULL)", "unbound_uint8 AND (unbound_uint8 OR NULL) AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_uint8 OR CAST(0, 'Nullable(UInt8)'))", "unbound_uint8 AND cast(unbound_uint8, 'Nullable(UInt8)') AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_uint8 OR CAST(1, 'Nullable(UInt8)'))", "unbound_uint8 AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_nullable_uint8 OR 0)", "unbound_uint8 AND unbound_nullable_uint8 AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_nullable_uint8 OR 1)", "unbound_uint8 AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_nullable_uint8 OR NULL)", "unbound_uint8 AND (unbound_nullable_uint8 OR NULL) AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_nullable_uint8 OR CAST(0, 'Nullable(UInt8)'))", "unbound_uint8 AND unbound_nullable_uint8 AND NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND NULL AND (unbound_nullable_uint8 OR CAST(1, 'Nullable(UInt8)'))", "unbound_uint8 AND NULL", test_optimize_predicate, context);

    runASTTest("unbound_uint8 OR 0 OR (unbound_uint8 AND 0)", "unbound_uint8 OR 0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_uint8 AND 1)", "unbound_uint8 OR unbound_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_uint8 AND NULL)", "unbound_uint8 OR (unbound_uint8 AND NULL)", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_uint8 AND CAST(0, 'Nullable(UInt8)'))", "unbound_uint8 OR cast(0, 'Nullable(UInt8)')", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_uint8 AND CAST(1, 'Nullable(UInt8)'))", "unbound_uint8 OR cast(unbound_uint8, 'Nullable(UInt8)')", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_nullable_uint8 AND 0)", "unbound_uint8 OR cast(0, 'Nullable(UInt8)')", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_nullable_uint8 AND 1)", "unbound_uint8 OR unbound_nullable_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_nullable_uint8 AND NULL)", "unbound_uint8 OR (unbound_nullable_uint8 AND NULL)", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_nullable_uint8 AND CAST(0, 'Nullable(UInt8)'))", "unbound_uint8 OR cast(0, 'Nullable(UInt8)')", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 0 OR (unbound_nullable_uint8 AND CAST(1, 'Nullable(UInt8)'))", "unbound_uint8 OR unbound_nullable_uint8", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_uint8 AND 0)", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_uint8 AND 1)", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_uint8 AND NULL)", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_uint8 AND CAST(0, 'Nullable(UInt8)'))", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_uint8 AND CAST(1, 'Nullable(UInt8)'))", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_nullable_uint8 AND 0)", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_nullable_uint8 AND 1)", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_nullable_uint8 AND NULL)", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_nullable_uint8 AND CAST(0, 'Nullable(UInt8)'))", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR 1 OR (unbound_nullable_uint8 AND CAST(1, 'Nullable(UInt8)'))", "1", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_uint8 AND 0)", "unbound_uint8 OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_uint8 AND 1)", "unbound_uint8 OR unbound_uint8 OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_uint8 AND NULL)", "unbound_uint8 OR (unbound_uint8 AND NULL) OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_uint8 AND CAST(0, 'Nullable(UInt8)'))", "unbound_uint8 OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_uint8 AND CAST(1, 'Nullable(UInt8)'))", "unbound_uint8 OR cast(unbound_uint8, 'Nullable(UInt8)') OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_nullable_uint8 AND 0)", "unbound_uint8 OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_nullable_uint8 AND 1)", "unbound_uint8 OR unbound_nullable_uint8 OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_nullable_uint8 AND NULL)", "unbound_uint8 OR (unbound_nullable_uint8 AND NULL) OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_nullable_uint8 AND CAST(0, 'Nullable(UInt8)'))", "unbound_uint8 OR NULL", test_optimize_predicate, context);
    runASTTest("unbound_uint8 OR NULL OR (unbound_nullable_uint8 AND CAST(1, 'Nullable(UInt8)'))", "unbound_uint8 OR unbound_nullable_uint8 OR NULL", test_optimize_predicate, context);

    runASTTest("1 + 2", "1", test_optimize_predicate, context);
    runASTTest("1.0 - 1.0", "0", test_optimize_predicate, context);
    runASTTest("1 + 2", "1", test_optimize_predicate, context);

    /* special case for optimizePredicate */
    // see more in optimizerConjunctPredicate
    runASTTest("IF(unbound_uint8, NULL, 0)", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 AND IF(unbound_uint8, 0, 0)", "0", test_optimize_predicate, context);
    runASTTest("unbound_uint8 = unbound_uint8", "1", test_optimize_predicate, context);
    runASTTest("IF(isNull(unbound_nullable_uint8), NULL, 1)", "isNotNull(unbound_nullable_uint8)", test_optimize_predicate, context);

    /** simplify in **/
    // left side is of scalar type
    runASTTest("'foo' IN 'foo'", "1", test_default_optimize, context);
    runASTTest("'foo' IN 'bar'", "0", test_default_optimize, context);
    runASTTest("'foo' IN unbound_string", "'foo' IN (unbound_string)", test_default_optimize, context);
    runASTTest("'foo' IN ('foo')", "1", test_default_optimize, context);
    runASTTest("'foo' IN ('bar')", "0", test_default_optimize, context);
    runASTTest("'foo' IN (unbound_string)", "'foo' IN (unbound_string)", test_default_optimize, context);
    runASTTest("'foo' IN ('bar', unbound_string, 'foo')", "1", test_default_optimize, context);
    runASTTest("'foo' IN ('bar', unbound_string, 'xyz', 'abc')", "'foo' IN tuple(unbound_string)", test_default_optimize, context);
    runASTTest("'foo' IN ('bar', unbound_string, concat('b', 'ar'), concat('ba', 'r'), unbound_string, unbound_string_2)", "'foo' IN (unbound_string, unbound_string_2)", test_default_optimize, context);
    runASTTest("'foo' IN (SELECT x FROM table)", "'foo' IN (SELECT x FROM table )", test_default_optimize, context);
    runASTTest("unbound_uint8 IN 1", "unbound_uint8 IN (1)", test_default_optimize, context);
    runASTTest("unbound_uint8 IN unbound_nullable_uint8", "unbound_uint8 IN (unbound_nullable_uint8)", test_default_optimize, context);
    runASTTest("unbound_uint8 IN (1)", "unbound_uint8 IN (1)", test_default_optimize, context);
    runASTTest("unbound_uint8 IN (unbound_nullable_uint8)", "unbound_uint8 IN (unbound_nullable_uint8)", test_default_optimize, context);
    runASTTest("unbound_uint8 IN (unbound_nullable_uint8, 1, unbound_uint32)", "unbound_uint8 IN (unbound_nullable_uint8, 1, unbound_uint32)", test_default_optimize, context);
    runASTTest("unbound_uint8 IN (unbound_uint32, unbound_nullable_uint8, 1, unbound_uint32, 1 + 1, 1)", "unbound_uint8 IN (unbound_uint32, unbound_nullable_uint8, 1, cast(2, 'UInt16'))", test_default_optimize, context);
    context->setFunctionDeterministic("rand", false);
    runASTTest("unbound_uint8 IN (rand(), unbound_uint32, rand(), unbound_uint32, rand())",
               "unbound_uint8 IN (rand(), unbound_uint32, rand(), rand())", test_default_optimize, context);

    // left side is of tuple type
    runASTTest("(1, 'foo') in (1, 'foo')", "(1, 'foo') IN (1, 'foo')", test_default_optimize, context);
    runASTTest("(1, 'foo') in ((1, 'foo'))", "(1, 'foo') IN (1, 'foo')", test_default_optimize, context);
    runASTTest("(1, 'foo') in ((1, 'foo'), (2, unbound_string), (3, 'bar'))",
               "(1, 'foo') IN ((1, 'foo'), (2, unbound_string), (3, 'bar'))", test_default_optimize, context);
    runASTTest("(1, 'foo') in ((2, unbound_string), (3, 'bar'), (unbound_int8, unbound_string), (2, unbound_string), (3, 'bar'))",
               "(1, 'foo') IN ((2, unbound_string), (3, 'bar'), (unbound_int8, unbound_string), (2, unbound_string), (3, 'bar'))", test_default_optimize, context);
    runASTTest("(1, 'foo') in (SELECT x, y FROM table)", "(1, 'foo') IN (SELECT x, y FROM table )", test_default_optimize, context);
    runASTTest("(unbound_int32, 'foo') in (1, 'foo')", "(unbound_int32, 'foo') IN (1, 'foo')", test_default_optimize, context);
    runASTTest("(unbound_int32, 'foo') in ((1, 'foo'))", "(unbound_int32, 'foo') IN (1, 'foo')", test_default_optimize, context);
    runASTTest("(unbound_int32, 'foo') in ((1, 'foo'), (2, unbound_string), (3, 'bar'))",
               "(unbound_int32, 'foo') IN ((1, 'foo'), (2, unbound_string), (3, 'bar'))", test_default_optimize, context);
    runASTTest("(unbound_int32, 'foo') in ((2, unbound_string), (3, 'bar'), (unbound_int8, unbound_string), (2, unbound_string), (3, 'bar'))",
               "(unbound_int32, 'foo') IN ((2, unbound_string), (3, 'bar'), (unbound_int8, unbound_string), (2, unbound_string), (3, 'bar'))", test_default_optimize, context);
    runASTTest("(unbound_int32, 'foo') in (SELECT x, y FROM table)", "(unbound_int32, 'foo') IN (SELECT x, y FROM table )", test_default_optimize, context);
    runASTTest("(unbound_int32, unbound_string) in (1, 'foo')", "(unbound_int32, unbound_string) IN (1, 'foo')", test_default_optimize, context);
    runASTTest("(unbound_int32, unbound_string) in ((1, 'foo'))", "(unbound_int32, unbound_string) IN (1, 'foo')", test_default_optimize, context);
    runASTTest("(unbound_int32, unbound_string) in ((1, 'foo'), (2, unbound_string), (3, 'bar'))",
               "(unbound_int32, unbound_string) IN ((1, 'foo'), (2, unbound_string), (3, 'bar'))", test_default_optimize, context);
    runASTTest("(unbound_int32, unbound_string) in ((2, unbound_string), (3, 'bar'), (unbound_int8, unbound_string), (2, unbound_string), (3, 'bar'))",
               "(unbound_int32, unbound_string) IN ((2, unbound_string), (3, 'bar'), (unbound_int8, unbound_string), (2, unbound_string), (3, 'bar'))", test_default_optimize, context);
    runASTTest("(unbound_int32, unbound_string) in (SELECT x, y FROM table)",
               "(unbound_int32, unbound_string) IN (SELECT x, y FROM table )", test_default_optimize, context);

    // not in
    runASTTest("1 + 1 NOT IN 2", "0", test_default_optimize, context);
    runASTTest("1 + 1 NOT IN 3", "1", test_default_optimize, context);
    runASTTest("1 + 1 NOT IN unbound_uint32", "cast(2, 'UInt16') NOT IN (unbound_uint32)", test_default_optimize, context);
    runASTTest("1 + 1 NOT IN (2)", "0", test_default_optimize, context);
    runASTTest("1 + 1 NOT IN (3)", "1", test_default_optimize, context);
    runASTTest("1 + 1 NOT IN (unbound_uint32)", "cast(2, 'UInt16') NOT IN (unbound_uint32)", test_default_optimize, context);
    runASTTest("1 + 1 NOT IN (3, unbound_uint32, 2)", "0", test_default_optimize, context);
    runASTTest("1 + 1 NOT IN (3, unbound_uint32, 2.0)", "0", test_default_optimize, context);
    runASTTest("1 + 1 NOT IN (3, unbound_uint32, 4, 5)", "cast(2, 'UInt16') NOT IN tuple(unbound_uint32)", test_default_optimize, context);
    runASTTest("1 + 1 NOT IN (1 + 2, 2 + 1, unbound_uint32 + 1, unbound_int16 * 3, unbound_uint32 + 1, unbound_int16 / 2)",
               "cast(2, 'UInt16') NOT IN (unbound_uint32 + 1, unbound_int16 * 3, unbound_int16 / 2)", test_default_optimize, context);
    runASTTest("1 + 1 NOT IN (SELECT x FROM table)", "cast(2, 'UInt16') NOT IN (SELECT x FROM table )", test_default_optimize, context);
    runASTTest("unbound_uint8 NOT IN 1", "unbound_uint8 NOT IN (1)", test_default_optimize, context);
    runASTTest("unbound_uint8 NOT IN unbound_nullable_uint8", "unbound_uint8 NOT IN (unbound_nullable_uint8)", test_default_optimize, context);
    runASTTest("unbound_uint8 NOT IN (1)", "unbound_uint8 NOT IN (1)", test_default_optimize, context);
    runASTTest("unbound_uint8 NOT IN (unbound_nullable_uint8)", "unbound_uint8 NOT IN (unbound_nullable_uint8)", test_default_optimize, context);
    runASTTest("unbound_uint8 NOT IN (unbound_nullable_uint8, 1, unbound_uint32)",
               "unbound_uint8 NOT IN (unbound_nullable_uint8, 1, unbound_uint32)", test_default_optimize, context);
    runASTTest("unbound_uint8 NOT IN (unbound_uint32, unbound_nullable_uint8, 1, unbound_uint32, 1 + 1, 1)",
               "unbound_uint8 NOT IN (unbound_uint32, unbound_nullable_uint8, 1, cast(2, 'UInt16'))", test_default_optimize, context);

    // global in/not in
    runASTTest("'foo' GLOBAL IN ('bar', unbound_string, concat('b', 'ar'), concat('ba', 'r'), unbound_string, unbound_string_2)",
               "'foo' GLOBAL IN (unbound_string, unbound_string_2)", test_default_optimize, context);
    runASTTest("1 + 1 GLOBAL NOT IN (1 + 2, 2 + 1, unbound_uint32 + 1, unbound_int16 * 3, unbound_uint32 + 1, unbound_int16 / 2)",
               "cast(2, 'UInt16') GLOBAL NOT IN (unbound_uint32 + 1, unbound_int16 * 3, unbound_int16 / 2)", test_default_optimize, context);

    // null
    runASTTest("NULL IN (1 + 2, 2 + 1, unbound_uint32 + 1, unbound_int16 * 3, unbound_uint32 + 1, unbound_int16 / 2)",
               "NULL IN (1 + 2, 2 + 1, unbound_uint32 + 1, unbound_int16 * 3, unbound_uint32 + 1, unbound_int16 / 2)", test_default_optimize, context);
    runASTTest("NULL NOT IN (1 + 2, 2 + 1, unbound_uint32 + 1, unbound_int16 * 3, unbound_uint32 + 1, unbound_int16 / 2)",
               "NULL NOT IN (1 + 2, 2 + 1, unbound_uint32 + 1, unbound_int16 * 3, unbound_uint32 + 1, unbound_int16 / 2)", test_default_optimize, context);
    runASTTest("1 + 1 IN (3, unbound_uint32, NULL)", "cast(2, 'UInt16') IN tuple(unbound_uint32)", test_default_optimize, context);
    runASTTest("1 + 1 NOT IN (3, unbound_uint32, NULL)", "cast(2, 'UInt16') NOT IN tuple(unbound_uint32)", test_default_optimize, context);
    runASTTest("NULL IN (3, unbound_uint32, NULL)", "NULL IN (3, unbound_uint32, NULL)", test_default_optimize, context);
    runASTTest("NULL NOT IN (3, unbound_uint32, NULL)", "NULL NOT IN (3, unbound_uint32, NULL)", test_default_optimize, context);
}

void runTest(const String & expr, const String & table, ContextMutablePtr context)
{
    ParserNotEmptyExpressionList parser(false, ParserSettings::ANSI);
    auto ast = parseQuery(parser, expr, 0, 99999);
    NamesAndTypes column_types{
        {"ui8", std::make_shared<DataTypeUInt8>()},
        {"ui8_1", std::make_shared<DataTypeUInt8>()},
        {"nui8", makeNullable(std::make_shared<DataTypeUInt8>())},
        {"i32", std::make_shared<DataTypeInt32>()},
        {"ni32", makeNullable(std::make_shared<DataTypeInt32>())},
        {"f64", std::make_shared<DataTypeFloat64>()},
        {"nf64", makeNullable(std::make_shared<DataTypeFloat64>())},
        {"s", std::make_shared<DataTypeString>()},
    };

    String evaluated_expr;

    try
    {
        if (auto ast_expr = ast->as<ASTExpressionList>())
        {
            std::transform(ast_expr->children.begin(), ast_expr->children.end(), ast_expr->children.begin(), [&](auto & item) {
                ExpressionInterpreterSettings settings{.identifier_resolver = ExpressionInterpreter::no_op_resolver};
                auto type_analyzer = TypeAnalyzer::create(context, column_types);
                auto res = ExpressionInterpreter::evaluate(item, context, type_analyzer, settings);
                if (std::holds_alternative<ASTPtr>(res.second))
                    return std::get<ASTPtr>(res.second);
                else
                    return LiteralEncoder::encode(std::get<Field>(res.second), res.first, context);
            });
        }
        evaluated_expr = serializeAST(*ast);
    }
    catch (...)
    {
    }

    if (evaluated_expr.empty())
        GTEST_FAIL() << "End-to-end Test fails: failed to evaluate expression: " << expr << std::endl;

    String query_in = "select ";
    String query_out = "select ";

    query_in += expr + " from " + table;
    query_out += evaluated_expr + " from " + table;

#ifdef DEBUG
    bool log_query = true;
#else
    bool log_query = false;
#endif

    String res_in = executeTestDBQuery(query_in, context, log_query);
    String res_out = executeTestDBQuery(query_out, context, log_query);

    if (res_in != res_out)
        GTEST_FAIL() << "End-to-end Test fails for expression: `" << expr << "` , expect result: " << std::endl
                     << res_in << ", actual result: " << std::endl
                     << res_out;
}

void prepareDB(ContextMutablePtr context)
{
    tryRegisterFunctions();
    executeTestDBQuery(
        "create table test_uniq_row ("
        "  ui8 UInt8,"
        "  i32 Int32,"
        "  s String"
        ") ENGINE = Memory", context);

    executeTestDBQuery("insert into test_uniq_row values (0, 0, '')", context);

    executeTestDBQuery(
        "create table test_numbers ("
        "  ui8 UInt8,"
        "  ui8_1 UInt8,"
        "  nui8 Nullable(UInt8),"
        "  i32 Int32,"
        "  ni32 Nullable(Int32),"
        "  f64 Float64,"
        "  nf64 Nullable(Float64)"
        ") ENGINE = Memory", context);

    executeTestDBQuery(
                    "insert into test_numbers values"
                    "(0, 1, 0, 0, 0, 0, 0),"
                    "(1, 0, 1, 1, 1, 1, 1),"
                    "(2, 1, 2, 2, 2, 2, 2),"
                    "(0, 0, NULL, 0, NULL, 0, NULL)", context);
}

TEST(OptimizerExpressionInterpreter, DISABLED_EndToEndTests)
{
    auto context = Context::createCopy(getContext().context);
    prepareDB(context);

    runTest("(1 + 2.5 - 1.5 ) * 10 / 2.0, toTypeName((1 + 2.5 - 1.5 ) * 10 / 2.0)", "test_uniq_row", context);
    runTest("-1 + (1000000 - 1000000), toTypeName(-1 + (1000000 - 1000000))", "test_uniq_row", context);
    runTest("-1.5 OR 1.5 AND 100000, toTypeName(-1.5 OR 1.5 AND 100000)", "test_uniq_row", context);
    runTest("'a' = 'a', toTypeName('a' = 'a')", "test_uniq_row", context);
    runTest("10 AND (0 OR NOT 1), toTypeName(10 AND (0 OR NOT 1))", "test_uniq_row", context);
    runTest("substring(concat('Hello, ', 'World!'), 3, 8), toTypeName(substring(concat('Hello, ', 'World!'), 3, 8))", "test_uniq_row", context);
    runTest("toYear(toDate('1994-01-01') + INTERVAL '1' YEAR), toTypeName(toYear(toDate('1994-01-01') + INTERVAL '1' YEAR))", "test_uniq_row", context);
    runTest("NULL AND 1.5 AND 0, toTypeName(NULL AND 1.5 AND 0)", "test_uniq_row", context);
    runTest("NULL OR NULL OR 0, toTypeName(NULL OR NULL OR 0)", "test_uniq_row", context);
    runTest("0 IS NOT NULL, toTypeName(0 IS NOT NULL)", "test_uniq_row", context);
    runTest("nullif(NULL, toDate('1994-01-01')), toTypeName(nullif(NULL, toDate('1994-01-01')))", "test_uniq_row", context);
    runTest("NULL between toDecimal32(1,2) AND 3.2, toTypeName(NULL between toDecimal32(1,2) AND 3.2)", "test_uniq_row", context);
    runTest("coalesce(NULL, NULL), toTypeName(coalesce(NULL, NULL))", "test_uniq_row", context);
    runTest("toTimeZone(toDateTime('2020-12-31 22:00:00', 'UTC'), 'Asia/Shanghai'), toTypeName(toTimeZone(toDateTime('2020-12-31 22:00:00', 'UTC'), 'Asia/Shanghai'))", "test_uniq_row", context);
    runTest("toYear(toDate('1994-01-01') + INTERVAL '1' YEAR), toTypeName(toYear(toDate('1994-01-01') + INTERVAL '1' YEAR))", "test_uniq_row", context);
    runTest("CAST(geohashDecode('ypzpgxczbzur').1, 'UInt32'), toTypeName(CAST(geohashDecode('ypzpgxczbzur').1, 'UInt32'))", "test_uniq_row", context);
    runTest("CAST(99, 'UInt32'), toTypeName(CAST(99, 'UInt32'))", "test_uniq_row", context);
    runTest("has(arrayMap(x -> (x + 2), array(1,2,3)), 5), toTypeName(has(arrayMap(x -> (x + 2), array(1,2,3)), 5))", "test_uniq_row", context);
    runTest("'foo' IN ('foo', 'bar'), toTypeName('foo' IN ('foo', 'bar'))", "test_uniq_row", context);

    runTest("i32 + NULL, toTypeName(i32 + NULL)", "test_uniq_row", context);
    runTest("i32 AND NULL, toTypeName(i32 AND NULL)", "test_uniq_row", context);
    runTest("multiIf(NULL, 'a', nui8, 'b', 'c'), toTypeName(multiIf(NULL, 'a', nui8, 'b', 'c'))", "test_numbers", context);

    runTest("ui8 IS NULL, toTypeName(ui8 IS NULL)", "test_numbers", context);
    runTest("ui8 IS NOT NULL, toTypeName(ui8 IS NOT NULL)", "test_numbers", context);
    runTest("nui8 IS NULL, toTypeName(nui8 IS NULL)", "test_numbers", context);
    runTest("nui8 IS NOT NULL, toTypeName(nui8 IS NOT NULL)", "test_numbers", context);

    runTest("ui8 AND 0, toTypeName(ui8 AND 0)", "test_numbers", context);
    runTest("ui8 AND 1, toTypeName(ui8 AND 1)", "test_numbers", context);
    runTest("ui8 AND 2, toTypeName(ui8 AND 2)", "test_numbers", context);
    runTest("ui8 AND NULL, toTypeName(ui8 AND NULL)", "test_numbers", context);
    runTest("ui8 AND CAST(0, 'Nullable(UInt8)'), toTypeName(ui8 AND CAST(0, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("ui8 AND CAST(1, 'Nullable(UInt8)'), toTypeName(ui8 AND CAST(1, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("ui8 AND CAST(2, 'Nullable(UInt8)'), toTypeName(ui8 AND CAST(2, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("ui8 AND 0 AND 1, toTypeName(ui8 AND 0 AND 1)", "test_numbers", context);
    runTest("ui8 AND 0 AND CAST(1, 'Nullable(UInt8)'), toTypeName(ui8 AND 0 AND CAST(1, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("ui8 AND 0 AND NULL, toTypeName(ui8 AND 0 AND NULL)", "test_numbers", context);
    runTest("ui8 AND 1 AND NULL, toTypeName(ui8 AND 1 AND NULL)", "test_numbers", context);
    runTest("ui8 AND 0 AND 1 AND NULL, toTypeName(ui8 AND 0 AND 1 AND NULL)", "test_numbers", context);
    runTest("nui8 AND 0, toTypeName(nui8 AND 0)", "test_numbers", context);
    runTest("nui8 AND 1, toTypeName(nui8 AND 1)", "test_numbers", context);
    runTest("nui8 AND 2, toTypeName(nui8 AND 2)", "test_numbers", context);
    runTest("nui8 AND NULL, toTypeName(nui8 AND NULL)", "test_numbers", context);
    runTest("nui8 AND CAST(0, 'Nullable(UInt8)'), toTypeName(nui8 AND CAST(0, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("nui8 AND CAST(1, 'Nullable(UInt8)'), toTypeName(nui8 AND CAST(1, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("nui8 AND CAST(2, 'Nullable(UInt8)'), toTypeName(nui8 AND CAST(2, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("nui8 AND 0 AND 1, toTypeName(nui8 AND 0 AND 1)", "test_numbers", context);
    runTest("nui8 AND 0 AND CAST(1, 'Nullable(UInt8)'), toTypeName(nui8 AND 0 AND CAST(1, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("nui8 AND 0 AND NULL, toTypeName(nui8 AND 0 AND NULL)", "test_numbers", context);
    runTest("nui8 AND 1 AND NULL, toTypeName(nui8 AND 1 AND NULL)", "test_numbers", context);
    runTest("nui8 AND 0 AND 1 AND NULL, toTypeName(nui8 AND 0 AND 1 AND NULL)", "test_numbers", context);
    runTest("i32 AND 0, toTypeName(i32 AND 0)", "test_numbers", context);
    runTest("i32 AND 1, toTypeName(i32 AND 1)", "test_numbers", context);
    runTest("i32 AND 2, toTypeName(i32 AND 2)", "test_numbers", context);
    runTest("i32 AND NULL, toTypeName(i32 AND NULL)", "test_numbers", context);
    runTest("i32 AND CAST(0, 'Nullable(UInt8)'), toTypeName(i32 AND CAST(0, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("i32 AND CAST(1, 'Nullable(UInt8)'), toTypeName(i32 AND CAST(1, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("i32 AND CAST(2, 'Nullable(UInt8)'), toTypeName(i32 AND CAST(2, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("i32 AND 0 AND 1, toTypeName(i32 AND 0 AND 1)", "test_numbers", context);
    runTest("i32 AND 0 AND CAST(1, 'Nullable(UInt8)'), toTypeName(i32 AND 0 AND CAST(1, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("i32 AND 0 AND NULL, toTypeName(i32 AND 0 AND NULL)", "test_numbers", context);
    runTest("i32 AND 1 AND NULL, toTypeName(i32 AND 1 AND NULL)", "test_numbers", context);
    runTest("i32 AND 0 AND 1 AND NULL, toTypeName(i32 AND 0 AND 1 AND NULL)", "test_numbers", context);
    runTest("ui8 AND ui8_1, toTypeName(ui8 AND ui8_1)", "test_numbers", context);
    runTest("ui8 AND ui8_1 AND 0, toTypeName(ui8 AND ui8_1 AND 0)", "test_numbers", context);
    runTest("ui8 AND ui8_1 AND 1, toTypeName(ui8 AND ui8_1 AND 1)", "test_numbers", context);
    runTest("ui8 AND ui8_1 AND NULL, toTypeName(ui8 AND ui8_1 AND NULL)", "test_numbers", context);
    runTest("ui8 AND ui8_1 AND CAST(0, 'Nullable(UInt8)'), toTypeName(ui8 AND ui8_1 AND CAST(0, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("ui8 AND ui8_1 AND CAST(1, 'Nullable(UInt8)'), toTypeName(ui8 AND ui8_1 AND CAST(1, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("ui8 AND nui8, toTypeName(ui8 AND nui8)", "test_numbers", context);
    runTest("ui8 AND nui8 AND 0, toTypeName(ui8 AND nui8 AND 0)", "test_numbers", context);
    runTest("ui8 AND nui8 AND 1, toTypeName(ui8 AND nui8 AND 1)", "test_numbers", context);
    runTest("ui8 AND nui8 AND NULL, toTypeName(ui8 AND nui8 AND NULL)", "test_numbers", context);
    runTest("ui8 AND nui8 AND CAST(0, 'Nullable(UInt8)'), toTypeName(ui8 AND nui8 AND CAST(0, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("ui8 AND nui8 AND CAST(1, 'Nullable(UInt8)'), toTypeName(ui8 AND nui8 AND CAST(1, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("ui8 OR 0, toTypeName(ui8 OR 0)", "test_numbers", context);
    runTest("ui8 OR 1, toTypeName(ui8 OR 1)", "test_numbers", context);
    runTest("ui8 OR 2, toTypeName(ui8 OR 2)", "test_numbers", context);
    runTest("ui8 OR NULL, toTypeName(ui8 OR NULL)", "test_numbers", context);
    runTest("ui8 OR CAST(0, 'Nullable(UInt8)'), toTypeName(ui8 OR CAST(0, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("ui8 OR CAST(1, 'Nullable(UInt8)'), toTypeName(ui8 OR CAST(1, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("ui8 OR CAST(2, 'Nullable(UInt8)'), toTypeName(ui8 OR CAST(2, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("ui8 OR 0 OR 1, toTypeName(ui8 OR 0 OR 1)", "test_numbers", context);
    runTest("ui8 OR 0 OR CAST(1, 'Nullable(UInt8)'), toTypeName(ui8 OR 0 OR CAST(1, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("ui8 OR 0 OR NULL, toTypeName(ui8 OR 0 OR NULL)", "test_numbers", context);
    runTest("ui8 OR 1 OR NULL, toTypeName(ui8 OR 1 OR NULL)", "test_numbers", context);
    runTest("ui8 OR 0 OR 1 OR NULL, toTypeName(ui8 OR 0 OR 1 OR NULL)", "test_numbers", context);
    runTest("nui8 OR 0, toTypeName(nui8 OR 0)", "test_numbers", context);
    runTest("nui8 OR 1, toTypeName(nui8 OR 1)", "test_numbers", context);
    runTest("nui8 OR 2, toTypeName(nui8 OR 2)", "test_numbers", context);
    runTest("nui8 OR NULL, toTypeName(nui8 OR NULL)", "test_numbers", context);
    runTest("nui8 OR CAST(0, 'Nullable(UInt8)'), toTypeName(nui8 OR CAST(0, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("nui8 OR CAST(1, 'Nullable(UInt8)'), toTypeName(nui8 OR CAST(1, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("nui8 OR CAST(2, 'Nullable(UInt8)'), toTypeName(nui8 OR CAST(2, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("nui8 OR 0 OR 1, toTypeName(nui8 OR 0 OR 1)", "test_numbers", context);
    runTest("nui8 OR 0 OR CAST(1, 'Nullable(UInt8)'), toTypeName(nui8 OR 0 OR CAST(1, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("nui8 OR 0 OR NULL, toTypeName(nui8 OR 0 OR NULL)", "test_numbers", context);
    runTest("nui8 OR 1 OR NULL, toTypeName(nui8 OR 1 OR NULL)", "test_numbers", context);
    runTest("nui8 OR 0 OR 1 OR NULL, toTypeName(nui8 OR 0 OR 1 OR NULL)", "test_numbers", context);
    runTest("i32 OR 0, toTypeName(i32 OR 0)", "test_numbers", context);
    runTest("i32 OR 1, toTypeName(i32 OR 1)", "test_numbers", context);
    runTest("i32 OR 2, toTypeName(i32 OR 2)", "test_numbers", context);
    runTest("i32 OR NULL, toTypeName(i32 OR NULL)", "test_numbers", context);
    runTest("i32 OR CAST(0, 'Nullable(UInt8)'), toTypeName(i32 OR CAST(0, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("i32 OR CAST(1, 'Nullable(UInt8)'), toTypeName(i32 OR CAST(1, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("i32 OR CAST(2, 'Nullable(UInt8)'), toTypeName(i32 OR CAST(2, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("i32 OR 0 OR 1, toTypeName(i32 OR 0 OR 1)", "test_numbers", context);
    runTest("i32 OR 0 OR CAST(1, 'Nullable(UInt8)'), toTypeName(i32 OR 0 OR CAST(1, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("i32 OR 0 OR NULL, toTypeName(i32 OR 0 OR NULL)", "test_numbers", context);
    runTest("i32 OR 1 OR NULL, toTypeName(i32 OR 1 OR NULL)", "test_numbers", context);
    runTest("i32 OR 0 OR 1 OR NULL, toTypeName(i32 OR 0 OR 1 OR NULL)", "test_numbers", context);
    runTest("ui8 OR ui8_1, toTypeName(ui8 OR ui8_1)", "test_numbers", context);
    runTest("ui8 OR ui8_1 OR 0, toTypeName(ui8 OR ui8_1 OR 0)", "test_numbers", context);
    runTest("ui8 OR ui8_1 OR 1, toTypeName(ui8 OR ui8_1 OR 1)", "test_numbers", context);
    runTest("ui8 OR ui8_1 OR NULL, toTypeName(ui8 OR ui8_1 OR NULL)", "test_numbers", context);
    runTest("ui8 OR ui8_1 OR CAST(0, 'Nullable(UInt8)'), toTypeName(ui8 OR ui8_1 OR CAST(0, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("ui8 OR ui8_1 OR CAST(1, 'Nullable(UInt8)'), toTypeName(ui8 OR ui8_1 OR CAST(1, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("ui8 OR nui8, toTypeName(ui8 OR nui8)", "test_numbers", context);
    runTest("ui8 OR nui8 OR 0, toTypeName(ui8 OR nui8 OR 0)", "test_numbers", context);
    runTest("ui8 OR nui8 OR 1, toTypeName(ui8 OR nui8 OR 1)", "test_numbers", context);
    runTest("ui8 OR nui8 OR NULL, toTypeName(ui8 OR nui8 OR NULL)", "test_numbers", context);
    runTest("ui8 OR nui8 OR CAST(0, 'Nullable(UInt8)'), toTypeName(ui8 OR nui8 OR CAST(0, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("ui8 OR nui8 OR CAST(1, 'Nullable(UInt8)'), toTypeName(ui8 OR nui8 OR CAST(1, 'Nullable(UInt8)'))", "test_numbers", context);
    runTest("ui8 AND 0 AND (ui8 OR 0), toTypeName(ui8 AND 0 AND (ui8 OR 0))", "test_numbers", context);
    runTest("ui8 AND 0 AND (ui8 OR 1), toTypeName(ui8 AND 0 AND (ui8 OR 1))", "test_numbers", context);
    runTest("ui8 AND 0 AND (ui8 OR NULL), toTypeName(ui8 AND 0 AND (ui8 OR NULL))", "test_numbers", context);
    runTest("ui8 AND 0 AND (ui8 OR CAST(0, 'Nullable(UInt8)')), toTypeName(ui8 AND 0 AND (ui8 OR CAST(0, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 AND 0 AND (ui8 OR CAST(1, 'Nullable(UInt8)')), toTypeName(ui8 AND 0 AND (ui8 OR CAST(1, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 AND 0 AND (nui8 OR 0), toTypeName(ui8 AND 0 AND (nui8 OR 0))", "test_numbers", context);
    runTest("ui8 AND 0 AND (nui8 OR 1), toTypeName(ui8 AND 0 AND (nui8 OR 1))", "test_numbers", context);
    runTest("ui8 AND 0 AND (nui8 OR NULL), toTypeName(ui8 AND 0 AND (nui8 OR NULL))", "test_numbers", context);
    runTest("ui8 AND 0 AND (nui8 OR CAST(0, 'Nullable(UInt8)')), toTypeName(ui8 AND 0 AND (nui8 OR CAST(0, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 AND 0 AND (nui8 OR CAST(1, 'Nullable(UInt8)')), toTypeName(ui8 AND 0 AND (nui8 OR CAST(1, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 AND 1 AND (ui8 OR 0), toTypeName(ui8 AND 1 AND (ui8 OR 0))", "test_numbers", context);
    runTest("ui8 AND 1 AND (ui8 OR 1), toTypeName(ui8 AND 1 AND (ui8 OR 1))", "test_numbers", context);
    runTest("ui8 AND 1 AND (ui8 OR NULL), toTypeName(ui8 AND 1 AND (ui8 OR NULL))", "test_numbers", context);
    runTest("ui8 AND 1 AND (ui8 OR CAST(0, 'Nullable(UInt8)')), toTypeName(ui8 AND 1 AND (ui8 OR CAST(0, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 AND 1 AND (ui8 OR CAST(1, 'Nullable(UInt8)')), toTypeName(ui8 AND 1 AND (ui8 OR CAST(1, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 AND 1 AND (nui8 OR 0), toTypeName(ui8 AND 1 AND (nui8 OR 0))", "test_numbers", context);
    runTest("ui8 AND 1 AND (nui8 OR 1), toTypeName(ui8 AND 1 AND (nui8 OR 1))", "test_numbers", context);
    runTest("ui8 AND 1 AND (nui8 OR NULL), toTypeName(ui8 AND 1 AND (nui8 OR NULL))", "test_numbers", context);
    runTest("ui8 AND 1 AND (nui8 OR CAST(0, 'Nullable(UInt8)')), toTypeName(ui8 AND 1 AND (nui8 OR CAST(0, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 AND 1 AND (nui8 OR CAST(1, 'Nullable(UInt8)')), toTypeName(ui8 AND 1 AND (nui8 OR CAST(1, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 AND NULL AND (ui8 OR 0), toTypeName(ui8 AND NULL AND (ui8 OR 0))", "test_numbers", context);
    runTest("ui8 AND NULL AND (ui8 OR 1), toTypeName(ui8 AND NULL AND (ui8 OR 1))", "test_numbers", context);
    runTest("ui8 AND NULL AND (ui8 OR NULL), toTypeName(ui8 AND NULL AND (ui8 OR NULL))", "test_numbers", context);
    runTest("ui8 AND NULL AND (ui8 OR CAST(0, 'Nullable(UInt8)')), toTypeName(ui8 AND NULL AND (ui8 OR CAST(0, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 AND NULL AND (ui8 OR CAST(1, 'Nullable(UInt8)')), toTypeName(ui8 AND NULL AND (ui8 OR CAST(1, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 AND NULL AND (nui8 OR 0), toTypeName(ui8 AND NULL AND (nui8 OR 0))", "test_numbers", context);
    runTest("ui8 AND NULL AND (nui8 OR 1), toTypeName(ui8 AND NULL AND (nui8 OR 1))", "test_numbers", context);
    runTest("ui8 AND NULL AND (nui8 OR NULL), toTypeName(ui8 AND NULL AND (nui8 OR NULL))", "test_numbers", context);
    runTest("ui8 AND NULL AND (nui8 OR CAST(0, 'Nullable(UInt8)')), toTypeName(ui8 AND NULL AND (nui8 OR CAST(0, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 AND NULL AND (nui8 OR CAST(1, 'Nullable(UInt8)')), toTypeName(ui8 AND NULL AND (nui8 OR CAST(1, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 OR 0 OR (ui8 AND 0), toTypeName(ui8 OR 0 OR (ui8 AND 0))", "test_numbers", context);
    runTest("ui8 OR 0 OR (ui8 AND 1), toTypeName(ui8 OR 0 OR (ui8 AND 1))", "test_numbers", context);
    runTest("ui8 OR 0 OR (ui8 AND NULL), toTypeName(ui8 OR 0 OR (ui8 AND NULL))", "test_numbers", context);
    runTest("ui8 OR 0 OR (ui8 AND CAST(0, 'Nullable(UInt8)')), toTypeName(ui8 OR 0 OR (ui8 AND CAST(0, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 OR 0 OR (ui8 AND CAST(1, 'Nullable(UInt8)')), toTypeName(ui8 OR 0 OR (ui8 AND CAST(1, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 OR 0 OR (nui8 AND 0), toTypeName(ui8 OR 0 OR (nui8 AND 0))", "test_numbers", context);
    runTest("ui8 OR 0 OR (nui8 AND 1), toTypeName(ui8 OR 0 OR (nui8 AND 1))", "test_numbers", context);
    runTest("ui8 OR 0 OR (nui8 AND NULL), toTypeName(ui8 OR 0 OR (nui8 AND NULL))", "test_numbers", context);
    runTest("ui8 OR 0 OR (nui8 AND CAST(0, 'Nullable(UInt8)')), toTypeName(ui8 OR 0 OR (nui8 AND CAST(0, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 OR 0 OR (nui8 AND CAST(1, 'Nullable(UInt8)')), toTypeName(ui8 OR 0 OR (nui8 AND CAST(1, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 OR 1 OR (ui8 AND 0), toTypeName(ui8 OR 1 OR (ui8 AND 0))", "test_numbers", context);
    runTest("ui8 OR 1 OR (ui8 AND 1), toTypeName(ui8 OR 1 OR (ui8 AND 1))", "test_numbers", context);
    runTest("ui8 OR 1 OR (ui8 AND NULL), toTypeName(ui8 OR 1 OR (ui8 AND NULL))", "test_numbers", context);
    runTest("ui8 OR 1 OR (ui8 AND CAST(0, 'Nullable(UInt8)')), toTypeName(ui8 OR 1 OR (ui8 AND CAST(0, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 OR 1 OR (ui8 AND CAST(1, 'Nullable(UInt8)')), toTypeName(ui8 OR 1 OR (ui8 AND CAST(1, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 OR 1 OR (nui8 AND 0), toTypeName(ui8 OR 1 OR (nui8 AND 0))", "test_numbers", context);
    runTest("ui8 OR 1 OR (nui8 AND 1), toTypeName(ui8 OR 1 OR (nui8 AND 1))", "test_numbers", context);
    runTest("ui8 OR 1 OR (nui8 AND NULL), toTypeName(ui8 OR 1 OR (nui8 AND NULL))", "test_numbers", context);
    runTest("ui8 OR 1 OR (nui8 AND CAST(0, 'Nullable(UInt8)')), toTypeName(ui8 OR 1 OR (nui8 AND CAST(0, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 OR 1 OR (nui8 AND CAST(1, 'Nullable(UInt8)')), toTypeName(ui8 OR 1 OR (nui8 AND CAST(1, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 OR NULL OR (ui8 AND 0), toTypeName(ui8 OR NULL OR (ui8 AND 0))", "test_numbers", context);
    runTest("ui8 OR NULL OR (ui8 AND 1), toTypeName(ui8 OR NULL OR (ui8 AND 1))", "test_numbers", context);
    runTest("ui8 OR NULL OR (ui8 AND NULL), toTypeName(ui8 OR NULL OR (ui8 AND NULL))", "test_numbers", context);
    runTest("ui8 OR NULL OR (ui8 AND CAST(0, 'Nullable(UInt8)')), toTypeName(ui8 OR NULL OR (ui8 AND CAST(0, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 OR NULL OR (ui8 AND CAST(1, 'Nullable(UInt8)')), toTypeName(ui8 OR NULL OR (ui8 AND CAST(1, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 OR NULL OR (nui8 AND 0), toTypeName(ui8 OR NULL OR (nui8 AND 0))", "test_numbers", context);
    runTest("ui8 OR NULL OR (nui8 AND 1), toTypeName(ui8 OR NULL OR (nui8 AND 1))", "test_numbers", context);
    runTest("ui8 OR NULL OR (nui8 AND NULL), toTypeName(ui8 OR NULL OR (nui8 AND NULL))", "test_numbers", context);
    runTest("ui8 OR NULL OR (nui8 AND CAST(0, 'Nullable(UInt8)')), toTypeName(ui8 OR NULL OR (nui8 AND CAST(0, 'Nullable(UInt8)')))", "test_numbers", context);
    runTest("ui8 OR NULL OR (nui8 AND CAST(1, 'Nullable(UInt8)')), toTypeName(ui8 OR NULL OR (nui8 AND CAST(1, 'Nullable(UInt8)')))", "test_numbers", context);

    runTest("'foo' in ('foo', 'bar', 'xyz'), toTypeName('foo' in ('foo', 'bar', 'xyz'))", "test_uniq_row", context);
    runTest("'foo' not in ('foo', 'bar', 'xyz'), toTypeName('foo' not in ('foo', 'bar', 'xyz'))", "test_uniq_row", context);
    runTest("ui8 in (0, 0, 0, 0), toTypeName(ui8 in (0, 0, 0, 0))", "test_uniq_row", context);
    runTest("ui8 not in (0, 0, 0, 0), toTypeName(ui8 not in (0, 0, 0, 0))", "test_uniq_row", context);
    runTest("ui8 in (1, 1, 1, 1), toTypeName(ui8 in (1, 1, 1, 1))", "test_uniq_row", context);
    runTest("ui8 not in (1, 1, 1, 1), toTypeName(ui8 not in (1, 1, 1, 1))", "test_uniq_row", context);

    runTest("ui8 in 1, toTypeName(ui8 in 1)", "test_numbers", context);
    runTest("ui8 in NULL, toTypeName(ui8 in NULL)", "test_numbers", context);
    runTest("ui8 in (NULL), toTypeName(ui8 in (NULL))", "test_numbers", context);
    runTest("ui8 in (1), toTypeName(ui8 in (1))", "test_numbers", context);
    runTest("ui8 in (1, 2), toTypeName(ui8 in (1, 2))", "test_numbers", context);
    runTest("ui8 in (1, 1), toTypeName(ui8 in (1, 1))", "test_numbers", context);
    runTest("ui8 in (1, NULL), toTypeName(ui8 in (1, NULL))", "test_numbers", context);
    runTest("nui8 in 1, toTypeName(nui8 in 1)", "test_numbers", context);
    runTest("nui8 in NULL, toTypeName(nui8 in NULL)", "test_numbers", context);
    runTest("nui8 in (NULL), toTypeName(nui8 in (NULL))", "test_numbers", context);
    runTest("nui8 in (1), toTypeName(nui8 in (1))", "test_numbers", context);
    runTest("nui8 in (1, 2), toTypeName(nui8 in (1, 2))", "test_numbers", context);
    runTest("nui8 in (1, 1), toTypeName(nui8 in (1, 1))", "test_numbers", context);
    runTest("nui8 in (1, NULL), toTypeName(nui8 in (1, NULL))", "test_numbers", context);
    runTest("ui8 not in 1, toTypeName(ui8 not in 1)", "test_numbers", context);
    runTest("ui8 not in NULL, toTypeName(ui8 not in NULL)", "test_numbers", context);
    runTest("ui8 not in (NULL), toTypeName(ui8 not in (NULL))", "test_numbers", context);
    runTest("ui8 not in (1), toTypeName(ui8 not in (1))", "test_numbers", context);
    runTest("ui8 not in (1, 2), toTypeName(ui8 not in (1, 2))", "test_numbers", context);
    runTest("ui8 not in (1, 1), toTypeName(ui8 not in (1, 1))", "test_numbers", context);
    runTest("ui8 not in (1, NULL), toTypeName(ui8 not in (1, NULL))", "test_numbers", context);
    runTest("nui8 not in 1, toTypeName(nui8 not in 1)", "test_numbers", context);
    runTest("nui8 not in NULL, toTypeName(nui8 not in NULL)", "test_numbers", context);
    runTest("nui8 not in (NULL), toTypeName(nui8 not in (NULL))", "test_numbers", context);
    runTest("nui8 not in (1), toTypeName(nui8 not in (1))", "test_numbers", context);
    runTest("nui8 not in (1, 2), toTypeName(nui8 not in (1, 2))", "test_numbers", context);
    runTest("nui8 not in (1, 1), toTypeName(nui8 not in (1, 1))", "test_numbers", context);
    runTest("nui8 not in (1, NULL), toTypeName(nui8 not in (1, NULL))", "test_numbers", context);

    runTest("i32 in 1, toTypeName(i32 in 1)", "test_numbers", context);
    runTest("i32 in NULL, toTypeName(i32 in NULL)", "test_numbers", context);
    runTest("i32 in (NULL), toTypeName(i32 in (NULL))", "test_numbers", context);
    runTest("i32 in (1), toTypeName(i32 in (1))", "test_numbers", context);
    runTest("i32 in (1, 2), toTypeName(i32 in (1, 2))", "test_numbers", context);
    runTest("i32 in (1, 1), toTypeName(i32 in (1, 1))", "test_numbers", context);
    runTest("i32 in (1, NULL), toTypeName(i32 in (1, NULL))", "test_numbers", context);
    runTest("ni32 in 1, toTypeName(ni32 in 1)", "test_numbers", context);
    runTest("ni32 in NULL, toTypeName(ni32 in NULL)", "test_numbers", context);
    runTest("ni32 in (NULL), toTypeName(ni32 in (NULL))", "test_numbers", context);
    runTest("ni32 in (1), toTypeName(ni32 in (1))", "test_numbers", context);
    runTest("ni32 in (1, 2), toTypeName(ni32 in (1, 2))", "test_numbers", context);
    runTest("ni32 in (1, 1), toTypeName(ni32 in (1, 1))", "test_numbers", context);
    runTest("ni32 in (1, NULL), toTypeName(ni32 in (1, NULL))", "test_numbers", context);
    runTest("i32 not in 1, toTypeName(i32 not in 1)", "test_numbers", context);
    runTest("i32 not in NULL, toTypeName(i32 not in NULL)", "test_numbers", context);
    runTest("i32 not in (NULL), toTypeName(i32 not in (NULL))", "test_numbers", context);
    runTest("i32 not in (1), toTypeName(i32 not in (1))", "test_numbers", context);
    runTest("i32 not in (1, 2), toTypeName(i32 not in (1, 2))", "test_numbers", context);
    runTest("i32 not in (1, 1), toTypeName(i32 not in (1, 1))", "test_numbers", context);
    runTest("i32 not in (1, NULL), toTypeName(i32 not in (1, NULL))", "test_numbers", context);
    runTest("ni32 not in 1, toTypeName(ni32 not in 1)", "test_numbers", context);
    runTest("ni32 not in NULL, toTypeName(ni32 not in NULL)", "test_numbers", context);
    runTest("ni32 not in (NULL), toTypeName(ni32 not in (NULL))", "test_numbers", context);
    runTest("ni32 not in (1), toTypeName(ni32 not in (1))", "test_numbers", context);
    runTest("ni32 not in (1, 2), toTypeName(ni32 not in (1, 2))", "test_numbers", context);
    runTest("ni32 not in (1, 1), toTypeName(ni32 not in (1, 1))", "test_numbers", context);
    runTest("ni32 not in (1, NULL), toTypeName(ni32 not in (1, NULL))", "test_numbers", context);

    runTest("1 in 0, toTypeName(1 in 0)", "test_uniq_row", context);
    runTest("1 in 1, toTypeName(1 in 1)", "test_uniq_row", context);
    runTest("1 in NULL, toTypeName(1 in NULL)", "test_uniq_row", context);
    runTest("1 in (NULL), toTypeName(1 in (NULL))", "test_uniq_row", context);
    runTest("1 in (0), toTypeName(1 in (0))", "test_uniq_row", context);
    runTest("1 in (1), toTypeName(1 in (1))", "test_uniq_row", context);
    runTest("1 in (0, 1), toTypeName(1 in (0, 1))", "test_uniq_row", context);
    runTest("1 in (0, 2), toTypeName(1 in (0, 2))", "test_uniq_row", context);
    runTest("1 in (0, 0), toTypeName(1 in (0, 0))", "test_uniq_row", context);
    runTest("1 in (1, 1), toTypeName(1 in (1, 1))", "test_uniq_row", context);
    runTest("1 in (0, NULL), toTypeName(1 in (0, NULL))", "test_uniq_row", context);
    runTest("1 in (1, NULL), toTypeName(1 in (1, NULL))", "test_uniq_row", context);
    runTest("1 not in 0, toTypeName(1 not in 0)", "test_uniq_row", context);
    runTest("1 not in 1, toTypeName(1 not in 1)", "test_uniq_row", context);
    runTest("1 not in NULL, toTypeName(1 not in NULL)", "test_uniq_row", context);
    runTest("1 not in (NULL), toTypeName(1 not in (NULL))", "test_uniq_row", context);
    runTest("1 not in (0), toTypeName(1 not in (0))", "test_uniq_row", context);
    runTest("1 not in (1), toTypeName(1 not in (1))", "test_uniq_row", context);
    runTest("1 not in (0, 1), toTypeName(1 not in (0, 1))", "test_uniq_row", context);
    runTest("1 not in (0, 2), toTypeName(1 not in (0, 2))", "test_uniq_row", context);
    runTest("1 not in (0, 0), toTypeName(1 not in (0, 0))", "test_uniq_row", context);
    runTest("1 not in (1, 1), toTypeName(1 not in (1, 1))", "test_uniq_row", context);
    runTest("1 not in (0, NULL), toTypeName(1 not in (0, NULL))", "test_uniq_row", context);
    runTest("1 not in (1, NULL), toTypeName(1 not in (1, NULL))", "test_uniq_row", context);

    runTest("CAST(1, 'Nullable(UInt8)') in 0, toTypeName(CAST(1, 'Nullable(UInt8)') in 0)", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') in 1, toTypeName(CAST(1, 'Nullable(UInt8)') in 1)", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') in NULL, toTypeName(CAST(1, 'Nullable(UInt8)') in NULL)", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') in (NULL), toTypeName(CAST(1, 'Nullable(UInt8)') in (NULL))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') in (0), toTypeName(CAST(1, 'Nullable(UInt8)') in (0))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') in (1), toTypeName(CAST(1, 'Nullable(UInt8)') in (1))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') in (0, 1), toTypeName(CAST(1, 'Nullable(UInt8)') in (0, 1))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') in (0, 2), toTypeName(CAST(1, 'Nullable(UInt8)') in (0, 2))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') in (0, 0), toTypeName(CAST(1, 'Nullable(UInt8)') in (0, 0))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') in (1, 1), toTypeName(CAST(1, 'Nullable(UInt8)') in (1, 1))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') in (0, NULL), toTypeName(CAST(1, 'Nullable(UInt8)') in (0, NULL))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') in (1, NULL), toTypeName(CAST(1, 'Nullable(UInt8)') in (1, NULL))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') not in 0, toTypeName(CAST(1, 'Nullable(UInt8)') not in 0)", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') not in 1, toTypeName(CAST(1, 'Nullable(UInt8)') not in 1)", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') not in NULL, toTypeName(CAST(1, 'Nullable(UInt8)') not in NULL)", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') not in (NULL), toTypeName(CAST(1, 'Nullable(UInt8)') not in (NULL))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') not in (0), toTypeName(CAST(1, 'Nullable(UInt8)') not in (0))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') not in (1), toTypeName(CAST(1, 'Nullable(UInt8)') not in (1))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') not in (0, 1), toTypeName(CAST(1, 'Nullable(UInt8)') not in (0, 1))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') not in (0, 2), toTypeName(CAST(1, 'Nullable(UInt8)') not in (0, 2))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') not in (0, 0), toTypeName(CAST(1, 'Nullable(UInt8)') not in (0, 0))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') not in (1, 1), toTypeName(CAST(1, 'Nullable(UInt8)') not in (1, 1))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') not in (0, NULL), toTypeName(CAST(1, 'Nullable(UInt8)') not in (0, NULL))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(UInt8)') not in (1, NULL), toTypeName(CAST(1, 'Nullable(UInt8)') not in (1, NULL))", "test_uniq_row", context);

    runTest("CAST(1, 'Int32') in 0, toTypeName(CAST(1, 'Int32') in 0)", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') in 1, toTypeName(CAST(1, 'Int32') in 1)", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') in NULL, toTypeName(CAST(1, 'Int32') in NULL)", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') in (NULL), toTypeName(CAST(1, 'Int32') in (NULL))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') in (0), toTypeName(CAST(1, 'Int32') in (0))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') in (1), toTypeName(CAST(1, 'Int32') in (1))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') in (0, 1), toTypeName(CAST(1, 'Int32') in (0, 1))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') in (0, 2), toTypeName(CAST(1, 'Int32') in (0, 2))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') in (0, 0), toTypeName(CAST(1, 'Int32') in (0, 0))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') in (1, 1), toTypeName(CAST(1, 'Int32') in (1, 1))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') in (0, NULL), toTypeName(CAST(1, 'Int32') in (0, NULL))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') in (1, NULL), toTypeName(CAST(1, 'Int32') in (1, NULL))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') not in 0, toTypeName(CAST(1, 'Int32') not in 0)", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') not in 1, toTypeName(CAST(1, 'Int32') not in 1)", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') not in NULL, toTypeName(CAST(1, 'Int32') not in NULL)", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') not in (NULL), toTypeName(CAST(1, 'Int32') not in (NULL))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') not in (0), toTypeName(CAST(1, 'Int32') not in (0))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') not in (1), toTypeName(CAST(1, 'Int32') not in (1))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') not in (0, 1), toTypeName(CAST(1, 'Int32') not in (0, 1))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') not in (0, 2), toTypeName(CAST(1, 'Int32') not in (0, 2))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') not in (0, 0), toTypeName(CAST(1, 'Int32') not in (0, 0))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') not in (1, 1), toTypeName(CAST(1, 'Int32') not in (1, 1))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') not in (0, NULL), toTypeName(CAST(1, 'Int32') not in (0, NULL))", "test_uniq_row", context);
    runTest("CAST(1, 'Int32') not in (1, NULL), toTypeName(CAST(1, 'Int32') not in (1, NULL))", "test_uniq_row", context);

    runTest("CAST(1, 'Nullable(Int32)') in 0, toTypeName(CAST(1, 'Nullable(Int32)') in 0)", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') in 1, toTypeName(CAST(1, 'Nullable(Int32)') in 1)", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') in NULL, toTypeName(CAST(1, 'Nullable(Int32)') in NULL)", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') in (NULL), toTypeName(CAST(1, 'Nullable(Int32)') in (NULL))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') in (0), toTypeName(CAST(1, 'Nullable(Int32)') in (0))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') in (1), toTypeName(CAST(1, 'Nullable(Int32)') in (1))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') in (0, 1), toTypeName(CAST(1, 'Nullable(Int32)') in (0, 1))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') in (0, 2), toTypeName(CAST(1, 'Nullable(Int32)') in (0, 2))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') in (0, 0), toTypeName(CAST(1, 'Nullable(Int32)') in (0, 0))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') in (1, 1), toTypeName(CAST(1, 'Nullable(Int32)') in (1, 1))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') in (0, NULL), toTypeName(CAST(1, 'Nullable(Int32)') in (0, NULL))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') in (1, NULL), toTypeName(CAST(1, 'Nullable(Int32)') in (1, NULL))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') not in 0, toTypeName(CAST(1, 'Nullable(Int32)') not in 0)", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') not in 1, toTypeName(CAST(1, 'Nullable(Int32)') not in 1)", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') not in NULL, toTypeName(CAST(1, 'Nullable(Int32)') not in NULL)", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') not in (NULL), toTypeName(CAST(1, 'Nullable(Int32)') not in (NULL))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') not in (0), toTypeName(CAST(1, 'Nullable(Int32)') not in (0))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') not in (1), toTypeName(CAST(1, 'Nullable(Int32)') not in (1))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') not in (0, 1), toTypeName(CAST(1, 'Nullable(Int32)') not in (0, 1))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') not in (0, 2), toTypeName(CAST(1, 'Nullable(Int32)') not in (0, 2))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') not in (0, 0), toTypeName(CAST(1, 'Nullable(Int32)') not in (0, 0))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') not in (1, 1), toTypeName(CAST(1, 'Nullable(Int32)') not in (1, 1))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') not in (0, NULL), toTypeName(CAST(1, 'Nullable(Int32)') not in (0, NULL))", "test_uniq_row", context);
    runTest("CAST(1, 'Nullable(Int32)') not in (1, NULL), toTypeName(CAST(1, 'Nullable(Int32)') not in (1, NULL))", "test_uniq_row", context);

    runTest("NULL in NULL, toTypeName(NULL in NULL)", "test_uniq_row", context);
    runTest("NULL in (NULL), toTypeName(NULL in (NULL))", "test_uniq_row", context);
//    runTest("NULL in (1, 2), toTypeName(NULL in (1, 2))", "test_uniq_row", context);
//    runTest("NULL in (1, NULL), toTypeName(NULL in (1, NULL))", "test_uniq_row", context);
    runTest("NULL not in NULL, toTypeName(NULL not in NULL)", "test_uniq_row", context);
    runTest("NULL not in (NULL), toTypeName(NULL not in (NULL))", "test_uniq_row", context);
//    runTest("NULL not in (1, 2), toTypeName(NULL not in (1, 2))", "test_uniq_row", context);
//    runTest("NULL not in (1, NULL), toTypeName(NULL not in (1, NULL))", "test_uniq_row", context);

}

