/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Optimizer/tests/gtest_base_plan_test.h>

#include <Analyzers/QueryAnalyzer.h>
#include <Analyzers/QueryRewriter.h>
#include <Databases/DatabaseMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/DistributedStages/PlanSegmentSplitter.h>
#include <Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Interpreters/InterpreterShowStatsQuery.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Interpreters/executeQuery.h>
#include <Optimizer/CardinalityEstimate/CardinalityEstimator.h>
#include <Optimizer/Dump/PlanReproducer.h>
#include <Optimizer/PlanOptimizer.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <QueryPlan/PlanPrinter.h>
#include <QueryPlan/QueryPlanner.h>
#include <Statistics/CacheManager.h>
#include <Statistics/CatalogAdaptor.h>
#include <Poco/NumberParser.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <fstream>
#include <iostream>

using namespace std::string_literals;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

BasePlanTest::BasePlanTest(const String & database_name_, const std::unordered_map<String, Field> & session_settings)
    : database_name(database_name_), session_context(Context::createCopy(getContext().context))
{
    tryRegisterFunctions();
    tryRegisterFormats();
    tryRegisterStorages();
    tryRegisterAggregateFunctions();
    tryRegisterHints();

    SettingsChanges setting_changes;

    setting_changes.emplace_back("enable_optimizer", true);
    setting_changes.emplace_back("enable_memory_catalog", true);
    setting_changes.emplace_back("dialect_type", "ANSI"s);

    for (const auto & item : session_settings)
        setting_changes.emplace_back(item.first, item.second);

    session_context->applySettingsChanges(setting_changes);

    if (DatabaseCatalog::instance().tryGetDatabase(database_name, session_context))
        DatabaseCatalog::instance().detachDatabase(session_context, database_name, true, false);

    auto database = std::make_shared<DatabaseMemory>(database_name, session_context);
    DatabaseCatalog::instance().attachDatabase(database_name, database);
    session_context->setCurrentDatabase(database_name);
}

ASTPtr BasePlanTest::parse(const std::string & query, ContextMutablePtr query_context)
{
    const char * begin = query.data();
    const char * end = begin + query.size();

    ParserQuery parser(end, ParserSettings::ANSI);
    auto ast = parseQuery(
        parser, begin, end, "", query_context->getSettingsRef().max_query_size, query_context->getSettingsRef().max_parser_depth);
    return ast;
}

QueryPlanPtr BasePlanTest::plan(const String & query, ContextMutablePtr query_context)
{
    auto ast = parse(query, query_context);

    {
        SelectIntersectExceptQueryVisitor::Data data{
            query_context->getSettingsRef().intersect_default_mode, query_context->getSettingsRef().except_default_mode};
        SelectIntersectExceptQueryVisitor{data}.visit(ast);
    }

    /// Normalize SelectWithUnionQuery
    NormalizeSelectWithUnionQueryVisitor::Data data{query_context->getSettingsRef().union_default_mode};
    NormalizeSelectWithUnionQueryVisitor{data}.visit(ast);

    ast = QueryRewriter().rewrite(ast, query_context);
    AnalysisPtr analysis = QueryAnalyzer::analyze(ast, query_context);
    QueryPlanPtr query_plan = QueryPlanner().plan(ast, *analysis, query_context);
    PlanOptimizer::optimize(*query_plan, query_context);
    return query_plan;
}

PlanSegmentTreePtr BasePlanTest::planSegment(const String & query, ContextMutablePtr query_context)
{
    auto query_plan = plan(query, query_context);

    QueryPlan plan = PlanNodeToNodeVisitor::convert(*query_plan);

    PlanSegmentTreePtr plan_segment_tree = std::make_unique<PlanSegmentTree>();
    ClusterInfoContext cluster_info_context{.query_plan = plan, .context = query_context, .plan_segment_tree = plan_segment_tree};
    PlanSegmentContext plan_segment_context = ClusterInfoFinder::find(*query_plan, cluster_info_context);
    PlanSegmentSplitter::split(plan, plan_segment_context);
    return plan_segment_tree;
}

std::string BasePlanTest::execute(const String & query, ContextMutablePtr query_context)
{
    ThreadStatus thread_status;
    thread_status.attachQueryContext(query_context);
    String res;
    ReadBufferFromString is1(query);
    WriteBufferFromString os1(res);
    executeQuery(is1, os1, false, query_context, {}, {}, false);
    return res;
}

ContextMutablePtr BasePlanTest::createQueryContext(std::unordered_map<std::string, Field> settings)
{
    auto query_context = Context::createCopy(session_context);
    query_context->setSessionContext(session_context);
    query_context->setQueryContext(query_context);
    query_context->setCurrentQueryId("test_plan");
    query_context->createPlanNodeIdAllocator();
    query_context->createSymbolAllocator();
    query_context->createOptimizerMetrics();
    for (const auto & item : settings)
        query_context->setSetting(item.first, item.second);
    return query_context;
}

std::vector<std::string> AbstractPlanTestSuite::loadQueries()
{
    auto path = getQueriesDir();
    if (!std::filesystem::exists(path))
        throw Exception(path.string() + " not found.", ErrorCodes::BAD_ARGUMENTS);

    std::vector<std::string> queries;
    for (const auto & entry : std::filesystem::directory_iterator(path))
        queries.emplace_back(entry.path().stem().string());

    auto try_convert_to_number = [](const std::string & name) {
        try
        {
            return std::stoi(name.substr(1));
        }
        catch (std::exception &)
        {
            return 0;
        }
    };
    std::sort(queries.begin(), queries.end(), [&](auto & a, auto & b) { return try_convert_to_number(a) < try_convert_to_number(b); });

    return queries;
}

AbstractPlanTestSuite::Query AbstractPlanTestSuite::loadQuery(const std::string & name)
{
    auto file = getQueriesDir() / (name + ".sql");
    std::vector<std::string> splits = loadFile(file, ';');

    std::unordered_map<std::string, Field> settings;
    std::vector<std::pair<std::string, ASTPtr>> sql;

    for (auto & split : splits)
    {
        auto ast = parse(split, session_context);
        if (ast->getType() == ASTType::ASTSetQuery)
            for (auto & set : ast->as<ASTSetQuery &>().changes)
                settings.emplace(set.name, set.value);
        else
            sql.emplace_back(split, ast);
    }
    return Query{file.stem().string(), settings, sql};
}

std::string AbstractPlanTestSuite::explain(const std::string & name)
{
    std::string explain;
    auto query = loadQuery(name);
    auto context = createQueryContext(query.settings);
    for (auto & sql : query.sql)
    {
        if (sql.second->getType() == DB::ASTType::ASTSelectQuery || sql.second->getType() == DB::ASTType::ASTSelectWithUnionQuery)
        {
            auto query_plan = plan(sql.first, context);

            CardinalityEstimator::estimate(*query_plan, context);
            explain += DB::PlanPrinter::textLogicalPlan(*query_plan, session_context, show_statistics, true);
        }
        else
            execute(sql.first, context);
    }
    return explain;
}

std::string AbstractPlanTestSuite::loadExplain(const std::string & name)
{
    auto file = getExpectedExplainDir() / (name + ".explain");
    if (!std::filesystem::exists(file))
        return "";
    return loadFile(file)[0];
}

void AbstractPlanTestSuite::saveExplain(const std::string & name, const std::string & explain)
{
    auto file = getExpectedExplainDir() / (name + ".explain");
    if (!std::filesystem::exists(file.parent_path()))
        std::filesystem::create_directories(file.parent_path());

    std::ofstream output;
    output.open(file);
    output << explain;
    output.close();
}

void AbstractPlanTestSuite::createTables()
{
    Statistics::CacheManager::initialize(10000, std::chrono::seconds(1000));
    for (auto & file : getTableDDLFiles())
    {
        for (auto & ddl : loadFile(file, ';'))
        {
            ASTPtr ast = parse(ddl, session_context);
            if (auto * create = ast->as<ASTCreateQuery>())
            {
                auto engine = std::make_shared<ASTFunction>();
                engine->name = "Memory";
                auto storage = std::make_shared<ASTStorage>();
                storage->set(storage->engine, engine);
                create->set(create->storage, storage);
            }

            ThreadStatus thread_status;
            thread_status.attachQueryContext(session_context);
            InterpreterCreateQuery create_interpreter(ast, session_context);
            create_interpreter.execute();
        }
    }
}

void AbstractPlanTestSuite::loadTableStatistics()
{
    auto path = getStatisticsFile();

    auto context = createQueryContext(std::unordered_map<String, Field>{{"graphviz_path", path.parent_path().string()}});

    auto file = path.parent_path() / std::filesystem::path(database_name + ".bin");
    std::string expected_database = path.filename().string().substr(0, path.filename().string().size() - 4);
    if (expected_database != database_name)
        throw Exception("database should be " + expected_database, ErrorCodes::BAD_ARGUMENTS);
    if (!std::filesystem::exists(file))
        throw Exception(file.string() + " not found.", ErrorCodes::BAD_ARGUMENTS);

    ThreadStatus thread_status;
    thread_status.attachQueryContext(context);
    InterpreterShowStatsQuery interpreter(parse("show stats __load;", context), context);
    interpreter.execute();
}

std::vector<std::string> AbstractPlanTestSuite::loadFile(const std::filesystem::path & path, char sep)
{
    if (!std::filesystem::exists(path))
        throw Exception(path.string() + " not found.", ErrorCodes::BAD_ARGUMENTS);

    std::vector<std::string> sections;
    std::ifstream in(path);

    if (sep == '\0')
    {
        std::ostringstream ss;
        ss << in.rdbuf();
        sections.emplace_back(ss.str());
    }
    else
    {
        std::string line;
        while (std::getline(in, line, sep))
        {
            boost::algorithm::trim(line);
            if (line.empty())
                continue;
            sections.emplace_back(std::move(line));
        }
    }
    return sections;
}

bool AbstractPlanTestSuite::enforce_regenerate()
{
    return std::getenv("REGENERATE") != nullptr;
}

int AbstractPlanTestSuite::regenerate_task_thread_size()
{
    if (auto * str = std::getenv("REGENERATE_TASK_THREAD_SIZE"))
    {
        int value;
        if (Poco::NumberParser::tryParse(String{str}, value) && value > 0 && value < 100)
        {
            return value;
        }
    }
    return 8;
}
}
