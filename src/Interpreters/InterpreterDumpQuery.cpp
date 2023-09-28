#include <Interpreters/InterpreterDumpQuery.h>

#include <chrono>
#include <Analyzers/QueryAnalyzer.h>
#include <Analyzers/tryEvaluateConstantExpression.h>
#include <Columns/ColumnMap.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/executeQuery.h>
#include <Optimizer/Dump/PlanReproducer.h>
#include <Optimizer/PlanOptimizer.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{

namespace
{
    ASTPtr makePredicate(const std::string & column, const std::string & op, Field && value)
    {
        return makeASTFunction(op, std::make_shared<ASTIdentifier>(column), std::make_shared<ASTLiteral>(value));
    }

    UInt64 convertToDateTime(const ASTPtr & expr, ContextPtr context)
    {
        auto rewrite = makeASTFunction("cast", expr, std::make_shared<ASTLiteral>("DateTime"));
        auto res = tryEvaluateConstantExpression(rewrite, context);
        if (!res.has_value() || res.value().getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot convert {} to type DateTime", expr->getColumnName());
        return res.value().get<UInt64>();
    }
}

std::string InterpreterDumpQuery::getDumpPath(ASTPtr & query_ptr_, ContextPtr context_)
{
    const auto & dump = query_ptr_->as<const ASTDumpQuery &>();
    if (!dump.dumpPath())
        return context_->getSettingsRef().graphviz_path.toString() + "dump/" + context_->getCurrentQueryId();
    String dump_path = dump.dumpPath()->as<ASTLiteral &>().value.safeGet<String>();
    if (dump_path.empty())
        return context_->getSettingsRef().graphviz_path.toString() + "dump/" + context_->getCurrentQueryId();
    if (dump_path.ends_with(".zip"))
        dump_path = dump_path.substr(0, dump_path.size() - 4);
    return DumpUtils::simplifyPath(dump_path);
}

BlockIO InterpreterDumpQuery::execute()
{
    auto & dump = query_ptr->as<ASTDumpQuery &>();
    switch (dump.kind)
    {
        case ASTDumpQuery::Kind::DDL:
            executeDumpDDLImpl();
            break;
        case ASTDumpQuery::Kind::Query:
            executeDumpQueryImpl();
            break;
        case ASTDumpQuery::Kind::Workload:
            executeDumpWorkloadImpl();
            break;
    }

    size_t tables_dumped = ddl_dumper.tables() + ddl_dumper.views();
    size_t queries_dumped = query_dumper.size();

    if (!tables_dumped && !queries_dumped)
    {
        LOG_INFO(log, "nothing to dump");
        return {};
    }

    if (tables_dumped)
    {
        ddl_dumper.dump();
        LOG_INFO(log, "Dumped {} tables to {}", tables_dumped, dump_path);
    }
    if (queries_dumped)
    {
        query_dumper.dump();
        LOG_INFO(log, "Dumped {} queries to {}", queries_dumped, dump_path);
    }

    DumpUtils::zipDirectory(dump_path);

    Block sample_block;

    ColumnWithTypeAndName col;
    col.name = "file";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    sample_block.insert(col);

    col.name = "tables dumped";
    col.type = std::make_shared<DataTypeUInt64>();
    col.column = col.type->createColumn();
    sample_block.insert(col);

    col.name = "(distinct) queries dumped";
    col.type = std::make_shared<DataTypeUInt64>();
    col.column = col.type->createColumn();
    sample_block.insert(col);


    MutableColumns res_columns = sample_block.cloneEmptyColumns();
    res_columns[0]->insert(getZipFilePath());
    res_columns[1]->insert(tables_dumped);
    res_columns[2]->insert(queries_dumped);

    BlockIO res;
    res.in = std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
    return res;
}

void InterpreterDumpQuery::executeDumpDDLImpl()
{
    auto & dump = query_ptr->as<ASTDumpQuery &>();
    if (dump.databases())
    {
        for (const auto & db : dump.databases()->children)
        {
            ddl_dumper.addTableFromDatabase(db->as<ASTIdentifier &>().name(), getContext());
        }
    }
    else
    {
        ddl_dumper.addTableAll(getContext());
    }
}

void InterpreterDumpQuery::executeDumpQueryImpl()
{
    auto & dump = query_ptr->as<ASTDumpQuery &>();
    if (dump.queryIds() && !dump.subquery())
    {
        enable_explain = true;
        executeDumpWorkloadImpl(); // workload will handle query ids in filter condition
    }
    else if (dump.subquery() && !dump.queryIds())
    {
        enable_explain = true;

        std::string sql = serializeAST(*dump.subquery());

        process(getContext()->getCurrentQueryId(),
                sql,
                std::make_shared<Settings>(getContext()->getSettings()),
                getContext()->getCurrentDatabase());
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot dump query: either query_ids or subquery should be specified");
}

void InterpreterDumpQuery::executeDumpWorkloadImpl()
{
    // obtain query logs
    String query_log_scan = prepareQueryLogSQL();
    LOG_DEBUG(log, "querying query_log with: {}", query_log_scan);
    BlockIO query_log_res = executeQuery(query_log_scan, Context::createCopy(getContext()), /*internal=*/true);
    BlockInputStreamPtr query_log_block_stream = query_log_res.getInputStream();

    while (Block block = query_log_block_stream->read())
    {
        processQueryLogBlock(block);
    }
}

void InterpreterDumpQuery::processQueryLogBlock(const Block & block)
{
    if (!block.rows())
        return;

    auto start_watch = std::chrono::high_resolution_clock::now();

    const auto & query_column = *(block.getByName("query").column); // ColumnString
    const auto & current_database_column = *(block.getByName("current_database").column); // ColumnString
    const auto & query_id_column = *(block.getByName("query_id").column); // ColumnString
    const auto & settings_names_column = *(block.getByName("Settings.Names").column); // ColumnArray
    const auto & settings_values_column = *(block.getByName("Settings.Values").column); // ColumnArray

    for (size_t i = 0; i < block.rows(); ++i)
    {
        QueryLogElement elem;

        elem.query = query_column[i].get<String>();
        elem.current_database = current_database_column[i].get<String>();
        elem.client_info.current_query_id = query_id_column[i].get<String>();
        elem.query_settings = std::make_shared<Settings>();
        Array settings_names_array = settings_names_column[i].get<Array>();
        Array settings_values_array = settings_values_column[i].get<Array>();
        for (size_t j = 0; j < settings_names_array.size(); ++j)
        {
            auto setting_name = settings_names_array[j].get<String>();
            auto setting_value = settings_values_array[j].get<String>();
            elem.query_settings->set(setting_name, setting_value);
        }

        process(elem.client_info.current_query_id,
                elem.query,
                elem.query_settings,
                elem.current_database);
    }

    auto stop_watch = std::chrono::high_resolution_clock::now();
    LOG_DEBUG(log, "processed {} entries, cost: {} ms", block.rows(),
              std::chrono::duration_cast<std::chrono::milliseconds>(stop_watch - start_watch).count());
}

void InterpreterDumpQuery::process(const String & query_id,
                                   const String & sql,
                                   const std::shared_ptr<Settings> & settings,
                                   const String & current_database)
{
    QueryDumper::Query elem(query_id, sql, current_database);

    // skipping recurring queries
    if (query_dumper.query_hash_frequencies.contains(elem.recurring_hash))
    {
        ++query_dumper.query_hash_frequencies[elem.recurring_hash];
        return;
    }

    QueryDumper::Query & query = query_dumper.add(elem);

    try
    {
        query.settings = settings;

        ContextMutablePtr query_context = query.buildQueryContextFrom(getContext());
        ASTPtr ast = ReproduceUtils::parse(query.query, query_context);

        // dump ddl
        if (enable_ddl)
        {
            auto start_watch = std::chrono::high_resolution_clock::now();
            auto worker_size = ddl_dumper.addTableFromSelectQuery(ast, query_context);
            if (worker_size)
                query.info.emplace(DumpUtils::QueryInfo::memory_catalog_worker_size, std::to_string(worker_size.value()));
            auto stop_watch = std::chrono::high_resolution_clock::now();
            LOG_DEBUG(log, "dump ddl and stats for query {} cost: {} ms", query_id,
                      std::chrono::duration_cast<std::chrono::milliseconds>(stop_watch - start_watch).count());
        }

        // dump explain
        if (enable_explain)
        {
            auto start_watch = std::chrono::high_resolution_clock::now();

            std::string explain = ReproduceUtils::obtainExplainString(query.query, query_context);
            query.info.emplace(DumpUtils::QueryInfo::explain, explain);

            auto stop_watch = std::chrono::high_resolution_clock::now();
            LOG_DEBUG(log, "dump explain for query {} cost: {} ms", query_id,
                      std::chrono::duration_cast<std::chrono::milliseconds>(stop_watch - start_watch).count());
        }
    } catch (Exception & e)
    {
        LOG_WARNING(log, "error when dumping query_id {}, query info may be incomplete. reason: {}", query_id, e.message());
    }
}

ASTPtr InterpreterDumpQuery::prepareQueryLogTableExpression() const
{
    auto query_log_table = std::make_shared<ASTTableExpression>();
    auto & dump_query = query_ptr->as<ASTDumpQuery &>();
    if (dump_query.cluster())
    {
        auto table_function = makeASTFunction("cluster",
                                              dump_query.cluster()->clone(),
                                              std::make_shared<ASTTableIdentifier>(DatabaseCatalog::SYSTEM_DATABASE),
                                              std::make_shared<ASTTableIdentifier>("query_log"));
        query_log_table->table_function = table_function;
        query_log_table->children.push_back(table_function);
    }
    else
    {
        auto table_ast = std::make_shared<ASTTableIdentifier>(DatabaseCatalog::SYSTEM_DATABASE, "query_log");
        query_log_table->database_and_table_name = table_ast;
        query_log_table->children.push_back(table_ast);
    }
    return query_log_table;
}

ASTPtr InterpreterDumpQuery::prepareQueryLogConditions() const
{
    auto & dump_query = query_ptr->as<ASTDumpQuery &>();
    if (auto where = dump_query.where())
        return where;

    ASTs conditions;
    conditions.push_back(makePredicate("is_initial_query", "equals", 1)); // is_initial_query = 1
    conditions.push_back(makePredicate("type", "equals", "QueryFinish")); // type = 'QueryFinish'

    if (auto query_ids = dump_query.queryIds())
    {
        auto ast = makeASTFunction("in", std::make_shared<ASTIdentifier>("query_id"), query_ids);
        conditions.push_back(ast);
    }
    else
    {
        conditions.push_back(makePredicate("query_kind", "equals", "Select")); // query_kind = 'Select'
        conditions.push_back(makeASTFunction("not", makePredicate("databases", "has", DatabaseCatalog::SYSTEM_DATABASE))); // not(has(databases, 'system'))
        // fixme: find better way to filter out this query from Suggest.cpp
        conditions.push_back(makeASTFunction("not", makePredicate("query", "startsWith", "SELECT DISTINCT arrayJoin(extractAll(name,")));

        auto begin_time = dump_query.beginTime();
        auto end_time = dump_query.endTime();
        if (begin_time)
        {
            convertToDateTime(begin_time, getContext());
            if (end_time && convertToDateTime(begin_time, getContext()) > convertToDateTime(end_time, getContext()))
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "begin time {} should be earlier than end time {}",
                    begin_time->getColumnName(),
                    end_time->getColumnName());
            }
            else if (convertToDateTime(begin_time, getContext()) > convertToDateTime(makeASTFunction("now"), getContext()))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "begin time {} should be earlier than now", begin_time->getColumnName());
            }

            auto ast = makeASTFunction("greaterOrEquals", std::make_shared<ASTIdentifier>("event_time"), begin_time);
            conditions.push_back(ast);
        }

        if (end_time)
        {
            convertToDateTime(end_time, getContext());
            auto ast = makeASTFunction("lessOrEquals", std::make_shared<ASTIdentifier>("event_time"), end_time);
            conditions.push_back(ast);
        }
    }

    if (conditions.size() > 1)
    {
        auto coalesced_predicates = makeASTFunction("and");
        coalesced_predicates->arguments->children = std::move(conditions);
        return coalesced_predicates;
    }
    else if (conditions.size() == 1)
    {
        return conditions.front();
    }

    return nullptr;
}


// select query, query_id, current_database, Settings
// from system.query_log
// where is_initial_query = 1 and type = 'QueryFinish' and query_kind = 'Select'
//       and not(has(databases, 'system')) and not(startsWith(query, 'SELECT DISTINCT arrayJoin(extractAll(name,'))
// order by event_time desc
// limit 100;
String InterpreterDumpQuery::prepareQueryLogSQL() const
{
    auto & dump_query = query_ptr->as<ASTDumpQuery &>();
    auto select = std::make_shared<ASTSelectQuery>();

    // SELECT ...
    select->setExpression(ASTSelectQuery::Expression::SELECT, prepareQueryLogSelectList());

    // FROM system.query_log | cluster(cluster_name, system, query_log)
    select->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    auto query_log_table = prepareQueryLogTableExpression();
    table_element->table_expression = query_log_table;
    table_element->children.push_back(query_log_table);
    select->tables()->children.push_back(table_element);

    // WHERE
    if (auto predicates = prepareQueryLogConditions())
        select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(predicates));

    // ORDER BY event_time desc
    select->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::make_shared<ASTExpressionList>());
    auto order_by_element = std::make_shared<ASTOrderByElement>();
    order_by_element->direction = -1;
    ASTPtr event_time_ast = std::make_shared<ASTIdentifier>("event_time");
    order_by_element->children.push_back(event_time_ast);
    select->orderBy()->children.push_back(order_by_element);

    // LIMIT
    if (dump_query.limitLength())
        select->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH,  dump_query.limitLength());
    else
        select->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH,  std::make_shared<ASTLiteral>(100));

    return serializeAST(*select);
}

// if you modify this, please also update processQueryLogBlock() accordingly
ASTPtr InterpreterDumpQuery::prepareQueryLogSelectList() const
{
    auto selects = std::make_shared<ASTExpressionList>();
    selects->children.push_back(std::make_shared<ASTIdentifier>("query"));
    selects->children.push_back(std::make_shared<ASTIdentifier>("current_database"));
    selects->children.push_back(std::make_shared<ASTIdentifier>("query_id"));
    selects->children.push_back(std::make_shared<ASTIdentifier>("Settings.Names"));
    selects->children.push_back(std::make_shared<ASTIdentifier>("Settings.Values"));
    return selects;
}

}
