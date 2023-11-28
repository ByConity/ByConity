#include <Interpreters/InterpreterReproduceQuery.h>

#include <Analyzers/QueryAnalyzer.h>
#include <Columns/IColumn.h>
#include <Common/CurrentThread.h>
#include <Common/SettingsChanges.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Optimizer/Dump/DumpUtils.h>
#include <Optimizer/Dump/PlanReproducer.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTReproduceQuery.h>
#include <Parsers/ASTSetQuery.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace DB
{

namespace
{
    std::string getStringFromLiteral(ASTPtr ast)
    {
        try
        {
            return ast->as<ASTLiteral &>().value.safeGet<String>();
        }
        catch (Exception & e)
        {
            throw Exception("invalid reproduce query: " + e.message(), ErrorCodes::BAD_ARGUMENTS);
        }
    }
}

BlockIO InterpreterReproduceQuery::execute()
{
    auto reproduce_query = query_ptr->as<ASTReproduceQuery &>();

    std::string zip_path = getStringFromLiteral(reproduce_query.reproducePath());
    PlanReproducer reproducer(zip_path, getContext());

    if (auto cluster = reproduce_query.cluster())
    {
        reproducer.setCluster(cluster->as<ASTIdentifier &>().name());
    }

    // reproduce ddl
    if (reproduce_query.mode == ASTReproduceQuery::Mode::DDL)
    {
        return reproduceDDLImpl(std::move(reproducer));
    }

    // get queries to reproduce
    std::vector<PlanReproducer::Query> queries_to_reproduce{};
    if (ASTPtr subquery = reproduce_query.subquery())
    {
        queries_to_reproduce.emplace_back(PlanReproducer::Query{/*query_id=*/"",
                                                                /*query=*/serializeAST(*subquery),
                                                                /*current_database=*/getContext()->getCurrentDatabase(),
                                                                SettingsChanges{}, std::nullopt, std::nullopt});
    }
    else if (ASTPtr query_id_literal = reproduce_query.queryId())
    {
        queries_to_reproduce.emplace_back(reproducer.getQuery(getStringFromLiteral(query_id_literal)));
    }
    else
    {
        if (!reproducer.getQueries())
            throw Exception("there is no query to reproduce in " + zip_path, ErrorCodes::BAD_ARGUMENTS);
        for (const auto & query_id : reproducer.getQueries()->getNames())
            queries_to_reproduce.emplace_back(reproducer.getQuery(query_id));
    }

    // get enforced settings
    SettingsChanges enforced_settings;
    if (reproduce_query.settingsChanges())
        enforced_settings = reproduce_query.settingsChanges()->as<ASTSetQuery &>().changes;

    if (reproduce_query.mode == ASTReproduceQuery::Mode::EXPLAIN)
    {
        return reproduceExplainImpl(std::move(reproducer),
                                    std::move(queries_to_reproduce),
                                    std::move(enforced_settings),
                                    reproduce_query.is_verbose);
    }
    else if (reproduce_query.mode == ASTReproduceQuery::Mode::EXECUTE)
    {
        return reproduceExecuteImpl(std::move(reproducer),
                                    std::move(queries_to_reproduce),
                                    std::move(enforced_settings),
                                    reproduce_query.is_verbose);
    }
    else
    {
        throw Exception("invalid reproduce query", ErrorCodes::LOGICAL_ERROR);
    }
}

BlockIO InterpreterReproduceQuery::reproduceDDLImpl(PlanReproducer && reproducer)
{
    reproducer.createTables(/*load_stats=*/true);

    Block sample_block;

    ColumnWithTypeAndName col;
    col.name = "database";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    sample_block.insert(col);

    col.name = "table";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    sample_block.insert(col);

    col.name = "status";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    sample_block.insert(col);

    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    for (const auto & [database, status] : reproducer.database_status)
    {
        std::string status_description = ReproduceUtils::toString(status);
        res_columns[0]->insert(database);
        res_columns[1]->insert("");
        res_columns[2]->insert(status_description);
    }

    for (const auto & [table, status] : reproducer.table_status)
    {
        std::string status_description = ReproduceUtils::toString(status);
        res_columns[0]->insert(table.database);
        res_columns[1]->insert(table.table);
        res_columns[2]->insert(status_description);
    }

    BlockIO res;
    res.in = std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
    return res;
}

BlockIO InterpreterReproduceQuery::reproduceExplainImpl(PlanReproducer && reproducer,
                                                        const std::vector<PlanReproducer::Query> & queries,
                                                        const SettingsChanges & enforced_settings,
                                                        bool verbose = false)
{
    Block sample_block;

    ColumnWithTypeAndName col;
    col.name = "query_id";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    sample_block.insert(col);

    col.name = "same_explain";
    col.type = std::make_shared<DataTypeUInt8>();
    col.column = col.type->createColumn();
    sample_block.insert(col);

    col.name = "exception";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    sample_block.insert(col);

    if (verbose)
    {
        col.name = "query";
        col.type = std::make_shared<DataTypeString>();
        col.column = col.type->createColumn();
        sample_block.insert(col);

        col.name = "original_explain";
        col.type = std::make_shared<DataTypeString>();
        col.column = col.type->createColumn();
        sample_block.insert(col);

        col.name = "reproduce_explain";
        col.type = std::make_shared<DataTypeString>();
        col.column = col.type->createColumn();
        sample_block.insert(col);
    }

    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    for (const auto & query : queries)
    {
        std::optional<std::string> exception;
        std::string reproduced;
        bool same_explain = false;
        try
        {
            ContextMutablePtr query_context = reproducer.makeQueryContext(query.settings_changes,
                                                                          query.current_database,
                                                                          query.memory_catalog_worker_size);
            query_context->applySettingsChanges(enforced_settings);
            reproduced = ReproduceUtils::obtainExplainString(query.query, query_context);
            same_explain = query.original_explain.has_value() && query.original_explain.value() == reproduced;
        }
        catch (Exception & e)
        {
            exception = (verbose) ? e.getStackTraceString() : e.message();
        }

        res_columns[0]->insert(query.query_id);
        res_columns[1]->insert(same_explain);
        res_columns[2]->insert(exception.value_or(""));
        if (verbose)
        {
            res_columns[3]->insert(query.query);
            res_columns[4]->insert(query.original_explain.value_or(""));
            res_columns[5]->insert(reproduced);
        }
    }

    BlockIO res;
    res.in = std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
    return res;
}

BlockIO InterpreterReproduceQuery::reproduceExecuteImpl(PlanReproducer && reproducer,
                                                        const std::vector<PlanReproducer::Query> & queries,
                                                        const SettingsChanges & enforced_settings,
                                                        bool verbose = false)
{
    Block sample_block;

    ColumnWithTypeAndName col;
    col.name = "query_id";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    sample_block.insert(col);

    col.name = "has_exception";
    col.type = std::make_shared<DataTypeUInt8>();
    col.column = col.type->createColumn();
    sample_block.insert(col);

    col.name = "exception";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    sample_block.insert(col);

    if (verbose)
    {
        col.name = "query";
        col.type = std::make_shared<DataTypeString>();
        col.column = col.type->createColumn();
        sample_block.insert(col);
    }

    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    for (const auto & query : queries)
    {
        std::optional<std::string> exception;
        try
        {
            ContextMutablePtr query_context = reproducer.makeQueryContext(query.settings_changes,
                                                                          query.current_database,
                                                                          query.memory_catalog_worker_size);
            query_context->applySettingsChanges(enforced_settings);
            String query_id = query_context->getCurrentQueryId();
            query_context->setCurrentQueryId(query_id + "_sub_query_" + query.query_id);
            auto & thread_status = CurrentThread::get();
            thread_status.attachQueryContext(query_context);

            String res;
            ReadBufferFromString is1(query.query);
            WriteBufferFromString os1(res);
            executeQuery(is1, os1, false, query_context, {}, {}, false);

            query_context->setCurrentQueryId(query_id);
        }
        catch (Exception & e)
        {
            exception = (verbose) ? e.getStackTraceString() : e.message();
        }

        res_columns[0]->insert(query.query_id);
        res_columns[1]->insert(exception.has_value());
        res_columns[2]->insert(exception.value_or(""));
        if (verbose)
        {
            res_columns[3]->insert(query.query);
        }
    }

    BlockIO res;
    res.in = std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
    return res;
}


}
