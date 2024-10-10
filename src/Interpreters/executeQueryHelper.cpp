#include <Catalog/Catalog.h>
#include <DataStreams/RemoteQueryExecutor.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/executeQueryHelper.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Processors/Sources/RemoteSource.h>
#include <Transaction/CnchExplicitTransaction.h>
#include <Transaction/CnchProxyTransaction.h>
#include <Transaction/ICnchTransaction.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPlan/ReadFromPreparedSource.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <Interpreters/QueryLog.h>
#include <Optimizer/QueryUseOptimizerChecker.h>
#include <common/logger_useful.h>


namespace DB
{

HostWithPorts getTargetServer(ContextPtr context, ASTPtr & ast)
{
    auto get_target_server_for_table = [&] (const String & database_name, const String & table_name) -> HostWithPorts
    {
        if (database_name == "system")
            return {};

        DatabaseAndTable db_and_tb = DatabaseCatalog::instance().tryGetDatabaseAndTable(StorageID(database_name, table_name), context);
        DatabasePtr db_ptr = std::move(db_and_tb.first);
        StoragePtr storage_ptr = std::move(db_and_tb.second);
        if (!db_ptr || !storage_ptr || db_ptr->getEngineName() != "Cnch")
            return {};

        auto topology_master = context->getCnchTopologyMaster();

        return topology_master->getTargetServer(
            UUIDHelpers::UUIDToString(storage_ptr->getStorageUUID()), storage_ptr->getServerVwName(), context->getTimestamp(), true);
    };

    auto get_main_table_from_query_settings = [&] (const std::vector<DatabaseAndTableWithAlias> & all_tables, const String & current_db)
    {
        StorageID main_table = StorageID::createEmpty();
        String explicit_main_table = context->getSettingsRef().explicit_main_table;
        if (!explicit_main_table.empty())
        {
            char * begin = explicit_main_table.data();
            char * end = begin + explicit_main_table.size();
            Tokens tokens(begin, end);
            IParser::Pos token_iterator(tokens, context->getSettingsRef().max_parser_depth);
            auto pos = token_iterator;
            Expected expected;
            String database_name, table_name;
            if (parseDatabaseAndTableName(pos, expected, database_name, table_name))
            {
                if (database_name.empty())
                    database_name = current_db;

                // Only if the specified main table shows up in select we can forward the query to its host server.
                if (std::any_of(all_tables.begin(), all_tables.end(), [&](const auto & ele){return ele.database == database_name && ele.table == table_name;}))
                {
                    main_table.database_name = std::move(database_name);
                    main_table.table_name = std::move(table_name);
                    LOG_DEBUG(
                        getLogger("executeQuery"),
                        "Get explicit main table `{}.{}` from query settings.",
                        database_name,
                        table_name);
                }
                else
                {
                    LOG_WARNING(
                        getLogger("executeQuery"),
                        "Ignore main table settings because `{}.{}` is not found in the query.",
                        database_name,
                        table_name);
                }
            }
        }
        return main_table;
    };

    if (const auto * alter = ast->as<ASTAlterQuery>())
    {
        if (alter->alter_object == ASTAlterQuery::AlterObjectType::DATABASE)
            return {};
        return get_target_server_for_table(alter->database.empty() ? context->getCurrentDatabase() : alter->database, alter->table);
    }
    else if (const auto * alter_mysql = ast->as<ASTAlterAnalyticalMySQLQuery>())
    {
        if(alter_mysql->alter_object == ASTAlterQuery::AlterObjectType::DATABASE)
            return {};
        return get_target_server_for_table(alter_mysql->database.empty() ? context->getCurrentDatabase() : alter_mysql->database, alter_mysql->table);
    }
    else if (const auto insert = ast->as<ASTInsertQuery>())
    {
        // for insert select. we find the main table from the query settings or table settings and try to route 
        // the query to the host server of the main table.
        if (insert->select)
        {
            ASTs tables;
            bool has_table_func = false;
            ASTSelectQuery::collectAllTables(insert->select.get(), tables, has_table_func);
            String current_db = context->getCurrentDatabase();

            std::vector<DatabaseAndTableWithAlias> db_and_tables;
            for (const auto & table_ast : tables)
                db_and_tables.emplace_back(DatabaseAndTableWithAlias(table_ast, current_db));

            StorageID main_table_from_query_setting = get_main_table_from_query_settings(db_and_tables, current_db);
            if (!main_table_from_query_setting.empty())
                return get_target_server_for_table(main_table_from_query_setting.database_name, main_table_from_query_setting.table_name);

            for (const auto & db_and_table : db_and_tables)
            {
                if (db_and_table.database == "system")
                    continue;
                DatabaseAndTable db_and_tb = DatabaseCatalog::instance().tryGetDatabaseAndTable(StorageID(db_and_table.database, db_and_table.table), context);
                DatabasePtr db_ptr = std::move(db_and_tb.first);
                StoragePtr storage_ptr = std::move(db_and_tb.second);
                if (!db_ptr || !storage_ptr || db_ptr->getEngineName() != "Cnch")
                    continue;
                
                auto * cnch = dynamic_cast<MergeTreeMetaBase *>(storage_ptr.get());
                if (!cnch)
                    continue;
                
                if (cnch->getSettings()->as_main_table)
                {
                    auto topology_master = context->getCnchTopologyMaster();
                    return topology_master->getTargetServer(
                        UUIDHelpers::UUIDToString(storage_ptr->getStorageUUID()), storage_ptr->getServerVwName(), context->getTimestamp(), true);
                }
            }
        }

        // Otherwise, do not route query
        return {};
    }
    else if (const auto * select = ast->as<ASTSelectWithUnionQuery>())
    {
        if (!context->getSettingsRef().enable_select_query_forwarding && !context->getSettingsRef().enable_multiple_table_select_query_forwarding)
            return {};

        ASTs tables;
        bool has_table_func = false;
        ASTSelectQuery::collectAllTables(ast.get(), tables, has_table_func);

        if (tables.empty() || has_table_func)
            return {};

        String current_db = context->getCurrentDatabase();
        if (tables.size() == 1)
        {
            DatabaseAndTableWithAlias db_and_table(tables[0], current_db);
            LOG_DEBUG(
                getLogger("executeQuery"),
                "Get main table `{}.{}` for current select query.",
                db_and_table.database,
                db_and_table.table);
            return get_target_server_for_table(db_and_table.database, db_and_table.table);
        }
        else
        {
            if (!context->getSettingsRef().enable_multiple_table_select_query_forwarding)
                return {};

            std::vector<DatabaseAndTableWithAlias> db_and_tables;
            for (const auto & table_ast : tables)
                db_and_tables.emplace_back(DatabaseAndTableWithAlias(table_ast, current_db));
            
            /// For multiple table select, we forward the query with the following policy:
            /// 1. If the main table is explicitly set, use the user defined main table.
            /// 2. If one of the table is set to be main table in the table settings, use that table
            /// 3. Pick up the first one table shows up in the query as main table.
            StorageID main_table_from_query_setting = get_main_table_from_query_settings(db_and_tables, current_db);
            if (!main_table_from_query_setting.empty())
                return get_target_server_for_table(main_table_from_query_setting.database_name, main_table_from_query_setting.table_name);

            StoragePtr main_storage;
            for (const auto & db_and_table : db_and_tables)
            {
                if (db_and_table.database == "system")
                    continue;
                
                DatabaseAndTable db_and_tb = DatabaseCatalog::instance().tryGetDatabaseAndTable(StorageID(db_and_table.database, db_and_table.table), context);
                DatabasePtr db_ptr = std::move(db_and_tb.first);
                StoragePtr storage_ptr = std::move(db_and_tb.second);
                if (!db_ptr || !storage_ptr || db_ptr->getEngineName() != "Cnch")
                    continue;
                
                if (!main_storage)
                    main_storage = std::move(storage_ptr);
                else
                {
                    auto * cnch = dynamic_cast<MergeTreeMetaBase *>(storage_ptr.get());
                    if (!cnch)
                        continue;
                    
                    // If multiple tables are set to be main table, pick up the first one.
                    if (cnch->getSettings()->as_main_table)
                    {
                        main_storage = std::move(storage_ptr);
                        break;
                    }
                }
            }

            if (main_storage)
            {
                LOG_DEBUG(
                    getLogger("executeQuery"),
                    "Get main table `{}.{}` for current select query.",
                    main_storage->getDatabaseName(),
                    main_storage->getTableName());
                auto topology_master = context->getCnchTopologyMaster();
                return topology_master->getTargetServer(
                    UUIDHelpers::UUIDToString(main_storage->getStorageUUID()), main_storage->getServerVwName(), context->getTimestamp(), true);
            }

            return {};
        }
    }
    else if (const auto * rename = ast->as<ASTRenameQuery>())
    {
        if (rename->database)
            return {};
        
        return get_target_server_for_table(rename->elements.at(0).from.database.empty() ? context->getCurrentDatabase() : rename->elements.at(0).from.database,
            rename->elements.at(0).from.table);
    }
    else
        return {};
}

void executeQueryByProxy(ContextMutablePtr context, const HostWithPorts & server, const ASTPtr & ast, BlockIO & res, bool in_interactive_txn, const String & query)
{
    auto session_txn = in_interactive_txn ? context->getSessionContext()->getCurrentTransaction() : nullptr;
    ProxyTransactionPtr proxy_txn;
    if (session_txn && session_txn->isPrimary())
    {
        proxy_txn = context->getCnchTransactionCoordinator().createProxyTransaction(server, session_txn->getPrimaryTransactionID(),
            isReadOnlyTransaction(ast.get()));
        context->setCurrentTransaction(proxy_txn);
        session_txn->as<CnchExplicitTransaction>()->addStatement(query);
    }

    /// Create connection to host
    const auto & query_client_info = context->getClientInfo();
    auto settings = context->getSettingsRef();
    res.remote_execution_conn = std::make_shared<Connection>(
        server.getHost(),
        server.tcp_port,
        context->getCurrentDatabase(), /*default_database_*/
        query_client_info.current_user,
        query_client_info.current_password,
        "", /*cluster_*/
        "", /*cluster_secret*/
        "server", /*client_name_*/
        Protocol::Compression::Enable,
        Protocol::Secure::Disable);
    res.remote_execution_conn->setDefaultDatabase(context->getCurrentDatabase());

    // PipelineExecutor requires block header.
    LOG_DEBUG(getLogger("executeQuery"), "Sending query as ordinary query");
    Block header;
    if ((context->getSettingsRef().enable_select_query_forwarding || context->getSettingsRef().enable_multiple_table_select_query_forwarding)
        && ast->as<ASTSelectWithUnionQuery>())
    {
        if (settings.enable_optimizer &&  QueryUseOptimizerChecker::check(ast, context))
            header = InterpreterSelectQueryUseOptimizer(ast, context, SelectQueryOptions(QueryProcessingStage::Complete).analyze()).getSampleBlock();
        else
            header = InterpreterSelectWithUnionQuery(ast, context, SelectQueryOptions(QueryProcessingStage::Complete).analyze()).getSampleBlock();
    }

    Pipes remote_pipes;
    auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(*res.remote_execution_conn, query, header, context);
    remote_query_executor->setPoolMode(PoolMode::GET_ONE);
    remote_query_executor->setServerForwarding(true);
    remote_query_executor->setQueryId(query_client_info.initial_query_id);
    remote_pipes.emplace_back(createRemoteSourcePipe(remote_query_executor, true, false, false, true));
    remote_pipes.back().addInterpreterContext(context);

    auto plan = std::make_unique<QueryPlan>();
    auto read_from_remote = std::make_unique<ReadFromPreparedSource>(Pipe::unitePipes(std::move(remote_pipes)));
    read_from_remote->setStepDescription("Read from remote server");
    plan->addStep(std::move(read_from_remote));
    res.pipeline = std::move(
        *plan->buildQueryPipeline(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context)));
    res.pipeline.addInterpreterContext(context);

    res.finish_callback = [proxy_txn, context, remote_query_executor](IBlockInputStream *, IBlockOutputStream *, QueryPipeline *) {
        /// Get the extended profile info which is mainly for INSERT SELECT/INSERT INFILE
        context->setExtendedProfileInfo(remote_query_executor->getExtendedProfileInfo());
        if (proxy_txn)
            proxy_txn->setTransactionStatus(CnchTransactionStatus::Finished);

        LOG_DEBUG(getLogger("executeQuery"), "Query success on remote server");

    };
    res.exception_callback = [proxy_txn, context]() {
        if (proxy_txn)
            proxy_txn->setTransactionStatus(CnchTransactionStatus::Aborted);

        LOG_DEBUG(getLogger("executeQuery"), "Query failed on remote server");
    };
}

/// Call this inside catch block.
void setExceptionStackTrace(QueryLogElement & elem)
{
    /// Disable memory tracker for stack trace.
    /// Because if exception is "Memory limit (for query) exceed", then we probably can't allocate another one string.
    MemoryTracker::BlockerInThread temporarily_disable_memory_tracker(VariableContext::Global);

    try
    {
        throw;
    }
    catch (const std::exception & e)
    {
        elem.stack_trace = getExceptionStackTraceString(e);
    }
    catch (...)
    {
    }
}
}
