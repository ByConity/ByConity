#include <Catalog/Catalog.h>
#include <DataStreams/RemoteQueryExecutor.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/executeQueryHelper.h>
#include <MergeTreeCommon/CnchTopologyMaster.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/Sources/RemoteSource.h>
#include <Transaction/CnchExplicitTransaction.h>
#include <Transaction/CnchProxyTransaction.h>
#include <Transaction/ICnchTransaction.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPlan/ReadFromPreparedSource.h>

#include <common/logger_useful.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>

namespace DB
{

HostWithPorts getTargetServer(ContextPtr context, ASTPtr & ast)
{
    /// Only get target server for main table
    String database, table;

    if (const auto * alter = ast->as<ASTAlterQuery>())
    {
        database = alter->database;
        table = alter->table;
    }
    else if (const auto * select = ast->as<ASTSelectWithUnionQuery>())
    {
        ASTs tables;
        bool has_table_func = false;
        ASTSelectQuery::collectAllTables(ast.get(), tables, has_table_func);
        // when query inlcudes multiple tables, it is better to just keep existing host since cannot guarantee all tables are in the same host.
        if (!has_table_func && !tables.empty() && tables.size() == 1)
        {
            // simplily use the first table if there are multiple tables used
            DatabaseAndTableWithAlias db_and_table(tables[0]);
            LOG_DEBUG(
                &Poco::Logger::get("executeQuery"),
                "Extract db and table {}.{} from the query.",
                db_and_table.database,
                db_and_table.table);
            database = db_and_table.database;
            table = db_and_table.table;
        }
        else
            return {};
    }
    else
        return {};

    if (database.empty())
        database = context->getCurrentDatabase();

    if (database == "system")
        return {};

    auto storage = DatabaseCatalog::instance().tryGetTable(StorageID(database, table), context);
    if (!storage)
        return {};

    auto topology_master = context->getCnchTopologyMaster();

    return topology_master->getTargetServer(
        UUIDHelpers::UUIDToString(storage->getStorageUUID()), storage->getServerVwName(), context->getTimestamp(), true);
}

void executeQueryByProxy(ContextMutablePtr context, const HostWithPorts & server, const ASTPtr & ast, BlockIO & res, bool in_interactive_txn, const String & query)
{
    auto session_txn = in_interactive_txn ? context->getSessionContext()->getCurrentTransaction() : nullptr;
    ProxyTransactionPtr proxy_txn;
    if (session_txn && session_txn->isPrimary())
    {
        proxy_txn = context->getCnchTransactionCoordinator().createProxyTransaction(server, session_txn->getPrimaryTransactionID());
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
    LOG_DEBUG(&Poco::Logger::get("executeQuery"), "Sending query as ordinary query");
    Block header;
    if (ast->as<ASTSelectWithUnionQuery>())
        header = InterpreterSelectWithUnionQuery(ast, context, SelectQueryOptions(QueryProcessingStage::Complete).analyze()).getSampleBlock();
    Pipes remote_pipes;
    auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(*res.remote_execution_conn, query, header, context);
    remote_query_executor->setPoolMode(PoolMode::GET_ONE);
    remote_query_executor->setServerForwarding(true);
    remote_pipes.emplace_back(createRemoteSourcePipe(remote_query_executor, true, false, false, true));
    remote_pipes.back().addInterpreterContext(context);

    auto plan = std::make_unique<QueryPlan>();
    auto read_from_remote = std::make_unique<ReadFromPreparedSource>(Pipe::unitePipes(std::move(remote_pipes)));
    read_from_remote->setStepDescription("Read from remote server");
    plan->addStep(std::move(read_from_remote));
    res.pipeline = std::move(
        *plan->buildQueryPipeline(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context)));
    res.pipeline.addInterpreterContext(context);

    res.finish_callback = [proxy_txn, context, remote_query_executor](IBlockInputStream *, IBlockOutputStream *, QueryPipeline *, UInt64) {
        /// Get the extended profile info which is mainly for INSERT SELECT/INSERT INFILE
        context->setExtendedProfileInfo(remote_query_executor->getExtendedProfileInfo());
        if (proxy_txn)
            proxy_txn->setTransactionStatus(CnchTransactionStatus::Finished);

        LOG_DEBUG(&Poco::Logger::get("executeQuery"), "Query success on remote server");

    };
    res.exception_callback = [proxy_txn, context](int) {
        if (proxy_txn)
            proxy_txn->setTransactionStatus(CnchTransactionStatus::Aborted);

        LOG_DEBUG(&Poco::Logger::get("executeQuery"), "Query failed on remote server");
    };
}

}
