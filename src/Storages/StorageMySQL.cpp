#include "StorageMySQL.h"

#if USE_MYSQL

#include <Storages/StorageFactory.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Formats/MySQLBlockInputStream.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Common/parseAddress.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTCreateQuery.h>
#include <mysqlxx/Transaction.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Pipe.h>
#include <Parsers/queryToString.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/parseQuery.h>
#include <Common/parseRemoteDescription.h>
#include <Catalog/Catalog.h>
#include <Storages/AlterCommands.h>
#include <Transaction/Actions/DDLAlterAction.h>
#include <Transaction/ICnchTransaction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

static String backQuoteMySQL(const String & x)
{
    String res(x.size(), '\0');
    {
        WriteBufferFromString wb(res);
        writeBackQuotedStringMySQL(x, wb);
    }
    return res;
}

StorageMySQL::StorageMySQL(
    const StorageID & table_id_,
    mysqlxx::PoolWithFailover && pool_,
    const std::string & remote_database_name_,
    const std::string & remote_table_name_,
    const bool replace_query_,
    const std::string & on_duplicate_clause_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    const MySQLSettings & mysql_settings_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , remote_database_name(remote_database_name_)
    , remote_table_name(remote_table_name_)
    , replace_query{replace_query_}
    , on_duplicate_clause{on_duplicate_clause_}
    , mysql_settings(mysql_settings_)
    , pool(std::make_shared<mysqlxx::PoolWithFailover>(pool_))
    , logger(getLogger(getStorageID().getNameForLogs()))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}


Pipe StorageMySQL::read(
    const Names & column_names_,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info_,
    ContextPtr context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned)
{
    storage_snapshot->check(column_names_);

    String query = transformQueryForExternalDatabase(
        query_info_,
        storage_snapshot->metadata->getColumns().getOrdinary(),
        IdentifierQuotingStyle::BackticksMySQL,
        remote_database_name,
        remote_table_name,
        context_);

    Block sample_block;
    for (const String & column_name : column_names_)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);

        WhichDataType which(column_data.type);
        /// Convert enum to string.
        if (which.isEnum())
            column_data.type = std::make_shared<DataTypeString>();
        sample_block.insert({ column_data.type, column_data.name });
    }


    StreamSettings mysql_input_stream_settings(context_->getSettingsRef(), mysql_settings.connection_auto_close);

    return Pipe(std::make_shared<SourceFromInputStream>(
            std::make_shared<MySQLWithFailoverBlockInputStream>(pool, query, sample_block, mysql_input_stream_settings)));
}


class StorageMySQLBlockOutputStream : public IBlockOutputStream
{
public:
    explicit StorageMySQLBlockOutputStream(
        const StorageMySQL & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const std::string & remote_database_name_,
        const std::string & remote_table_name_,
        const mysqlxx::PoolWithFailover::Entry & entry_,
        const size_t & mysql_max_rows_to_insert)
        : storage{storage_}
        , metadata_snapshot{metadata_snapshot_}
        , remote_database_name{remote_database_name_}
        , remote_table_name{remote_table_name_}
        , entry{entry_}
        , max_batch_rows{mysql_max_rows_to_insert}
    {
    }

    Block getHeader() const override { return metadata_snapshot->getSampleBlock(); }

    void write(const Block & block) override
    {
        auto blocks = splitBlocks(block, max_batch_rows);
        mysqlxx::Transaction trans(entry);
        try
        {
            for (const Block & batch_data : blocks)
            {
                writeBlockData(batch_data);
            }
            trans.commit();
        }
        catch (...)
        {
            trans.rollback();
            throw;
        }
    }

    void writeBlockData(const Block & block)
    {
        WriteBufferFromOwnString sqlbuf;
        sqlbuf << (storage.replace_query ? "REPLACE" : "INSERT") << " INTO ";
        if (!remote_database_name.empty())
            sqlbuf << backQuoteMySQL(remote_database_name) << ".";
        sqlbuf << backQuoteMySQL(remote_table_name);
        sqlbuf << " (" << dumpNamesWithBackQuote(block) << ") VALUES ";

        auto writer = FormatFactory::instance().getOutputStream("Values", sqlbuf, metadata_snapshot->getSampleBlock(), storage.getContext());
        writer->write(block);

        if (!storage.on_duplicate_clause.empty())
            sqlbuf << " ON DUPLICATE KEY " << storage.on_duplicate_clause;

        sqlbuf << ";";

        auto query = this->entry->query(sqlbuf.str());
        query.execute();
    }

    Blocks splitBlocks(const Block & block, const size_t & max_rows) const
    {
        /// Avoid Excessive copy when block is small enough
        if (block.rows() <= max_rows)
            return Blocks{std::move(block)};

        const size_t split_block_size = ceil(block.rows() * 1.0 / max_rows);
        Blocks split_blocks(split_block_size);

        for (size_t idx = 0; idx < split_block_size; ++idx)
            split_blocks[idx] = block.cloneEmpty();

        const size_t columns = block.columns();
        const size_t rows = block.rows();
        size_t offsets = 0;
        UInt64 limits = max_batch_rows;
        for (size_t idx = 0; idx < split_block_size; ++idx)
        {
            /// For last batch, limits should be the remain size
            if (idx == split_block_size - 1)
                limits = rows - offsets;
            for (size_t col_idx = 0; col_idx < columns; ++col_idx)
            {
                split_blocks[idx].getByPosition(col_idx).column = block.getByPosition(col_idx).column->cut(offsets, limits);
            }
            offsets += max_batch_rows;
        }

        return split_blocks;
    }

    static std::string dumpNamesWithBackQuote(const Block & block)
    {
        WriteBufferFromOwnString out;
        for (auto it = block.begin(); it != block.end(); ++it)
        {
            if (it != block.begin())
                out << ", ";
            out << backQuoteMySQL(it->name);
        }
        return out.str();
    }

private:
    const StorageMySQL & storage;
    StorageMetadataPtr metadata_snapshot;
    std::string remote_database_name;
    std::string remote_table_name;
    mysqlxx::PoolWithFailover::Entry entry;
    size_t max_batch_rows;
};


BlockOutputStreamPtr StorageMySQL::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    return std::make_shared<StorageMySQLBlockOutputStream>(
        *this,
        metadata_snapshot,
        remote_database_name,
        remote_table_name,
        pool->get(),
        local_context->getSettingsRef().mysql_max_rows_to_insert);
}

 void StorageMySQL::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /*context*/) const
 {
    if (commands.size() != 1 || commands.front().type != AlterCommand::CHANGE_ENGINE)
        throw Exception("StorageMySQL only support CHANGE_ENGINE command", ErrorCodes::NOT_IMPLEMENTED);
 }

void StorageMySQL::alter(const AlterCommands & params, ContextPtr query_context, TableLockHolder & /*alter_lock_holder*/)
{
    auto & engine_func = params.front().engine->as<ASTFunction&>();
    if (engine_func.name != "MySQL")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Change Engine only support modify engine from MySQL to MySQL, but got {}", engine_func.name);
    if (!engine_func.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown arguments for new MySQL engine");

    auto engine_args = engine_func.arguments->children;

    if (engine_args.size() < 5 || engine_args.size() > 7)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Storage MySQL requires 5-7 parameters: MySQL('host:port' (or addresses_pattern), database, table, user, password[, replace_query, on_duplicate_clause]), but got {}",
            engine_args.size());

    for (auto & engine_arg : engine_args)
        engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, query_context);

    const String & new_host_ports = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
    const String & new_remote_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
    const String & new_remote_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
    const String & username = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
    const String & password = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();
    size_t max_addresses = query_context->getSettingsRef().glob_expansion_max_elements;

    auto addresses = parseRemoteDescriptionForExternalDatabase(new_host_ports, max_addresses, 3306);
    auto new_pool = std::make_shared<mysqlxx::PoolWithFailover>(
        new_remote_database, addresses,
        username, password,
        MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_START_CONNECTIONS,
        mysql_settings.connection_pool_size,
        mysql_settings.connection_max_tries);

    /// check and get new table schema.
    auto table_definition = getCreateTableSql();
    const char * begin = table_definition.data();
    const char * end = table_definition.data() + table_definition.size();
    ParserQueryWithOutput parser{end};
    ASTPtr ast_query = parseQuery(parser, begin, end, "CreateMySQL", 0, 0);

    /// replace ast_storage
    auto & new_ast_create = ast_query->as<ASTCreateQuery &>();
    auto new_ast_storage = std::make_shared<ASTStorage>();
    new_ast_storage->set(new_ast_storage->engine, params.front().engine);
    new_ast_create.replace(new_ast_create.storage, new_ast_storage);

    /// replace engine in memory
    if (new_remote_database != remote_database_name)
        remote_database_name = new_remote_database;
    if (new_remote_table != remote_table_name)
        remote_table_name = new_remote_table;

    pool = std::move(new_pool);

    if (engine_args.size() >= 6)
        replace_query = engine_args[5]->as<ASTLiteral &>().value.safeGet<UInt64>();
    if (engine_args.size() == 7)
        on_duplicate_clause = engine_args[6]->as<ASTLiteral &>().value.safeGet<String>();

    LOG_DEBUG(logger, "Updated engine for StorageMySQL in memory.");

    /// replace table schema in catalog
    TransactionCnchPtr txn = query_context->getCurrentTransaction();
    auto action = txn->createAction<DDLAlterAction>(shared_from_this(), query_context->getSettingsRef(), query_context->getCurrentQueryId());
    auto & alter_act = action->as<DDLAlterAction &>();

    alter_act.setOldSchema(table_definition);
    auto new_schema = queryToString(new_ast_create);
    alter_act.setNewSchema(new_schema);

    txn->appendAction(action);
    txn->commitV1();
    LOG_DEBUG(logger, "Updated shared metadata in Catalog.");
}

void registerStorageMySQL(StorageFactory & factory)
{
    factory.registerStorage("MySQL", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() < 5 || engine_args.size() > 7)
            throw Exception(
                "Storage MySQL requires 5-7 parameters: MySQL('host:port' (or 'addresses_pattern'), database, table, 'user', 'password'[, replace_query, 'on_duplicate_clause']).",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

        /// 3306 is the default MySQL port.
        const String & host_port = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        const String & remote_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        const String & remote_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        const String & username = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
        const String & password = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();
        size_t max_addresses = args.getContext()->getSettingsRef().glob_expansion_max_elements;

        /// TODO: move some arguments from the arguments to the SETTINGS.
        MySQLSettings mysql_settings;
        if (args.storage_def->settings)
        {
            mysql_settings.loadFromQuery(*args.storage_def);
        }

        if (!mysql_settings.connection_pool_size)
            throw Exception("connection_pool_size cannot be zero.", ErrorCodes::BAD_ARGUMENTS);

        auto addresses = parseRemoteDescriptionForExternalDatabase(host_port, max_addresses, 3306);
        mysqlxx::PoolWithFailover pool(remote_database, addresses,
            username, password,
            MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_START_CONNECTIONS,
            mysql_settings.connection_pool_size,
            mysql_settings.connection_max_tries);

        bool replace_query = false;
        std::string on_duplicate_clause;
        if (engine_args.size() >= 6)
            replace_query = engine_args[5]->as<ASTLiteral &>().value.safeGet<UInt64>();
        if (engine_args.size() == 7)
            on_duplicate_clause = engine_args[6]->as<ASTLiteral &>().value.safeGet<String>();

        if (replace_query && !on_duplicate_clause.empty())
            throw Exception(
                "Only one of 'replace_query' and 'on_duplicate_clause' can be specified, or none of them",
                ErrorCodes::BAD_ARGUMENTS);

        return StorageMySQL::create(
            args.table_id,
            std::move(pool),
            remote_database,
            remote_table,
            replace_query,
            on_duplicate_clause,
            args.columns,
            args.constraints,
            args.comment,
            args.getContext(),
            mysql_settings);
    },
    {
        .supports_settings = true,
        .source_access_type = AccessType::MYSQL,
    });
}

}

#endif
