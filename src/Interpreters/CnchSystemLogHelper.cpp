#include <Interpreters/CnchSystemLogHelper.h>
#include <Transaction/Actions/DDLCreateAction.h>
#include <Catalog/Catalog.h>
#include <Common/SettingsChanges.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/formatAST.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Transaction/TransactionCoordinatorRcCnch.h>
#include <CloudServices/CnchCreateQueryHelper.h>


namespace DB
{

bool createDatabaseInCatalog(
    ContextPtr global_context,
    const String & database_name,
    Poco::Logger& logger)
{
    bool ret = true;
    try
    {
        TxnTimestamp start_time = global_context->getTimestamp();
        auto catalog = global_context->getCnchCatalog();
        if (!catalog->isDatabaseExists(database_name, start_time))
        {
            try
            {
                LOG_INFO(&logger, "Creating database {} in catalog", database_name);
                catalog->createDatabase(database_name, UUIDHelpers::generateV4(), start_time, start_time);
            }
            catch (Exception & e)
            {
                LOG_WARNING(&logger, "Failed to create database {}, got exception {}", database_name, e.message());
                ret = false;
            }
        }
    }
    catch (Exception & e)
    {
        LOG_WARNING(&logger, "Unable to get timestamp, got exception: {}", e.message());
        ret = false;
    }

    return ret;
}

String makeAlterColumnQuery(const String& database, const String& table, const Block& expected, const Block& actual)
{
    if (blocksHaveEqualStructure(actual, expected))
        return {};

    Names expected_names = expected.getNames();
    std::sort(expected_names.begin(), expected_names.end());

    Names actual_names = actual.getNames();
    std::sort(actual_names.begin(), actual_names.end());

    Names adding_columns;
    std::set_difference(
        expected_names.begin(),
        expected_names.end(),
        actual_names.begin(),
        actual_names.end(),
        std::inserter(adding_columns, adding_columns.begin())
    );

    Names dropping_columns;
    std::set_difference(
        actual_names.begin(),
        actual_names.end(),
        expected_names.begin(),
        expected_names.end(),
        std::inserter(dropping_columns, dropping_columns.begin())
    );

    Names intersection_columns;
    std::set_intersection(
        actual_names.begin(),
        actual_names.end(),
        expected_names.begin(),
        expected_names.end(),
        std::inserter(intersection_columns, intersection_columns.begin())
    );

    Names type_changing_columns;
    for (const String& s : intersection_columns)
    {
        const ColumnWithTypeAndName& expected_type_and_name = expected.getByName(s);
        const ColumnWithTypeAndName& actual_type_and_name = actual.getByName(s);

        if (expected_type_and_name.type && actual_type_and_name.type)
        {
            if (expected_type_and_name.type->getName() != actual_type_and_name.type->getName())
            {
                type_changing_columns.push_back(s);
            }
        }
    }

    /// find name_after: ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after | FIRST]
    std::vector<std::pair<std::string, std::string>> name_after_map;
    if (!adding_columns.empty())
    {
        Names unsorted_expected_names = expected.getNames();
        std::string name_after_expr = "FIRST";
        for (const std::string & s : unsorted_expected_names)
        {
            auto it = std::find(std::begin(adding_columns), std::end(adding_columns), s);
            if (it != std::end(adding_columns))
                name_after_map.push_back(std::make_pair(*it, name_after_expr));

            name_after_expr = "AFTER "+ s;
        }
    }

    if (!name_after_map.empty() || !dropping_columns.empty() || !type_changing_columns.empty())
    {
        String alter_query = "ALTER TABLE " + database + "." + table;

        String separator = "";
        for (const std::string & s : dropping_columns)
        {
            alter_query += separator + " DROP COLUMN " + backQuoteIfNeed(s);
            separator = ",";
        }

        for (const std::string & s : type_changing_columns)
        {
            const ColumnWithTypeAndName& type_and_name = expected.getByName(s);
            if (type_and_name.type)
            {
                alter_query += separator + " MODIFY COLUMN " + backQuoteIfNeed(s)
                                + " " + type_and_name.type->getName();
                separator = ",";
            }
        }

        for (const auto & p : name_after_map)
        {
            const ColumnWithTypeAndName& type_and_name = expected.getByName(p.first);
            if (type_and_name.type)
            {
                alter_query += separator + " ADD COLUMN " + backQuoteIfNeed(p.first)
                                + " " + type_and_name.type->getName()
                                + " " + p.second;
                separator = ",";
            }
        }

        return alter_query;
    }

    return {};
}

AlterTTLType getAlterTTLType(const String& ttl, StoragePtr& storage, Poco::Logger& logger)
{
    auto merge_tree_storage = dynamic_pointer_cast<MergeTreeMetaBase>(storage);
    if (merge_tree_storage)
    {
        ParserExpression expression_parser;
        ASTPtr ttl_table;
        Expected expected;
        Tokens tokens(ttl.data(), ttl.data() + ttl.size(), ttl.size());
        IParser::Pos token_iterator(tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH);

        auto parsed = expression_parser.parse(token_iterator, ttl_table, expected);
        String merge_storage_ttl;

        if (parsed && merge_tree_storage->getInMemoryMetadataPtr()->getTableTTLs().definition_ast)
        {
            merge_storage_ttl = queryToString(merge_tree_storage->getInMemoryMetadataPtr()->getTableTTLs().definition_ast);
            merge_storage_ttl.erase(std::remove(merge_storage_ttl.begin(), merge_storage_ttl.end(), '`'),
                                    merge_storage_ttl.end());
        }

        if (!ttl.empty() && !(merge_tree_storage->getInMemoryMetadataPtr()->getTableTTLs().definition_ast))
        {
            return AlterTTLType::add_ttl;
        }
        else if (!ttl.empty()
                && (parsed && merge_tree_storage->getInMemoryMetadataPtr()->getTableTTLs().definition_ast &&
                    (queryToString(ttl_table) != merge_storage_ttl)))
        {
            return AlterTTLType::modify_ttl;
        }
        else if (ttl.empty() && merge_tree_storage->getInMemoryMetadataPtr()->getTableTTLs().definition_ast)
        {
            return AlterTTLType::delete_ttl;
        }
    }
    else
    {
        LOG_DEBUG(&logger, "Non-MergeTreeMetaBase storage found. This should not happen.");
    }
    return AlterTTLType::none;
}

String makeAlterTTLQuery(const String & database, const String & table, const String & ttl, AlterTTLType type, Poco::Logger& logger)
{
    switch (type)
    {
        case AlterTTLType::add_ttl:
            LOG_DEBUG(&logger, "Generating query to add TTL");
            return "ALTER TABLE " + database + "." +  table + " MODIFY TTL " + ttl;
        case AlterTTLType::modify_ttl:
            LOG_DEBUG(&logger, "Generating query to modify TTL");
            return "ALTER TABLE " + database + "." +  table + " MODIFY TTL " + ttl;
        case AlterTTLType::delete_ttl:
            LOG_DEBUG(&logger, "Generating query to delete TTL");
            return "ALTER TABLE " + database + "." +  table + " REMOVE TTL";
        default:
            return "";
    }
}

String makeAlterSettingsQuery(
    const String & database,
    const String & table,
    StoragePtr storage,
    const SettingsChanges & changes,
    Poco::Logger & logger)
{
    ParserCreateQuery parser;
    auto create_query_string = storage->getCreateTableSql();
    ASTPtr ast = parseQuery(parser, create_query_string, "for table " + table, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    auto ast_create_query = ast->as<ASTCreateQuery &>();

    auto & storage_settings = ast_create_query.storage->settings->changes;

    String alter_query = "ALTER TABLE " + database + "." + table + " MODIFY SETTING ";

    bool has_change = false;

    for (const auto & change : changes)
    {
        auto it = std::find_if(storage_settings.begin(), storage_settings.end(), [&change](auto & c) { return c.name == change.name; });
        if ((it != storage_settings.end() && it->value != change.value)
        || it == storage_settings.end())
        {
            has_change = true;
            // FIXME: Also allow for other change values if needed
            UInt64 uint_val;
            String str_val;
            if (change.value.tryGet<UInt64>(uint_val))
                alter_query +=  change.name + " = " + std::to_string(uint_val) + ", ";
            else if (change.value.tryGet<String>(str_val))
                alter_query +=  change.name + " = " + str_val + ", ";
            else
                LOG_DEBUG(&logger, "Encountered unexpected setting type in alter settings query"+ String(change.value.getTypeName()));
        }
    }

    if (alter_query.ends_with(", "))
        alter_query = alter_query.substr(0, alter_query.length() - 2);

    if (has_change)
        return alter_query;
    else
        return String{};
}

bool createCnchTable(
    ContextPtr global_context,
    const String & database,
    const String & table,
    const String & query,
    Poco::Logger & logger)
{
    bool ret = true;
    ParserCreateQuery parser;
    const String table_description = database + "." + table;
    ASTPtr ast = parseQuery(parser, query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    if (!ast)
    {
        LOG_ERROR(&logger, "Failed to create CNCH table {}, failed to parse create query: {}", table_description, query);
        return false;
    }

    auto query_context = Context::createCopy(global_context);
    query_context->makeSessionContext();
    query_context->makeQueryContext();

    auto & create = ast->as<ASTCreateQuery &>();
    create.uuid = UUIDHelpers::generateV4();

    WriteBufferFromOwnString statement_buf;
    formatAST(create, statement_buf, false);
    writeChar('\n', statement_buf);
    String create_table_sql = statement_buf.str();

    auto & txn_coordinator = global_context->getCnchTransactionCoordinator();
    TransactionCnchPtr txn = txn_coordinator.createTransaction(CreateTransactionOption().setContext(query_context));

    CreateActionParams params = {database, table, create.uuid, create_table_sql};
    auto create_table = txn->createAction<DDLCreateAction>(std::move(params));
    txn->appendAction(std::move(create_table));

    try
    {
        txn_coordinator.commitV1(txn);
    }
    catch (Exception & e)
    {
        LOG_WARNING(&logger, "Failed to create CNCH table " + table_description + ", got exception: " + e.message());
        ret = false;
    }

    txn_coordinator.finishTransaction(txn);
    return ret;
}

bool prepareCnchTable(
    ContextPtr global_context,
    const String & database,
    const String & table,
    const String & create_query,
    Poco::Logger& logger)
{
    auto catalog = global_context->getCnchCatalog();

    if (!catalog->isTableExists(database, table, TxnTimestamp::maxTS()))
    {
        LOG_INFO(&Poco::Logger::get("CnchSystemLog"), "Creating CNCH System log table: {}.{}", database, table);
        return createCnchTable(global_context, database, table, create_query, logger);
    }

    return true;
}

bool syncTableSchema(
    ContextPtr global_context,
    const String & database,
    const String & table,
    const Block & expected_block,
    const String & ttl,
    const SettingsChanges & expected_settings,
    Poco::Logger& logger)
{
    bool ret = true;
    auto catalog = global_context->getCnchCatalog();

    auto query_context = Context::createCopy(global_context);
    query_context->makeSessionContext();
    query_context->makeQueryContext();

    StoragePtr storage = nullptr;
    try
    {
        storage = catalog->getTable(*query_context, database, table, TxnTimestamp::maxTS());
    }
    catch (Exception & e)
    {
        LOG_WARNING(&logger, "Failed to get CNCH table " + database + "." + table + ", got exception: " + e.message());
        ret = false;
    }

    if (storage)
    {
        const Block actual_block = storage->getInMemoryMetadataPtr()->getSampleBlockNonMaterialized();
        auto execute_alter_query = [&, query_context] (const String & alter_query)
        {
            try
            {
                ParserAlterQuery parser;
                ASTPtr ast = parseQuery(parser, alter_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

                if (ast)
                {
                    TransactionCnchPtr txn = global_context->getCnchTransactionCoordinator().createTransaction();
                    if (txn)
                    {
                            LOG_INFO(&logger, "execute alter query: " + alter_query);
                            query_context->setCurrentTransaction(txn);
                            InterpreterAlterQuery intepreter_alter_query{ast, query_context};
                            intepreter_alter_query.execute();
                            query_context->getCnchTransactionCoordinator().finishTransaction(txn);
                    }
                }
            }
            catch (Exception & e)
            {
                LOG_WARNING(&logger, "Failed to alter table for cnch system log, got exception: " + e.message());
                return false;
            }
            return true;
        };
        if (!blocksHaveEqualStructure(actual_block, expected_block))
        {
            String alter_query = makeAlterColumnQuery(database, table, expected_block, actual_block);


            if (!alter_query.empty())
            {
                ret &= execute_alter_query(alter_query);
            }
        }

        // Alter table's TTL if the existing one is different
        auto alter_ttl_type = getAlterTTLType(ttl, storage, logger);
        if (alter_ttl_type != AlterTTLType::none)
        {
            ret &= execute_alter_query(makeAlterTTLQuery(database, table, ttl, alter_ttl_type, logger));
        }

        auto alter_settings_query = makeAlterSettingsQuery(database, table, storage, expected_settings, logger);
        if (!alter_settings_query.empty())
        {
            ret &= execute_alter_query(alter_settings_query);
        }

    }
    else
        ret = false;

    return ret;
}

bool createView(
    ContextPtr global_context,
    const String & database,
    const String & table,
    Poco::Logger& logger)
{
    bool ret = true;

    try
    {
        auto query_context = Context::createCopy(global_context);
        query_context->makeSessionContext();
        query_context->makeQueryContext();

        String create_view_query = "CREATE VIEW IF NOT EXISTS system." + table + " AS SELECT * from " + database + "." + table;
        ParserCreateQuery create_parser;
        ASTPtr create_view_ast = parseQuery(create_parser, create_view_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        InterpreterCreateQuery create_interpreter(create_view_ast, query_context);
        create_interpreter.execute();
        LOG_INFO(&logger, "Create view with query " + create_view_query + " successful!");
    }
    catch (Exception & e)
    {
        LOG_DEBUG(&logger, "Failed to create view for " + database + "." + table + ", met exception " + e.message());
        ret = false;
    }
    return ret;
}

}/// end namespace
