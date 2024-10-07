#include<Interpreters/InterpreterUpdateQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/trySetVirtualWarehouse.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <Transaction/ICnchTransaction.h>
#include <IO/WriteBufferFromString.h>
#include <Optimizer/PredicateUtils.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Access/ContextAccess.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int SYNTAX_ERROR;
    extern const int BAD_ARGUMENTS;
}

static ContextMutablePtr createQueryContext(ContextPtr context, bool use_optimizer)
{
    auto query_context = Context::createCopy(context);
    query_context->makeQueryContext();
    auto old_query_id = context->getInitialQueryId();
    auto new_query_id = old_query_id + "__create_stats_internal__" + UUIDHelpers::UUIDToString(UUIDHelpers::generateV4());
    query_context->setCurrentQueryId(new_query_id); // generate random query_id
    query_context->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;

    SettingsChanges changes;
    auto enable_optimizer = use_optimizer;
    changes.emplace_back("enable_optimizer", enable_optimizer);
    query_context->applySettingsChanges(changes);

    return query_context;
}

static inline BlockIO
getTableBlockIO(const StoragePtr & storage, ContextMutablePtr query_context)
{
    WriteBufferFromOwnString insert_columns_str;
    const NamesAndTypesList & insert_columns_names = storage->getInMemoryMetadataPtr()->getColumns().getOrdinary();

    for (auto iterator = insert_columns_names.begin(); iterator != insert_columns_names.end(); ++iterator)
    {
        if (iterator != insert_columns_names.begin())
            insert_columns_str << ", ";

        insert_columns_str << backQuoteIfNeed(iterator->name);
    }

    BlockIO res = executeQuery("INSERT INTO " + storage->getStorageID().getFullTableName() + "(" + insert_columns_str.str() + ")" + " VALUES",
        query_context, true);

    if (!res.out)
        throw Exception("LOGICAL ERROR: It is a bug.", ErrorCodes::LOGICAL_ERROR);

    return res;
}

InterpreterUpdateQuery::InterpreterUpdateQuery(const ASTPtr & query_ptr_, ContextPtr context_)
    : WithContext(context_),
      query_ptr(query_ptr_),
      log(getLogger("InterpreterUpdateQuery"))
{
}

BlockIO InterpreterUpdateQuery::execute()
{
    const auto & update = query_ptr->as<ASTUpdateQuery &>();
    auto table_id = getContext()->resolveStorageID(update, Context::ResolveOrdinary);
    getContext()->checkAccess(AccessType::ALTER_UPDATE, table_id);

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name, getContext());
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

    auto metadata_ptr = table->getInMemoryMetadataPtr();
    if (!metadata_ptr->hasUniqueKey())
        throw Exception("UPDATE statement only supports table with UNIQUE KEY.", ErrorCodes::NOT_IMPLEMENTED);

    auto table_lock = table->lockForShare(getContext()->getInitialQueryId(), getContext()->getSettingsRef().lock_acquire_timeout);

    if (getContext()->getSettingsRef().insert_if_not_exists)
        return executePartialUpdate(table);
    else
    {
        ASTPtr insert_ast = transformToInterpreterInsertQuery(table);
        LOG_DEBUG(log, "[Update] Convert to INSERT SELECT: {}", DB::serializeAST(*insert_ast));

        InterpreterInsertQuery interpreter_insert(
            insert_ast,
            getContext(),
            /*allow_materialized_*/false,
            /*no_squash_*/false,
            /*no_destination_*/false,
            AccessType::ALTER_UPDATE);

        return interpreter_insert.execute();
    }
}

static ASTTableExpression * getFirstTableExpression(const ASTUpdateQuery & update)
{
    if (!update.tables)
        return {};

    auto & tables_in_update_query = update.tables->as<ASTTablesInSelectQuery &>();
    if (tables_in_update_query.children.empty())
        return {};

    auto & tables_element = tables_in_update_query.children[0]->as<ASTTablesInSelectQueryElement &>();
    if (!tables_element.table_expression)
        return {};

    return tables_element.table_expression->as<ASTTableExpression>();
}

static String getTableExpressionAlias(const ASTTableExpression * table_expression)
{
    if (table_expression->subquery)
        return table_expression->subquery->tryGetAlias();
    else if (table_expression->table_function)
        return table_expression->table_function->tryGetAlias();
    else if (table_expression->database_and_table_name)
        return table_expression->database_and_table_name->tryGetAlias();

    return String();
}

ASTPtr InterpreterUpdateQuery::prepareInterpreterSelectQuery(const StoragePtr & storage)
{
    auto res = std::make_shared<ASTSelectQuery>();
    const auto & ast_update = query_ptr->as<ASTUpdateQuery &>();

    auto metadata_ptr = storage->getInMemoryMetadataPtr();

    /// get all columns involved in partition keys or unique keys
    NameSet immutable_columns;
    for (const auto & key : metadata_ptr->getColumnsRequiredForUniqueKey())
        immutable_columns.emplace(key);
    for (const auto & key : metadata_ptr->getColumnsRequiredForPartitionKey())
        immutable_columns.emplace(key);

    NameSet ordinary_columns;
    for (const auto & column : metadata_ptr->getColumns().getOrdinary())
        ordinary_columns.emplace(column.name);

    ColumnsDescription::ColumnOnUpdates onupdate_columns = metadata_ptr->getColumns().getOnUpdates();

    /// collect assignments
    std::unordered_map<String, ASTPtr> assignments;
    String update_table_alias;
    for (const auto & child : ast_update.assignment_list->children)
    {
        const ASTAssignment * assignment = child->as<ASTAssignment>();
        if (!assignment)
            throw Exception("Syntax error in update statement. " + child->getID(), ErrorCodes::SYNTAX_ERROR);

        if (const auto & t = assignment->table_name; !t.empty())
        {
            if (update_table_alias.empty())
                update_table_alias = t;
            else if (update_table_alias != t)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "UPDATE multi tables is not supported. Tables: {}, {}", update_table_alias, t);
        }

        if (immutable_columns.count(assignment->column_name))
            throw Exception("Updating partition/unique keys is not allowed.", ErrorCodes::BAD_ARGUMENTS);

        if (!ordinary_columns.count(assignment->column_name))
            throw Exception("There is no column named " + assignment->column_name, ErrorCodes::BAD_ARGUMENTS);

        assignments.emplace(assignment->column_name, assignment->expression()->clone());
    }

    auto select_list = std::make_shared<ASTExpressionList>();

    for (const auto & ordinary_column : metadata_ptr->getColumns().getOrdinary())
    {
        ASTPtr element;
        if (assignments.count(ordinary_column.name))
            element = assignments[ordinary_column.name];
        else if (onupdate_columns.count(ordinary_column.name))
            element = onupdate_columns[ordinary_column.name];
        else
            element = std::make_shared<ASTIdentifier>(ordinary_column.name);
        select_list->children.push_back(element);
    }

    res->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list));

    if (ast_update.where_condition)
        res->setExpression(ASTSelectQuery::Expression::WHERE, ast_update.where_condition->clone());
    if (ast_update.order_by_expr)
        res->setExpression(ASTSelectQuery::Expression::ORDER_BY, ast_update.order_by_expr->clone());
    if (ast_update.limit_value)
        res->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, ast_update.limit_value->clone());
    if (ast_update.limit_offset)
        res->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, ast_update.limit_offset->clone());

    if (ast_update.single_table)
    {
        if (!update_table_alias.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No table alias found: {}", update_table_alias);
        res->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
        auto tables = res->tables();
        auto tables_elem = std::make_shared<ASTTablesInSelectQueryElement>();
        auto table_expr = std::make_shared<ASTTableExpression>();
        tables->children.push_back(tables_elem);
        tables_elem->table_expression = table_expr;
        tables_elem->children.push_back(table_expr);
        table_expr->database_and_table_name = std::make_shared<ASTTableIdentifier>(storage->getDatabaseName(), storage->getTableName());
        table_expr->children.push_back(table_expr->database_and_table_name);
    }
    else
    {
        const auto & first_table = getFirstTableExpression(ast_update);
        auto first_table_alias = getTableExpressionAlias(first_table);

        /// Check that only the first table is updated.
        if (update_table_alias.empty())
        {
            /// By default, if update_table is empty, it means the first table is updated.
        }
        else
        {
            if (first_table_alias.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "SET with table alias but there is no table alias. {}, {}", update_table_alias);
            else if (first_table_alias != update_table_alias)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "It's only allowed to update the first table `{}`, but `{}` is given.", first_table_alias, update_table_alias);
        }

        res->setExpression(ASTSelectQuery::Expression::TABLES, ast_update.tables->clone());
    }

    return res;
}

BlockIO InterpreterUpdateQuery::executePartialUpdate(const StoragePtr & storage)
{
    const auto & ast_update = query_ptr->as<ASTUpdateQuery &>();
    /// Check partial update valid here
    if (!ast_update.single_table)
        throw Exception("Partial update is only valid for single table", ErrorCodes::BAD_ARGUMENTS);

    if (!ast_update.where_condition)
        throw Exception("Partial update needs where expression to indicate partition keys & unique keys", ErrorCodes::BAD_ARGUMENTS);

    auto cnch_table = dynamic_pointer_cast<StorageCnchMergeTree>(storage);
    if (!cnch_table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} is not cnch merge tree", storage->getStorageID().getNameForLogs());

    if (!cnch_table->getSettings()->partition_level_unique_keys)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PartialUpdate with partition_level_unique_keys=0 is not allowed");

    auto metadata_ptr = storage->getInMemoryMetadataPtr();
    /// get all columns involved in partition keys or unique keys
    NameSet immutable_columns;
    for (const auto & key : metadata_ptr->getColumnsRequiredForUniqueKey())
        immutable_columns.emplace(key);
    for (const auto & key : metadata_ptr->getColumnsRequiredForPartitionKey())
        immutable_columns.emplace(key);

    NameSet ordinary_columns;
    for (const auto & column : metadata_ptr->getColumns().getOrdinary())
        ordinary_columns.emplace(column.name);

    std::unordered_map<String, ASTPtr> name_to_expression_map;
    for (const auto & conjunct : PredicateUtils::extractConjuncts(ast_update.where_condition))
    {
        const auto * func = conjunct->as<ASTFunction>();
        if (!func || func->name != "equals")
            continue;
        const auto * column = func->arguments->children[0]->as<ASTIdentifier>();
        if (!column || !ordinary_columns.count(column->name()))
            continue;
        if (func->arguments->children[1]->getType() != ASTType::ASTLiteral)
            continue;
        name_to_expression_map[column->name()] = func->arguments->children[1];
    }

    /// check if partition keys and unique keys are all in the name_to_expression_map
    for (const auto & required_key : immutable_columns)
    {
        if (!name_to_expression_map.count(required_key))
            throw Exception("Partial update needs explicitly specify partition keys & unique keys", ErrorCodes::BAD_ARGUMENTS);
    }

    ASTPtr query = prepareInterpreterSelectQuery(storage);
    auto & select_ast = query->as<ASTSelectQuery &>();
    select_ast.setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::make_shared<ASTLiteral>(1));
    select_ast.setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, std::make_shared<ASTLiteral>(0));
    LOG_DEBUG(log, "Convert to SELECT to judge whether row exists, query: {}", DB::serializeAST(*query));

    /// Internal SQL doesn't work well in optimizer mode, mainly due to PlanSegmentExecutor
    /// XXX: This query is not a complex join query, so we will use the non-optimizer mode temporarily.
    auto block_io = getTableBlockIO(storage, createQueryContext(getContext(), /*use_optimizer*/false));
    auto basic_input_stream = InterpreterFactory::get(query, createQueryContext(getContext(), /*use_optimizer*/false))->execute().getInputStream();

    bool found_data = true;
    Block block = basic_input_stream->read();
    if (!block || block.rows() == 0)
        found_data = false;

    if (found_data)
    {
        LOG_TRACE(log, "Data already exists in storage: {}, do update.", storage->getStorageID().getNameForLogs());
        /// insert directly rather than using insert select
        auto input_stream = ConvertingBlockInputStream(std::make_shared<OneBlockInputStream>(block), block_io.out->getHeader(), ConvertingBlockInputStream::MatchColumnsMode::Position);
        copyData(input_stream, *block_io.out);
        return BlockIO{};
    }
    else
    {
        ASTPtr insert_ast = prepareInsertQueryForPartialUpdate(storage, name_to_expression_map);
        LOG_DEBUG(log, "[PartialUpdate] Convert to INSERT SELECT: {}", DB::serializeAST(*insert_ast));

        InterpreterInsertQuery interpreter_insert(
            insert_ast,
            getContext(),
            /*allow_materialized_*/false,
            /*no_squash_*/false,
            /*no_destination_*/false,
            AccessType::ALTER_UPDATE);

        return interpreter_insert.execute();
    }
}

ASTPtr InterpreterUpdateQuery::prepareInsertQueryForPartialUpdate(const StoragePtr & storage, const std::unordered_map<String, ASTPtr> & name_to_expression_map)
{
    auto res = std::make_shared<ASTInsertQuery>();
    const auto & ast_update = query_ptr->as<ASTUpdateQuery &>();

    res->table_id = storage->getStorageID();

    auto metadata_ptr = storage->getInMemoryMetadataPtr();
    /// get all columns involved in partition keys or unique keys
    NameSet immutable_columns;
    for (const auto & key : metadata_ptr->getColumnsRequiredForUniqueKey())
        immutable_columns.emplace(key);
    for (const auto & key : metadata_ptr->getColumnsRequiredForPartitionKey())
        immutable_columns.emplace(key);

    NameSet ordinary_columns;
    for (const auto & column : metadata_ptr->getColumns().getOrdinary())
        ordinary_columns.emplace(column.name);

    /// collect assignments
    std::unordered_map<String, ASTPtr> assignments;
    for (const auto & child : ast_update.assignment_list->children)
    {
        if (const ASTAssignment * assignment = child->as<ASTAssignment>())
        {
            if (immutable_columns.count(assignment->column_name))
                throw Exception("Updating partition/unique keys is not allowed.", ErrorCodes::BAD_ARGUMENTS);

            if (!ordinary_columns.count(assignment->column_name))
                throw Exception("There is no column named " + assignment->column_name, ErrorCodes::BAD_ARGUMENTS);

            assignments.emplace(assignment->column_name, assignment->expression()->clone());
        }
        else
            throw Exception("Syntax error in update statement. " + child->getID(), ErrorCodes::SYNTAX_ERROR);
    }

    auto insert_list = std::make_shared<ASTExpressionList>();
    auto select_list = std::make_shared<ASTExpressionList>();

    for (const auto & entry : assignments)
    {
        insert_list->children.push_back(std::make_shared<ASTIdentifier>(entry.first));
        select_list->children.push_back(entry.second);
    }

    for (const auto & entry : name_to_expression_map)
    {
        insert_list->children.push_back(std::make_shared<ASTIdentifier>(entry.first));
        select_list->children.push_back(entry.second);
    }

    res->columns = std::move(insert_list);
    auto select_ast = std::make_shared<ASTSelectQuery>();
    select_ast->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list));
    res->select = select_ast;

    auto & settings_ast = query_ptr->as<ASTUpdateQuery &>().settings_ast;
    if (settings_ast)
        res->settings_ast = query_ptr->as<ASTUpdateQuery &>().settings_ast->clone();
    return res;
}

/***
 * Change UPDATE statement to INSERT SELECT query:
 * eg: (with table t(p_date, uk, c1, c2))
 *  UPDATE t SET c1 = c2, c2 = c1 WHERE p_date = '2023-01-01'
 *  =>
 *  INSERT INTO t SELECT p_date, ul, c2, c1 WHERE p_date = '2023-01-01'
*/
ASTPtr InterpreterUpdateQuery::transformToInterpreterInsertQuery(const StoragePtr & storage)
{
    auto res = std::make_shared<ASTInsertQuery>();

    res->table_id = storage->getStorageID();
    res->select = prepareInterpreterSelectQuery(storage);
    auto & settings_ast = query_ptr->as<ASTUpdateQuery &>().settings_ast;
    if (settings_ast)
        res->settings_ast = query_ptr->as<ASTUpdateQuery &>().settings_ast->clone();
    return res;
}

}

/// UPDATE customer LEFT JOIN new_customer ON customer.customer_id = new_customer.customer_id SET customer.customer_age = 42 WHERE new_customer.customer_id = 1;
