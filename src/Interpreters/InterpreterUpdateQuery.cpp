#include<Interpreters/InterpreterUpdateQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/trySetVirtualWarehouse.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <Transaction/ICnchTransaction.h>
#include <IO/WriteBufferFromString.h>
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

InterpreterUpdateQuery::InterpreterUpdateQuery(const ASTPtr & query_ptr_, ContextPtr context_)
    : WithContext(context_),
      query_ptr(query_ptr_),
      log(&Poco::Logger::get("InterpreterUpdateQuery"))
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

    ASTPtr insert_ast = transformToInterpreterInsertQuery(table); 

    InterpreterInsertQuery interpreter_insert(insert_ast, getContext());

    return interpreter_insert.execute();
}


ASTPtr InterpreterUpdateQuery::prepareInterpreterSelectQuery(const StoragePtr & storage)
{
    auto res = std::make_shared<ASTSelectQuery>();
    const auto & update = query_ptr->as<ASTUpdateQuery &>();

    auto metadata_ptr = storage->getInMemoryMetadataPtr();

    // get all columns involved in partition keys or unique keys
    NameSet immutable_columns;
    for (const auto & key : metadata_ptr->getColumnsRequiredForUniqueKey())
        immutable_columns.emplace(key);
    for (const auto & key : metadata_ptr->getColumnsRequiredForPartitionKey())
        immutable_columns.emplace(key);

    NameSet ordinary_columns;
    for (const auto & column : metadata_ptr->getColumns().getOrdinary())
        ordinary_columns.emplace(column.name);

    //collect assinments
    std::unordered_map<String, ASTPtr> assignments;
    for (const auto & child : update.assignment_list->children)
    {
        if (const ASTAssignment * assinment = child->as<ASTAssignment>())
        {
            if (immutable_columns.count(assinment->column_name))
                throw Exception("Updating parition/unique keys is not allowed.", ErrorCodes::BAD_ARGUMENTS);

            if (!ordinary_columns.count(assinment->column_name))
                throw Exception("There is no column named " + assinment->column_name, ErrorCodes::BAD_ARGUMENTS);

            assignments.emplace(assinment->column_name, assinment->expression()->clone());
        }
        else
            throw Exception("Syntax error in update statement. " + child->getID(), ErrorCodes::SYNTAX_ERROR);
    }

    auto select_list = std::make_shared<ASTExpressionList>();

    for (const auto & ordinary_column : metadata_ptr->getColumns().getOrdinary())
    {
        ASTPtr element;
        if (assignments.count(ordinary_column.name))
            element = assignments[ordinary_column.name];
        else
            element = std::make_shared<ASTIdentifier>(ordinary_column.name);
        select_list->children.push_back(element);
    }

    res->setExpression(ASTSelectQuery::Expression::SELECT, select_list);
    res->setExpression(ASTSelectQuery::Expression::WHERE, update.where_condition->clone());

    if (update.order_by_expr)
        res->setExpression(ASTSelectQuery::Expression::ORDER_BY, update.order_by_expr->clone());
    if (update.limit_value)
        res->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, update.limit_value->clone());
    if (update.limit_offset)
        res->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, update.limit_offset->clone());

    res->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables = res->tables();
    auto tables_elem = std::make_shared<ASTTablesInSelectQueryElement>();
    auto table_expr = std::make_shared<ASTTableExpression>();
    tables->children.push_back(tables_elem);
    tables_elem->table_expression = table_expr;
    tables_elem->children.push_back(table_expr);
    table_expr->database_and_table_name = std::make_shared<ASTTableIdentifier>(storage->getDatabaseName(), storage->getTableName());
    table_expr->children.push_back(table_expr->database_and_table_name);

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
