#include <Access/AccessFlags.h>
#include <Columns/ColumnString.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterShowCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/formatAST.h>
#include <Parsers/formatTenantDatabaseName.h>
#include <Storages/IStorage.h>
#include <Common/typeid_cast.h>
#include "ExternalCatalog/IExternalCatalogMgr.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int THERE_IS_NO_QUERY;
    extern const int BAD_ARGUMENTS;
}

BlockIO InterpreterShowCreateQuery::execute()
{
    BlockIO res;
    if (auto * show_query = query_ptr->as<ASTShowCreateExternalCatalogQuery>())
    {
        res.in = executeForExternalImpl();
    }
    else if (auto * external_table_query = query_ptr->as<ASTShowCreateExternalTableQuery>())
    {
        res.in = executeForExternalTableImpl();
    }
    else
    {
        res.in = executeImpl();
    }
    return res;
}


Block InterpreterShowCreateQuery::getSampleBlock()
{
    return Block{{ColumnString::create(), std::make_shared<DataTypeString>(), "statement"}};
}

BlockInputStreamPtr InterpreterShowCreateQuery::executeForExternalImpl()
{
    auto catalog_name = query_ptr->as<ASTShowCreateExternalCatalogQuery>()->catalog;
    auto create_query =ExternalCatalog::Mgr::instance().getCatalogCreateQuery(catalog_name);
    if(!create_query.has_value() || create_query->empty()){
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "catalog {} does not exist",catalog_name);
    }
    MutableColumnPtr column = ColumnString::create();
    column->insert(create_query.value());
    return std::make_shared<OneBlockInputStream>(Block{{std::move(column), std::make_shared<DataTypeString>(), "statement"}});
}

BlockInputStreamPtr InterpreterShowCreateQuery::executeForExternalTableImpl()
{
    auto * query = query_ptr->as<ASTShowCreateExternalTableQuery>();
    auto catalog_name = query->catalog;
    auto catalog_ptr = ExternalCatalog::Mgr::instance().tryGetCatalog(catalog_name);
    if (!catalog_ptr)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "catalog {} does not exist", catalog_name);
    }
    auto table = catalog_ptr->getTable(query->database, query->table, getContext());
    auto create_query = table->getCreateTableSql();
    MutableColumnPtr column = ColumnString::create();
    column->insert(create_query);
    return std::make_shared<OneBlockInputStream>(Block{{std::move(column), std::make_shared<DataTypeString>(), "statement"}});
}


BlockInputStreamPtr InterpreterShowCreateQuery::executeImpl()
{
    ASTPtr create_query;
    ASTQueryWithTableAndOutput * show_query;
    if ((show_query = query_ptr->as<ASTShowCreateTableQuery>()) || (show_query = query_ptr->as<ASTShowCreateViewQuery>())
        || (show_query = query_ptr->as<ASTShowCreateDictionaryQuery>()))
    {
        auto resolve_table_type = show_query->temporary ? Context::ResolveExternal : Context::ResolveOrdinary;
        auto table_id = getContext()->resolveStorageID(*show_query, resolve_table_type);

        bool is_dictionary = static_cast<bool>(query_ptr->as<ASTShowCreateDictionaryQuery>());

        if (is_dictionary)
            getContext()->checkAccess(AccessType::SHOW_DICTIONARIES, table_id);
        else
            getContext()->checkAccess(AccessType::SHOW_COLUMNS, table_id);

        create_query = DatabaseCatalog::instance()
                           .getDatabase(table_id.database_name, getContext())
                           ->getCreateTableQuery(table_id.table_name, getContext());

        auto & ast_create_query = create_query->as<ASTCreateQuery &>();
        getOriginalDatabaseName(ast_create_query.database).swap(ast_create_query.database);
        if (query_ptr->as<ASTShowCreateViewQuery>())
        {
            if (!ast_create_query.isView())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "{}.{} is not a VIEW",
                    backQuote(ast_create_query.database),
                    backQuote(ast_create_query.table));
        }
        else if (is_dictionary)
        {
            if (!ast_create_query.is_dictionary)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "{}.{} is not a DICTIONARY",
                    backQuote(ast_create_query.database),
                    backQuote(ast_create_query.table));
        }
    }
    else if ((show_query = query_ptr->as<ASTShowCreateDatabaseQuery>()))
    {
        if (show_query->temporary)
            throw Exception("Temporary databases are not possible.", ErrorCodes::SYNTAX_ERROR);
        show_query->database = getContext()->resolveDatabase(show_query->database);
        getContext()->checkAccess(AccessType::SHOW_DATABASES, show_query->database);
        create_query = DatabaseCatalog::instance().getDatabase(show_query->database, getContext())->getCreateDatabaseQuery();
        auto & ast_create_query = create_query->as<ASTCreateQuery &>();
        getOriginalDatabaseName(ast_create_query.database).swap(ast_create_query.database);
    }

    if (!create_query)
        throw Exception(
            "Unable to show the create query of " + show_query->table + ". Maybe it was created by the system.",
            ErrorCodes::THERE_IS_NO_QUERY);

    if (!getContext()->getSettingsRef().show_table_uuid_in_table_create_query_if_not_nil)
    {
        auto & create = create_query->as<ASTCreateQuery &>();
        create.uuid = UUIDHelpers::Nil;
        create.to_inner_uuid = UUIDHelpers::Nil;
    }

    String res = create_query->formatWithHiddenSecrets(/* max_length= */ 0, /* one_line= */ false, /*no_alias*/ false, getContext()->getSettingsRef().dialect_type, /*remove_tenant_id*/true);

    MutableColumnPtr column = ColumnString::create();
    column->insert(res);

    if (getContext()->getSettingsRef().dialect_type == DialectType::MYSQL) {
        MutableColumnPtr name_column = ColumnString::create();
        if ((show_query = query_ptr->as<ASTShowCreateTableQuery>()))
        {
            name_column->insert(show_query->table);
            return std::make_shared<OneBlockInputStream>(Block{{move(name_column), std::make_shared<DataTypeString>(), "Table"},
                                                                {std::move(column), std::make_shared<DataTypeString>(), "Create Table"}});
        }
        else if ((show_query = query_ptr->as<ASTShowCreateViewQuery>()))
        {
            name_column->insert(show_query->table);
            MutableColumnPtr character_set_client_column = ColumnString::create();
            MutableColumnPtr collation_connection_column = ColumnString::create();
            // TODO: Replace with actual values
            character_set_client_column->insert("utf8mb4");
            collation_connection_column->insert("utf8mb4_0900_ai_ci");
            return std::make_shared<OneBlockInputStream>(Block{{move(name_column), std::make_shared<DataTypeString>(), "View"},
                                                    {std::move(column), std::make_shared<DataTypeString>(), "Create View"},
                                                    {std::move(character_set_client_column), std::make_shared<DataTypeString>(), "character_set_client"},
                                                    {std::move(collation_connection_column), std::make_shared<DataTypeString>(), "collation_connection"}});
        }
         else if ((show_query = query_ptr->as<ASTShowCreateDatabaseQuery>()))
        {
            name_column->insert(getOriginalDatabaseName(show_query->database));
            return std::make_shared<OneBlockInputStream>(Block{{move(name_column), std::make_shared<DataTypeString>(), "Database"},
                                        {std::move(column), std::make_shared<DataTypeString>(), "Create Database"}});
        }
        // TODO: add more show create that mysql has ...

        // fall back to clickhouse syntax
        return std::make_shared<OneBlockInputStream>(Block{{std::move(column), std::make_shared<DataTypeString>(), "statement"}});
    }

    return std::make_shared<OneBlockInputStream>(Block{{std::move(column), std::make_shared<DataTypeString>(), "statement"}});
}
}
