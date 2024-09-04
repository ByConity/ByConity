#include <algorithm>
#include <iterator>
#include <Interpreters/InterpreterDeleteQuery.h>

#include <Access/ContextAccess.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/trySetVirtualWarehouse.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Storages/MutationCommands.h>
#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_IS_READ_ONLY;
    extern const int SUPPORT_IS_DISABLED;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
}


InterpreterDeleteQuery::InterpreterDeleteQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterDeleteQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    const ASTDeleteQuery & delete_query = query_ptr->as<ASTDeleteQuery &>();
    auto table_id = getContext()->resolveStorageID(delete_query, Context::ResolveOrdinary);
    getContext()->checkAccess(AccessType::ALTER_DELETE, table_id);
    query_ptr->as<ASTDeleteQuery &>().database = table_id.database_name;
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name, getContext());

    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

    auto table_lock = table->lockForShare(getContext()->getCurrentQueryId(), getContext()->getSettingsRef().lock_acquire_timeout);
    auto metadata_snapshot = table->getInMemoryMetadataPtr();

    /// For now, we only support DELETE FROM CNCH unique table (CnchMergeTree with unique key).
    if (table->supportsLightweightDelete())
    {
        if (!getContext()->getSettingsRef().enable_lightweight_delete)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Lightweight delete is disabled. Set `enable_lightweight_delete` setting to enable it");

        if (!delete_query.predicate)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "DELETE FROM without any predication is not supported. Please use TRUNCATE instead.");

        /// Convert the DELETE FROM query to a INSERT query to mark target rows as deleted.
        Names unique_key_columns = metadata_snapshot->getColumnsRequiredForUniqueKey();
        NameSet all_key_columns(unique_key_columns.begin(), unique_key_columns.end());
        Names partition_key_columns = metadata_snapshot->getColumnsRequiredForPartitionKey();
        std::copy(partition_key_columns.begin(), partition_key_columns.end(), std::inserter(all_key_columns, all_key_columns.end()));
        String str_key_columns = fmt::format("`{}`", fmt::join(all_key_columns, "`, `"));

        /// Build query: "INSERT INTO d.t(p, k, _delete_flag_) SELECT p, k, 1 FROM d.t WHERE predicate"
        String insert_query =
            "INSERT INTO " + table->getStorageID().getFullTableName() 
            + "(" + str_key_columns + ", `_delete_flag_`) "
            + "SELECT " + str_key_columns + ", 1 "
            + "FROM " + table->getStorageID().getFullTableName()
            + " WHERE " + serializeAST(*delete_query.predicate);
        if (delete_query.settings_ast)
            insert_query += " SETTINGS " + serializeAST(*delete_query.settings_ast);

        const char * end = insert_query.data() + insert_query.size();
        ParserInsertQuery parser(end, {});
        ASTPtr insert_ast = parseQuery(
            parser,
            insert_query.data(),
            insert_query.data() + insert_query.size(),
            "Forward DELETE FROM to INSERT",
            0,
            DBMS_DEFAULT_MAX_PARSER_DEPTH);

        InterpreterInsertQuery insert_interpreter(
            insert_ast,
            getContext(),
            /*allow_materialized_*/false,
            /*no_squash_*/false,
            /*no_destination_*/false,
            AccessType::ALTER_DELETE);

        return insert_interpreter.execute();
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DELETE query is not supported for table {}", table->getStorageID().getFullTableName());
    }
}

}
