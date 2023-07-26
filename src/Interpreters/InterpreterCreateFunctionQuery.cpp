#include <Interpreters/InterpreterCreateFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsLoader.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Interpreters/Context.h>
#include <Interpreters/broadcastUDFQueryToCluster.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/formatAST.h>
#include <Transaction/ICnchTransaction.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int CNCH_TRANSACTION_NOT_INITIALIZED;
    extern const int UNKNOWN_DATABASE;
    extern const int FUNCTION_ALREADY_EXISTS;
}

BlockIO InterpreterCreateFunctionQuery::execute()
{
    ASTCreateFunctionQuery & create_function_query = query_ptr->as<ASTCreateFunctionQuery &>();
    auto current_context = getContext();
    //TODO: change to use account name instead of db name

    if (create_function_query.database_name.empty())
    {
        create_function_query.database_name = current_context->getCurrentDatabase();
    }

    if (create_function_query.database_name == "default")
    {
        throw Exception("Must specify a database for UDF", ErrorCodes::UNKNOWN_DATABASE);
    }
    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_FUNCTION);

    if (create_function_query.or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_FUNCTION);

    current_context->checkAccess(access_rights_elements);

    WriteBufferFromOwnString create_stream;
    formatAST(create_function_query, create_stream, false);
    create_function_query.version = generateChecksum(create_stream.str());
    if (isHostServer(is_internal, current_context))
    {
        return executeForHostServer(create_function_query);
    }
    return executeForNonHostServer(create_function_query);
}

BlockIO InterpreterCreateFunctionQuery::executeForHostServer(const ASTCreateFunctionQuery & create_function_query)
{
    auto current_context = getContext();
    auto & function_name = create_function_query.function_name;
    auto & database_name = create_function_query.database_name;

    if (!is_internal)
        DatabaseCatalog::instance().assertDatabaseExists(database_name, current_context);

    IntentLockPtr udf_lock;
    TransactionCnchPtr txn = current_context->getCurrentTransaction();
    if (!txn)
        throw Exception("Cnch transaction is not initialized", ErrorCodes::CNCH_TRANSACTION_NOT_INITIALIZED);
    udf_lock = txn->createIntentLock(IntentLock::DB_LOCK_PREFIX, database_name, function_name);
    udf_lock->lock();
    bool throw_if_exists = !create_function_query.if_not_exists && !create_function_query.or_replace;
    bool replace_if_exists = create_function_query.or_replace;
    const auto & resolved_function_name = UserDefinedSQLFunctionFactory::instance().getResolvedFunctionName(database_name, function_name);
    UserDefinedSQLFunctionFactory::instance().registerFunction(
        current_context, resolved_function_name, function_name, query_ptr, throw_if_exists, replace_if_exists);
    auto & loader = current_context->getUserDefinedSQLObjectsLoader();

    try
    {
        loader.storeObjectOnCatalog(database_name, function_name, create_function_query);   
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::FUNCTION_ALREADY_EXISTS && replace_if_exists) {
            if (replace_if_exists)
            {
                loader.removeObjectOnCatalog(database_name, function_name);
                loader.storeObjectOnCatalog(database_name, function_name, create_function_query);  
            }
            else if (throw_if_exists)
            {
                throw Exception("UDF with function name - " + function_name + " in database - " +  database_name + " already exists on catalog.", ErrorCodes::FUNCTION_ALREADY_EXISTS);
            }
        }
        else 
        {
            throw e;
        }
    }

    try
    {
        if (!create_function_query.is_lambda)
        {
            loader.storeObject(
                UserDefinedSQLObjectType::ExternalFunction,
                resolved_function_name,
                create_function_query,
                throw_if_exists,
                replace_if_exists,
                current_context->getSettingsRef());
        }

    }
    catch (Exception e)
    {
        LOG_DEBUG(log, "Store function" + function_name + "on disk failed, got exception " + e.message());
        UserDefinedSQLFunctionFactory::instance().dropFunctionLocally(current_context, resolved_function_name, false);
    }

    try
    {
        broadcastUDFQueryToCluster(query_ptr, current_context);
    }
    catch (...)
    {
        // Even if one of the server fails, let it fail.
        // Next time when the query comes, the failed server will fetch from ByteKV.
    }

    LOG_DEBUG(log, "Sucessfull Load udf function name " + function_name);
    return {};
}


BlockIO InterpreterCreateFunctionQuery::executeForNonHostServer(const ASTCreateFunctionQuery & create_function_query)
{
    auto current_context = getContext();
    auto & function_name = create_function_query.function_name;
    auto & database_name = create_function_query.database_name;
    auto guard = DatabaseCatalog::instance().getDDLGuard(database_name, function_name);
    bool throw_if_exists = !create_function_query.if_not_exists && !create_function_query.or_replace;
    bool replace_if_exists = create_function_query.or_replace;
    const auto & resolved_function_name = UserDefinedSQLFunctionFactory::instance().getResolvedFunctionName(database_name, function_name);

    UserDefinedSQLFunctionFactory::instance().registerFunction(
        current_context, resolved_function_name, function_name, query_ptr, throw_if_exists, replace_if_exists);
    try
    {
        if (!create_function_query.is_lambda)
        {
            UserDefinedSQLObjectType obj_type = UserDefinedSQLObjectType::ExternalFunction;
            auto & loader = current_context->getUserDefinedSQLObjectsLoader();
            loader.storeObject(
                obj_type,
                resolved_function_name,
                create_function_query,
                throw_if_exists,
                replace_if_exists,
                current_context->getSettingsRef());
        }
    }
    catch (Exception e)
    {
        LOG_DEBUG(log, "Store function" + function_name + "on disk failed, got exception " + e.message());
        UserDefinedSQLFunctionFactory::instance().dropFunctionLocally(current_context, resolved_function_name, false);
    }

    LOG_INFO(log, "Sucessfull non local host Load udf function name " + function_name);
    return {};
}

size_t InterpreterCreateFunctionQuery::generateChecksum(const String & value)
{
    return std::hash<std::string>{}(value);
}

}
