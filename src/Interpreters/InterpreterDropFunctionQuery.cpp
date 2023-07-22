#include <Parsers/ASTDropFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsLoader.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/InterpreterDropFunctionQuery.h>
#include <Interpreters/broadcastUDFQueryToCluster.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int UNKNOWN_DATABASE;
}

BlockIO InterpreterDropFunctionQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    ASTDropFunctionQuery & drop_function_query = query_ptr->as<ASTDropFunctionQuery &>();
    auto current_context = getContext();
    //TODO: change to use account name instead of db name

    if (drop_function_query.database_name.empty())
    {
        drop_function_query.database_name = current_context->getCurrentDatabase();
    }

    if (drop_function_query.database_name == "default")
    {
        throw Exception("Must specify a database for UDF", ErrorCodes::UNKNOWN_DATABASE);
    }
    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_FUNCTION);

    current_context->checkAccess(access_rights_elements);

    bool throw_if_not_exists = !drop_function_query.if_exists;
    bool cnch_local = drop_function_query.cnch_local;
    UserDefinedSQLFunctionFactory::instance().unregisterFunction(
        current_context,
        drop_function_query.database_name,
        drop_function_query.function_name,
        throw_if_not_exists,
        drop_function_query.cnch_local);
    LOG_DEBUG(log, "unregister function" + drop_function_query.database_name + "." + drop_function_query.function_name + "on disk succeed");
    if (!cnch_local)
    {
        drop_function_query.if_exists = true;
        drop_function_query.cnch_local = true;
        broadcastUDFQueryToCluster(query_ptr, current_context);
    }

    return {};
}

}
