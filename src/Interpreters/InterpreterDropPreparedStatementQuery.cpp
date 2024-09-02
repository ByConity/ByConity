#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/InterpreterDropPreparedStatementQuery.h>
#include <Interpreters/PreparedStatement/PreparedStatementManager.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTPreparedStatement.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_PREPARE;
}

BlockIO InterpreterDropPreparedStatementQuery::execute()
{
    auto current_context = getContext();
    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_PREPARED_STATEMENT);
    current_context->checkAccess(access_rights_elements);

    const auto * drop = query_ptr->as<const ASTDropPreparedStatementQuery>();
    if (!drop || drop->name.empty())
        throw Exception("Drop Prepare logical error", ErrorCodes::LOGICAL_ERROR);

    // if (!drop->cluster.empty())
    //     return executeDDLQueryOnCluster(query_ptr, current_context);

    auto * prepared_manager = current_context->getPreparedStatementManager();

    // get prepare from cache
    if (!prepared_manager)
        throw Exception("Prepare cache has to be initialized", ErrorCodes::LOGICAL_ERROR);

    prepared_manager->remove(drop->name, !drop->if_exists, current_context);
    return {};
}
}
