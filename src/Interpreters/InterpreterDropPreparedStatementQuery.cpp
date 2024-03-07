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
    const auto * drop = query_ptr->as<const ASTDropPreparedStatementQuery>();
    if (!drop || drop->name.empty())
        throw Exception("Drop Prepare logical error", ErrorCodes::LOGICAL_ERROR);

    auto current_context = getContext();
    // if (!drop->cluster.empty())
    //     return executeDDLQueryOnCluster(query_ptr, current_context);

    auto * prepared_manager = current_context->getPreparedStatementManager();

    // get prepare from cache
    if (!prepared_manager)
        throw Exception("Prepare cache has to be initialized", ErrorCodes::LOGICAL_ERROR);

    prepared_manager->remove(drop->name, !drop->if_exists);
    return {};
}
}
