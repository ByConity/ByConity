#include <Interpreters/InterpreterDropWorkerGroupQuery.h>

#include <Interpreters/Context.h>
#include <Parsers/ASTDropWorkerGroupQuery.h>
#include <ResourceManagement/ResourceManagerClient.h>

namespace DB
{
InterpreterDropWorkerGroupQuery::InterpreterDropWorkerGroupQuery(const ASTPtr & query_ptr_, ContextPtr context_)
    : WithContext(context_), query_ptr(query_ptr_) {}

BlockIO InterpreterDropWorkerGroupQuery::execute()
{
    // auto & drop_query = query_ptr->as<ASTDropWorkerGroupQuery &>();

    // auto rm_client = getContext()->getResourceManagerClient();
    // rm_client->dropWorkerGroup(drop_query.worker_group_id, drop_query.if_exists);

    return {};
}

}
