
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDropWarehouseQuery.h>
#include <Parsers/ASTDropWarehouseQuery.h>
#include <ResourceManagement/ResourceManagerClient.h>



namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_MANAGER_ERROR;
}
InterpreterDropWarehouseQuery::InterpreterDropWarehouseQuery(const ASTPtr & query_ptr_, ContextPtr context_) 
    : WithContext(context_), query_ptr(query_ptr_) {}


BlockIO InterpreterDropWarehouseQuery::execute()
{
    // auto & drop = query_ptr->as<ASTDropWarehouseQuery &>();

    // auto client = getContext()->getResourceManagerClient();
    // client->dropVirtualWarehouse(vw_name, drop.if_exists);

    return {};
}

}
