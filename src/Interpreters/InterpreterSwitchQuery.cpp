#include <Access/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSwitchQuery.h>
#include <Parsers/ASTSwitchQuery.h>
#include <Parsers/formatTenantDatabaseName.h>
#include <Common/typeid_cast.h>


namespace DB
{

BlockIO InterpreterSwitchQuery::execute()
{
    String catalog_name = query_ptr->as<ASTSwitchQuery &>().catalog;
    getContext()->getSessionContext()->setCurrentCatalog(catalog_name);
    return {};
}

}
