#include <Parsers/ASTSwitchQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSwitchQuery.h>
#include <Access/AccessFlags.h>
#include <Common/typeid_cast.h>
#include "Parsers/ASTSwitchQuery.h"


namespace DB
{

BlockIO InterpreterSwitchQuery::execute()
{
    const String & new_database = query_ptr->as<ASTSwitchQuery &>().catalog;
    getContext()->getSessionContext()->setCurrentDatabase(new_database);
    return {};
}

}
