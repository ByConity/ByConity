
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterShowWarehousesQuery.h>
#include <Parsers/ASTShowWarehousesQuery.h>


namespace DB
{

InterpreterShowWarehousesQuery::InterpreterShowWarehousesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) 
    : WithMutableContext(context_), query_ptr(query_ptr_) {}

String InterpreterShowWarehousesQuery::getRewrittenQuery()
{
    auto & show = query_ptr->as<ASTShowWarehousesQuery &>();
    std::stringstream rewritten_query;

    rewritten_query << "SELECT * from system.virtual_warehouses";

    if (!show.like.empty())
    {
       rewritten_query << " WHERE name LIKE " << std::quoted(show.like, '\'');
    }

    rewritten_query << " ORDER BY name ";
    return rewritten_query.str();

}

BlockIO InterpreterShowWarehousesQuery::execute()
{
    return executeQuery(getRewrittenQuery(), getContext(), true);
}

}
