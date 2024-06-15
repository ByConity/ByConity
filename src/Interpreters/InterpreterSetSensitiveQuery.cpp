#include <Parsers/ASTSetSensitiveQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetSensitiveQuery.h>
#include <Catalog/Catalog.h>

namespace DB
{


BlockIO InterpreterSetSensitiveQuery::execute()
{
    const auto & ast = query_ptr->as<ASTSetSensitiveQuery &>();
    getContext()->getCnchCatalog()->putSensitiveResource(ast.database, ast.table, ast.column, ast.target, ast.value);
    return {};
}

}
