#include <Parsers/ASTSetSensitiveQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetSensitiveQuery.h>
#include <Catalog/Catalog.h>
#include <Access/AccessFlags.h>

namespace DB
{


BlockIO InterpreterSetSensitiveQuery::execute()
{
    const auto & ast = query_ptr->as<ASTSetSensitiveQuery &>();
    const auto ctx = getContext();

    if (ast.target == "DATABASE")
        ctx->checkAccess(AccessType::SET_SENSITIVE, ast.database);
    else if (ast.target == "TABLE")
        ctx->checkAccess(AccessType::SET_SENSITIVE, ast.database, ast.table);
    else if (ast.target == "COLUMN")
        ctx->checkAccess(AccessType::SET_SENSITIVE, ast.database, ast.table, ast.column);

    ctx->getCnchCatalog()->putSensitiveResource(ast.database, ast.table, ast.column, ast.target, ast.value);
    return {};
}

}
