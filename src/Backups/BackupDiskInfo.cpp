#include <Backups/BackupDiskInfo.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

String BackupDiskInfo::toString() const
{
    auto func = std::make_shared<ASTFunction>();
    func->name = backup_engine_name;
    func->no_empty_args = true;

    auto list = std::make_shared<ASTExpressionList>();
    list->children.push_back(std::make_shared<ASTLiteral>(disk_name));
    list->children.push_back(std::make_shared<ASTLiteral>(backup_dir));

    func->arguments = list;
    func->children.push_back(list);
    return serializeAST(*func);
}


BackupDiskInfo BackupDiskInfo::fromString(const String & str)
{
    ParserIdentifierWithOptionalParameters parser;
    ASTPtr ast = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    return fromAST(*ast);
}


BackupDiskInfo BackupDiskInfo::fromAST(const IAST & ast)
{
    const auto * func = ast.as<const ASTFunction>();
    if (!func)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected disk function, got {}", serializeAST(ast));

    BackupDiskInfo res;
    res.backup_engine_name = func->name;

    if (func->arguments)
    {
        const auto * list = func->arguments->as<const ASTExpressionList>();
        if (!list)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected list, got {}", serializeAST(*func->arguments));

        if (list->children.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Backup engine 'Disk' requires 2 arguments (disk_name, path)");

        res.disk_name = list->children[0]->as<const ASTLiteral>()->value.safeGet<String>();
        res.backup_dir = list->children[1]->as<const ASTLiteral>()->value.safeGet<String>();
    }

    return res;
}

}
