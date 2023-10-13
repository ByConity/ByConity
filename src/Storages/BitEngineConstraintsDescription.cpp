#include <Storages/BitEngineConstraintsDescription.h>

#include <Parsers/formatAST.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

BitEngineConstraintsDescription BitEngineConstraintsDescription::parse(const String & str)
{
    if (str.empty())
        return {};

    BitEngineConstraintsDescription res;
    ParserConstraintDeclarationList parser;
    ASTPtr list = parseQuery(parser, str, 0, 0);

    for (const auto & constraint : list->children)
        res.constraints.push_back(std::dynamic_pointer_cast<ASTBitEngineConstraintDeclaration>(constraint));

    return res;
}

}
