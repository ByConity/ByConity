#include <Parsers/ParserAlterWarehouseQuery.h>
#include <Parsers/ASTAlterWarehouseQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>

namespace DB
{

bool ParserAlterWarehouseQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTAlterWarehouseQuery>();
    node = query;

    ParserKeyword s_alter_warehouse("ALTER WAREHOUSE ");
    ParserKeyword s_rename_to("RENAME TO");
    ParserKeyword s_settings("SETTINGS");
    ParserIdentifier rename_p;
    ParserSetQuery settings_p(/* parse_only_internals_ = */ true);

    ASTPtr settings;

    if (!ParserKeyword{"ALTER WAREHOUSE"}.ignore(pos, expected))
        return false;

    ASTPtr warehouse_name_ast;
    if (!ParserIdentifier{}.parse(pos, warehouse_name_ast, expected))
        return false;
    String warehouse_name = getIdentifierName(warehouse_name_ast);

    while (true)
    {
        if (s_rename_to.ignore(pos, expected))
        {
            ASTPtr new_warehouse_name_ast;
            if (query->rename_to.empty() && rename_p.parse(pos, new_warehouse_name_ast, expected))
            {
                query->rename_to = getIdentifierName(new_warehouse_name_ast);
                continue;
            }
            else
                return false;
        }

        break;
    }

    if (s_settings.ignore(pos, expected))
    {
        if (!settings_p.parse(pos, settings, expected))
            return false;
    }

    query->name = std::move(warehouse_name);
    if (settings)
        query->set(query->settings, settings);
    node = query;
    return true;
}

}
