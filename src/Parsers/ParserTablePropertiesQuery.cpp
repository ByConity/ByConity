#include <memory>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/TablePropertiesQueriesASTs.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserTablePropertiesQuery.h>

#include <Common/typeid_cast.h>
#include "Parsers/IAST_fwd.h"


namespace DB
{


bool ParserTablePropertiesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_exists("EXISTS");
    ParserKeyword s_temporary("TEMPORARY");
    ParserKeyword s_describe("DESCRIBE");
    ParserKeyword s_desc("DESC");
    ParserKeyword s_show("SHOW");
    ParserKeyword s_create("CREATE");
    ParserKeyword s_external_catalog("EXTERNAL CATALOG");
    ParserKeyword s_database("DATABASE");
    ParserKeyword s_table("TABLE");
    ParserKeyword s_view("VIEW");
    ParserKeyword s_dictionary("DICTIONARY");
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p;

    ASTPtr catalog;
    ASTPtr database;
    ASTPtr table;
    std::shared_ptr<ASTQueryWithTableAndOutput> query;
    bool parse_only_catalog_name = false;
    bool parse_only_database_name = false;
    bool parse_show_create_view = false;
    bool exists_view = false;

    bool temporary = false;
    if (s_exists.ignore(pos, expected))
    {
        if (s_database.ignore(pos, expected))
        {
            query = std::make_shared<ASTExistsDatabaseQuery>();
            parse_only_database_name = true;
        }
        else if (s_view.ignore(pos, expected))
        {
            query = std::make_shared<ASTExistsViewQuery>();
            exists_view = true;
        }
        else
        {
            if (s_temporary.ignore(pos, expected))
                temporary = true;

            if (s_table.checkWithoutMoving(pos, expected))
                query = std::make_shared<ASTExistsTableQuery>();
            else if (s_dictionary.checkWithoutMoving(pos, expected))
                query = std::make_shared<ASTExistsDictionaryQuery>();
            else
                query = std::make_shared<ASTExistsTableQuery>();
        }
    }
    else if (s_show.ignore(pos, expected))
    {
        if (!s_create.ignore(pos, expected))
            return false;
        if (s_external_catalog.ignore(pos, expected))
        {
            parse_only_catalog_name = true;
            query = std::make_shared<ASTShowCreateExternalCatalogQuery>();
        }
        else if (s_database.ignore(pos, expected))
        {
            parse_only_database_name = true;
            query = std::make_shared<ASTShowCreateDatabaseQuery>();
        }
        else if (s_dictionary.checkWithoutMoving(pos, expected))
            query = std::make_shared<ASTShowCreateDictionaryQuery>();
        else if (s_view.ignore(pos, expected))
        {
            query = std::make_shared<ASTShowCreateViewQuery>();
            parse_show_create_view = true;
        }
        else
            query = std::make_shared<ASTShowCreateTableQuery>();
    }
    else
    {
        return false;
    }
    if (parse_only_catalog_name)
    {
        if (!name_p.parse(pos, catalog, expected))
            return false;
        tryRewriteHiveCatalogName(catalog, pos.getContext());
    }
    else if (parse_only_database_name)
    {
        if (!name_p.parse(pos, database, expected))
            return false;
        tryRewriteCnchDatabaseName(database, pos.getContext());
    }
    else
    {
        if (!(exists_view || parse_show_create_view))
        {
            if (temporary || s_temporary.ignore(pos, expected))
                query->temporary = true;

            if (!s_table.ignore(pos, expected))
                s_dictionary.ignore(pos, expected);
        }
        if (!name_p.parse(pos, table, expected))
            return false;
        if (s_dot.ignore(pos, expected))
        {
            database = table;
            if (!name_p.parse(pos, table, expected))
                return false;
            if (s_dot.ignore(pos, expected))
            {
                catalog = database;
                database = table;
                if (!name_p.parse(pos, table, expected))
                    return false;
                query = std::make_shared<ASTShowCreateExternalTableQuery>();
                tryRewriteHiveCatalogName(catalog, pos.getContext());
            }
            else
            {
                tryRewriteCnchDatabaseName(database, pos.getContext());
            }
        }
    }

    tryGetIdentifierNameInto(catalog, query->catalog);
    tryGetIdentifierNameInto(database, query->database);
    tryGetIdentifierNameInto(table, query->table);

    node = query;

    return true;
}


}
