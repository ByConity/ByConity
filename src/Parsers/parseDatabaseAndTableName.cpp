#include "parseDatabaseAndTableName.h"
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>


namespace DB
{

bool parseDatabaseAndTableName(IParser::Pos & pos, Expected & expected, String & database_str, String & table_str, bool rewrite_db)
{
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier table_parser;

    ASTPtr database;
    ASTPtr table;

    database_str = "";
    table_str = "";

    if (!table_parser.parse(pos, database, expected))
        return false;

    if (s_dot.ignore(pos))
    {
        if (!table_parser.parse(pos, table, expected))
        {
            database_str = "";
            return false;
        }
        if (rewrite_db)
            tryRewriteCnchDatabaseName(database, pos.getContext());

        tryGetIdentifierNameInto(database, database_str);
        tryGetIdentifierNameInto(table, table_str);
    }
    else
    {
        database_str = "";
        tryGetIdentifierNameInto(database, table_str);
    }

    return true;
}


bool parseDatabaseAndTableNameOrAsterisks(IParser::Pos & pos, Expected & expected, String & database, bool & any_database, String & table, bool & any_table)
{
    return IParserBase::wrapParseImpl(pos, [&]
    {
        if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
        {
            auto pos_before_dot = pos;
            if (ParserToken{TokenType::Dot}.ignore(pos, expected)
                    && ParserToken{TokenType::Asterisk}.ignore(pos, expected))
            {
                /// *.*
                any_database = true;
                database.clear();
                any_table = true;
                table.clear();
                return true;
            }

            /// *
            pos = pos_before_dot;
            any_database = false;
            database.clear();
            any_table = true;
            table.clear();
            return true;
        }

        ASTPtr ast_db;
        ASTPtr ast_tb;
        ASTPtr ast_catalog;
        ASTPtr ast_tmp;
        ParserIdentifier identifier_parser;
        if (identifier_parser.parse(pos, ast_db, expected))
        {
            auto pos_before_dot = pos;
            if (ParserToken{TokenType::Dot}.ignore(pos, expected))
            {
                if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                {
                    /// db.*
                    tryRewriteCnchDatabaseName(ast_db, pos.getContext());

                    any_database = false;
                    database = getIdentifierName(ast_db);
                    any_table = true;
                    table.clear();
                    return true;
                }
                else if (identifier_parser.parse(pos, ast_tb, expected))
                {
                    if(ParserToken{TokenType::Dot}.ignore(pos,expected))
                    {
                        // catalog.db.*
                        if (ParserToken{TokenType::Asterisk}.ignore(pos, expected))
                        {
                            ast_catalog = ast_db;
                            ast_db = ast_tb;
                            tryAppendCatalogName(ast_catalog,ast_db);
                            tryRewriteCnchDatabaseName(ast_db, pos.getContext());
                            any_database =false;
                            database = getIdentifierName(ast_db);
                            any_table = true;
                            table.clear();
                            return true;
                        } else if(identifier_parser.parse(pos, ast_tmp ,expected ))
                        {
                        // catalog.db.table
                            ast_catalog = ast_db;
                            ast_db = ast_tb;
                            ast_tb = ast_tmp;
                            any_database = false;
                            any_table = false;
                            tryAppendCatalogName(ast_catalog,ast_db);
                            tryRewriteCnchDatabaseName(ast_db, pos.getContext());
                            database = getIdentifierName(ast_db); 
                            table = getIdentifierName(ast_tb);
                            return true; 
                        }
                    }

                    /// db.table
                    tryRewriteCnchDatabaseName(ast_db, pos.getContext());

                    any_database = false;
                    database = getIdentifierName(ast_db);
                    any_table = false;
                    table = getIdentifierName(ast_tb);
                    return true;
                }
            }

            /// table
            ast_tb = ast_db;
            pos = pos_before_dot;
            any_database = false;
            database.clear();
            any_table = false;
            table = getIdentifierName(ast_tb);
            return true;
        }

        return false;
    });
}

bool parseDatabase(IParser::Pos & pos, Expected & expected, String & database_str)
{
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier identifier_parser;

    ASTPtr database;
    database_str = "";

    if (!identifier_parser.parse(pos, database, expected))
        return false;

    tryRewriteCnchDatabaseName(database, pos.getContext());
    tryGetIdentifierNameInto(database, database_str);
    return true;
}

}
