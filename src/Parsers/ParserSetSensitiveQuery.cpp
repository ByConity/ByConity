#include <algorithm>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetSensitiveQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserSetSensitiveQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>



namespace DB
{

bool ParserSetSensitiveQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserToken open(TokenType::OpeningRoundBracket);
    ParserToken close(TokenType::ClosingRoundBracket);
    ParserToken s_eq(TokenType::Equals);

    ParserNameList columns_p;

    ParserKeyword s_set("SET SENSITIVE");
    ParserKeyword s_database("DATABASE");
    ParserKeyword s_table("TABLE");
    ParserKeyword s_column("COLUMN");
    ParserUnsignedInteger set_value_p;


    if (!s_set.ignore(pos, expected))
        return false;
    // ASTSetSensitiveQuery contains: database string, table string, columns ASTPtr (contains StringLiterals --> use Field.getString or something to extract)
    auto query = std::make_shared<ASTSetSensitiveQuery>();

    if (s_database.ignore(pos, expected))
    {
        query->target = "DATABASE";
        if(!parseDatabase(pos, expected, query->database))
            return false;
    }
    else if (s_table.ignore(pos, expected))
    {
        query->target = "TABLE";
        if(!parseDatabaseAndTableName(pos, expected, query->database, query->table))
            return false;
        if(query->database.empty())
        {
            expected.add(pos, "database name");
            return false;
        }
    }
    else if (s_column.ignore(pos, expected))
    {
        query->target = "COLUMN";
        if(!parseDatabaseAndTableName(pos, expected, query->database, query->table))
            return false;

        if(query->database.empty())
        {
            expected.add(pos, "database name");
            return false;
        }

        if (!open.ignore(pos))
            return false;

        ASTPtr columns;
        if (!columns_p.parse(pos, columns, expected))
            return false;

        if (!close.ignore(pos))
            return false;

        if (columns->children.size() > 1)
        {
            expected.add(pos, "number of columns to be 1");
            return false;
        }

        for (auto & col : columns->children)
            query->column = col->as<ASTIdentifier>()->name();
    }
    else
        return false;


    if (!s_eq.ignore(pos, expected))
        return false;

    ASTPtr set_value;
    if (!set_value_p.parse(pos, set_value, expected))
        return false;


    ASTLiteral * literal = set_value->as<ASTLiteral>();
    Int64 set_value_literal = literal->value.get<Int64>();

    if (set_value_literal == 1)
        query->value = true;
    else if (set_value_literal == 0)
        query->value = false;
    else
        return false;

    node = query;

    return true;
}


}
