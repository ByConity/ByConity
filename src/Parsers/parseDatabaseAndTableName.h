#pragma once
#include <Parsers/IParser.h>

namespace DB
{

/// Parses [db.]name
bool parseDatabaseAndTableName(IParser::Pos & pos, Expected & expected, String & database_str, String & table_str, bool rewrite_db = true);

/// Parses [db.]name or [db.]* or [*.]*
bool parseDatabaseAndTableNameOrAsterisks(IParser::Pos & pos, Expected & expected, String & database, bool & any_database, String & table, bool & any_table);

bool parseDatabase(IParser::Pos & pos, Expected & expected, String & database_str);

}
