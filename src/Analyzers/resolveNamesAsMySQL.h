#pragma once

#include <Parsers/ASTSelectQuery.h>

namespace DB
{
void resolveNamesInHavingAsMySQL(ASTSelectQuery & select_query);
}
