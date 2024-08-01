#pragma once

#include <Interpreters/Context.h>
#include <Parsers/IAST.h>
#include <Parsers/Lexer.h>
#include <Common/SipHash.h>
#include <Common/StringUtils/StringUtils.h>
#include <Interpreters/SQLBinding/SQLBindingCache.h>


namespace DB
{
class SQLBindingUtils
{
public:
    static ASTPtr getASTFromBindings(const char * begin, const char * end, ASTPtr ast, ContextMutablePtr & context);
    static UUID getQueryASTHash(ASTPtr query, String tenant_id = "");
    static UUID getReExpressionHash(const char * begin, const char * end, String tenant_id = "");
//    static bool matchPattern(const char * begin, const char * end, const String & pattern);
    static bool isMatchBinding(const char * begin, const char * end, SQLBindingObject & binding);
    static String getNormalizedQuery(const char * begin, const char * end);
    static String getASTStr(const ASTPtr & ast);
    static String getShowBindingsHeader(size_t row_number);
};

}
