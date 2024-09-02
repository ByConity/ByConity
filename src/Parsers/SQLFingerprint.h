#include <Core/Types.h>
#include <Interpreters/Context.h>
#include "Core/SettingsEnums.h"

namespace DB
{
class SQLFingerprint
{
public:
    String generateIntermediateAST(
        const char * query_begin,
        const char * query_end,
        size_t max_query_size = 262144,
        size_t max_parser_depth = 1000,
        DialectType dialect_type = DialectType::CLICKHOUSE);
        
    String generate(
        const char * query_begin,
        const char * query_end,
        size_t max_query_size = 262144,
        size_t max_parser_depth = 1000,
        DialectType dialect_type = DialectType::CLICKHOUSE);

    String generate(ASTPtr & ast);
    String generateMD5(ASTPtr & ast);
};
}
