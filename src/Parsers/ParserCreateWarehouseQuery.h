#pragma once
#include <Parsers/IParserBase.h>

namespace DB
{

/* Parses queries like
 * CREATE WAREHOUSE [IF NOT EXISTS] name
 * SETTINGS
 * [AUTO SUSPEND = 0|1]
 * [AUTO RESUME = 0|1]
 * [MAX_CLUSTER_COUNT = <num>]
 * [MIN_CLUSTER_COUNT = <num>]
 * [WORKER_COUNT = <num>,]
 * [MAX_CONCURRENCY_LEVEL = num]
 */
class ParserCreateWarehouseQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE WAREHOUSE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
