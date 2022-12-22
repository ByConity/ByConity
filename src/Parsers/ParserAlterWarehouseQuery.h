#pragma once
#include <Parsers/IParserBase.h>

namespace DB
{

/* ALTER WAREHOUSE name
 * [RENAME TO some_name]
 * [AUTO_SUSPEND = <some number>,]
 * [AUTO_RESUME = 0|1,]
 * [MAX_CLUSTER_COUNT = <num>,]
 * [MIN_CLUSTER_COUNT = <num>,]
 * [WORKER_COUNT = <num>,]
 * [MAX_CONCURRENCY_LEVEL = num]
 */
class ParserAlterWarehouseQuery : public IParserBase
{
protected:
    const char * getName() const override { return "ALTER WAREHOUSE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
