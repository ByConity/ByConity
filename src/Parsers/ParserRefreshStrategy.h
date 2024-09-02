#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// Parser for ASTRefreshStrategy
/*
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [database.]<mv_name>
[REFRESH [ASYNC [START (<start_time>)] [EVERY (INTERVAL refresh_interval) ] | SYNC | MANUAL]] AS <query_statement>;
refresh_interval only accept unit day/hour/minute/second 
*/
class ParserRefreshStrategy : public IParserBase
{
protected:
    const char * getName() const override { return "refresh strategy"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
