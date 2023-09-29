#pragma once

#include <Parsers/ASTDumpQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParserBase.h>


namespace DB
{
/**
 * Query DUMP WORKLOAD
 */
class ParserDumpQuery : public IParserDialectBase
{
protected:
    const char * getName() const override { return "DUMP WORKLOAD query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

}
