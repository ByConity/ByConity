#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

class ParserOptimizeQueryColumnsSpecification : public IParserDialectBase
{
protected:
    const char * getName() const override { return "column specification for OPTIMIZE ... DEDUPLICATE BY"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

/** Query OPTIMIZE TABLE [db.]name [PARTITION partition] [FINAL] [DEDUPLICATE]
  */
class ParserOptimizeQuery : public IParserDialectBase
{
protected:
    const char * getName() const override { return "OPTIMIZE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    using IParserDialectBase::IParserDialectBase;
};

}
