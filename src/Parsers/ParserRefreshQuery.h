#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
    /** Query like this:
      * REFRESH MATERIALIZED VIEW [db.]name
      *     PARTITION partition
      */
    class ParserRefreshQuery : public IParserDialectBase
    {
    protected:
        const char * getName() const override { return "REFRESH query"; }
        bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    public:
        using IParserDialectBase::IParserDialectBase;
    };
}
