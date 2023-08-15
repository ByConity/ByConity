#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserAlterDiskCacheQuery : public IParserBase
{
protected:
    const char * getName() const override { return "ALTER DISK CACHE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
