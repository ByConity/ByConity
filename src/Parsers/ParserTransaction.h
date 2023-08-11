#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ASTTransaction.h>
#include <Parsers/CommonParsers.h>

namespace DB
{

template <class ASTQuery>
class ParserTransactionCommon : public IParserBase
{
protected:
    const char * getName() const override { return ASTQuery::keyword; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        ParserKeyword s_keyword {ASTQuery::keyword};
        if (!s_keyword.ignore(pos, expected))
            return false;

        node = std::make_shared<ASTQuery>();
        return true;
    }
};

using ParserBeginTransactionQuery = ParserTransactionCommon<ASTBeginTransactionQuery>;
using ParserBeginQuery = ParserTransactionCommon<ASTBeginQuery>;
using ParserCommitQuery = ParserTransactionCommon<ASTCommitQuery>;
using ParserRollbackQuery = ParserTransactionCommon<ASTRollbackQuery>;
using ParserShowStatementsQuery = ParserTransactionCommon<ASTShowStatementsQuery>;

} // end of namespace DB
