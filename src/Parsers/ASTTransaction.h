#pragma once

#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

template <class ASTSubClass>
class ASTTransactionCommon : public ASTQueryWithOutput
{
public:
    String getID(char) const override { return ASTSubClass::keyword; }

    ASTPtr clone() const override { return std::make_shared<ASTSubClass>(); }

    void formatQueryImpl(const FormatSettings & settings, FormatState & , FormatStateStacked ) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << ASTSubClass::keyword << (settings.hilite ? hilite_none : "");
    }
};

struct ASTBeginQuery : public ASTTransactionCommon<ASTBeginQuery> { static constexpr auto keyword = "BEGIN"; };
struct ASTBeginTransactionQuery : public ASTTransactionCommon<ASTBeginTransactionQuery> { static constexpr auto keyword = "BEGIN TRANSACTION"; };
struct ASTCommitQuery : public ASTTransactionCommon<ASTCommitQuery> { static constexpr auto keyword = "COMMIT"; };
struct ASTRollbackQuery : public ASTTransactionCommon<ASTRollbackQuery> { static constexpr auto keyword = "ROLLBACK"; };
struct ASTShowStatementsQuery : public ASTTransactionCommon<ASTShowStatementsQuery> { static constexpr auto keyword = "SHOW STATEMENTS"; };

} // end of namespace DB
