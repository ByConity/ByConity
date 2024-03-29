#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>

namespace DB
{
/// DELETE FROM [db.]name WHERE ...
class ASTDeleteQuery : public ASTQueryWithTableAndOutput
{
public:
    String getID(char delim) const final;
    ASTPtr clone() const final;
    ASTType getType() const override { return ASTType::ASTDeleteQuery; }

    ASTPtr predicate;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
