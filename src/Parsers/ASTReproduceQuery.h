#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
namespace DB
{
/**
 * Query Dump certain information for SELECT xxx, such as ddl, statistics, settings_changed and so on.
**/
class ASTReproduceQuery : public ASTQueryWithOutput
{
public:
    String reproduce_path;
    String getID(char) const override { return "ReproduceQuery"; }

    ASTType getType() const override { return ASTType::ASTReproduceQuery; }

    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
