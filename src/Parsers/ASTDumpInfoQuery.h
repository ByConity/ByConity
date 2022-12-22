#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
namespace DB
{
/**
 * Query Dump certain information for SELECT xxx, such as ddl, statistics, settings_changed and so on.
**/
class ASTDumpInfoQuery : public ASTQueryWithOutput
{
public:
    String syntax_error;
    String dump_string;
    ASTPtr dump_query; // query to dump
    String getID(char) const override { return "DumpInfoQuery"; }

    ASTType getType() const override { return ASTType::ASTDumpInfoQuery; }

    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};
}
