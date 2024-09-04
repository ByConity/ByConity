#pragma once

#include <sstream>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/IAST.h>
#include "Common/SettingsChanges.h"
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

class ASTAutoStatsQuery : public ASTQueryWithTableAndOutput
{
public:
    enum class QueryPrefix
    {
        Alter,
        Create,
        Drop,
        Show
    };

    QueryPrefix prefix;
    // whether this query target at a single table or all tables
    bool any_database = false; // use asterisk
    bool any_table = false; // use asterisk
    // bool override = false; // override db.table when specifying db.*

    std::optional<SettingsChanges> settings_changes_opt;


    String getID(char delim) const override
    {
        // TODO(gouguilin)
        std::ostringstream res;
        res << static_cast<int>(prefix) << delim;
        res << "auto stats";
        return res.str();
    }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTAutoStatsQuery>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

    ASTType getType() const override
    {
        return ASTType::ASTAutoStatsQuery;
    }
    // TODO: to proto, from proto

private:
    // maybe override if this is not sufficient
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
