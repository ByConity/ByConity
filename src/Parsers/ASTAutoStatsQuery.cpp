#include <sstream>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTAutoStatsQuery.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/IAST.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SettingsChanges.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

using QueryPrefix = ASTAutoStatsQuery::QueryPrefix;
static String queryPrefixToString(QueryPrefix prefix)
{
    switch (prefix)
    {
        case QueryPrefix::Alter:
            return "ALTER";
        case QueryPrefix::Create:
            return "CREATE";
        case QueryPrefix::Drop:
            return "DROP";
        case QueryPrefix::Show:
            return "SHOW";
        default:
            throw Exception("unimplemnted", ErrorCodes::NOT_IMPLEMENTED);
    }
}


void ASTAutoStatsQuery::formatQueryImpl(const FormatSettings & s, FormatState &, FormatStateStacked) const
{
    s.ostr << (s.hilite ? hilite_keyword : "") << queryPrefixToString(prefix) << " AUTO STATS" << (s.hilite ? hilite_none : "");
    if (prefix == QueryPrefix::Alter)
    {
        // DO NOTHING
    }
    else
    {
        s.ostr << " ";
        if (any_database)
            s.ostr << "*.";
        else if (!database.empty())
            s.ostr << backQuoteIfNeed(database) << ".";
        s.ostr << (any_table ? "*" : backQuoteIfNeed(table));
    }

    if (settings_changes_opt)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << " WITH " << (s.hilite ? hilite_none : "");
        bool is_first = true;
        for (auto [k, v] : settings_changes_opt.value())
        {
            if (!is_first)
            {
                s.ostr << ", ";
            }
            is_first = false;

            s.ostr << k << "=" << applyVisitor(FieldVisitorToString(), v);
        }
    }
}
}
