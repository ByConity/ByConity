#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/IAST.h>
#include <Common/quoteString.h>

#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

enum class StatsQueryKind
{
    TABLE_STATS,
    COLUMN_STATS,
    ALL_STATS
};

String formatStatsQueryKind(StatsQueryKind kind);

template <typename StatsQueryInfo>
class ASTStatsQueryBase : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    StatsQueryKind kind = StatsQueryKind::ALL_STATS;
    // whether this query target at a single table or all tables
    bool target_all = false;
    String column;

    String getID(char delim) const override
    {
        std::ostringstream res;

        res << StatsQueryInfo::QueryPrefix << delim << formatStatsQueryKind(kind);

        if (!target_all)
        {
            if (!database.empty())
                res << delim << database;

            res << delim << table;
        }

        if (!column.empty())
            res << delim << column;

        getStatsQueryIDSuffix(res, delim);
        return res.str();
    }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTStatsQueryBase>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

    ASTType getType() const override { return StatsQueryInfo::Type; }

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override
    {
        return removeOnCluster<ASTStatsQueryBase>(clone(), new_database);
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << StatsQueryInfo::QueryPrefix << ' ' << formatStatsQueryKind(kind)
                      << (settings.hilite ? hilite_none : "") << ' ';

        if (target_all)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << "ALL" << (settings.hilite ? hilite_none : "");
        else
            settings.ostr << (settings.hilite ? hilite_identifier : "") << (!database.empty() ? backQuoteIfNeed(database) + "." : "")
                          << backQuoteIfNeed(table) << (settings.hilite ? hilite_none : "");

        if (!column.empty())
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " AT COLUMN " << (settings.hilite ? hilite_none : "")
                          << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(column) << (settings.hilite ? hilite_none : "");

        formatOnCluster(settings);
        formatStatsQuerySuffix(settings, state, frame);
    }

    virtual void getStatsQueryIDSuffix(std::ostringstream &, char) const { }

    virtual void formatStatsQuerySuffix(const FormatSettings &, FormatState &, FormatStateStacked) const { }
};

struct CreateStatsQueryInfo
{
    static constexpr auto Type = ASTType::ASTCreateStatsQuery;
    static constexpr auto QueryPrefix = "CREATE";
};

struct ShowStatsQueryInfo
{
    static constexpr auto Type = ASTType::ASTShowStatsQuery;
    static constexpr auto QueryPrefix = "SHOW";
};

struct DropStatsQueryInfo
{
    static constexpr auto Type = ASTType::ASTDropStatsQuery;
    static constexpr auto QueryPrefix = "DROP";
};

using ASTShowStatsQuery = ASTStatsQueryBase<ShowStatsQueryInfo>;
using ASTDropStatsQuery = ASTStatsQueryBase<DropStatsQueryInfo>;

class ASTCreateStatsQuery : public ASTStatsQueryBase<CreateStatsQueryInfo>
{
public:
    ASTPtr partition;
    bool if_not_exists = false;
    std::optional<Int64> buckets;
    std::optional<Int64> topn;
    std::optional<Int64> samples;

    ASTPtr clone() const override;

protected:
    void getStatsQueryIDSuffix(std::ostringstream &, char) const override;
    void formatStatsQuerySuffix(const FormatSettings &, FormatState &, FormatStateStacked) const override;
};

}
