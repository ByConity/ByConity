/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <sstream>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/IAST.h>
#include <Common/quoteString.h>

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
struct CreateStatsQueryInfo;


template <typename StatsQueryInfo>
class ASTStatsQueryBase : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    StatsQueryKind kind = StatsQueryKind::ALL_STATS;
    StatisticsCachePolicy cache_policy = StatisticsCachePolicy::Default;
    // whether this query target at a single table or all tables
    bool any_database = false; // use asterisk
    bool any_table = false; // use asterisk
    std::vector<String> columns;
    std::optional<SettingsChanges> settings_changes_opt;

    String getID(char delim) const override
    {
        // this is not important, simplify it to just title
        std::ostringstream res;
        res << StatsQueryInfo::QueryPrefix << delim << formatStatsQueryKind(kind);
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
    // CREATE/DROP/SHOW STATS/COLUMN_STATS
    void formatQueryPrefix(const FormatSettings & settings, FormatState &, FormatStateStacked) const
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << StatsQueryInfo::QueryPrefix << ' ' << formatStatsQueryKind(kind)
                      << (settings.hilite ? hilite_none : "");
    }

    // ALL/<db_table_name> AT COLUMNS (<col1>, <col2>)
    void formatQueryMiddle(const FormatSettings & s, FormatState &, FormatStateStacked) const
    {
        s.ostr << " ";
        if (any_database && any_table)
        {
            s.ostr << "*.*";
        }
        else if (any_table)
        {
            assert(!any_database);
            if (database.empty())
            {
                s.ostr << (s.hilite ? hilite_keyword : "") << "ALL" << (s.hilite ? hilite_none : "");
            }
            else
            {
                s.ostr << (s.hilite ? hilite_identifier : "") << backQuoteIfNeed(database) << (s.hilite ? hilite_none : "");
                s.ostr << ".*";
            }
        }
        else
        {
            assert(!any_database);
            assert(!any_table);

            s.ostr << (s.hilite ? hilite_identifier : "") << (!database.empty() ? backQuoteIfNeed(database) + "." : "")
                   << backQuoteIfNeed(table) << (s.hilite ? hilite_none : "");

            if (!columns.empty())
            {
                s.ostr << (s.hilite ? hilite_keyword : "") << " (" << (s.hilite ? hilite_none : "");
                s.ostr << fmt::format(FMT_STRING("{}"), fmt::join(columns, ", "));
                s.ostr << (s.hilite ? hilite_keyword : "") << ")" << (s.hilite ? hilite_none : "");
            }
        }

        formatOnCluster(s);
    }

    // postfix only for show/drop stats
    void formatQueryPostfix(const FormatSettings & settings, FormatState &, FormatStateStacked) const
    {
        if (cache_policy != StatisticsCachePolicy::Default)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN " << (settings.hilite ? hilite_none : "");
            if (cache_policy == StatisticsCachePolicy::Cache)
                settings.ostr << (settings.hilite ? hilite_keyword : "") << "CACHE" << (settings.hilite ? hilite_none : "");
            else if (cache_policy == StatisticsCachePolicy::Catalog)
                settings.ostr << (settings.hilite ? hilite_keyword : "") << "CATALOG" << (settings.hilite ? hilite_none : "");
            else
                throw Exception("Unknown cache policy", ErrorCodes::SYNTAX_ERROR);
        }
    }

    // maybe override if this is not sufficient
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        formatQueryPrefix(settings, state, frame);
        formatQueryMiddle(settings, state, frame);
        formatQueryPostfix(settings, state, frame);
    }
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
    enum class SampleType
    {
        Default = 0,
        FullScan = 1,
        Sample = 2
    };
    enum class SyncMode
    {
        Default = 0,
        Sync = 1,
        Async = 2
    };
    SampleType sample_type = SampleType::Default;
    SyncMode sync_mode = SyncMode::Default;
    std::optional<Int64> sample_rows = std::nullopt;
    std::optional<double> sample_ratio = std::nullopt;

    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
