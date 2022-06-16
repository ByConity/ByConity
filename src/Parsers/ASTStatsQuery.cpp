#include <Parsers/ASTStatsQuery.h>

namespace DB
{

String formatStatsQueryKind(StatsQueryKind kind)
{
    if (kind == StatsQueryKind::TABLE_STATS)
        return "TABLE_STATS";
    else if (kind == StatsQueryKind::COLUMN_STATS)
        return "COLUMN_STATS";
    else if (kind == StatsQueryKind::ALL_STATS)
        return "STATS";
    else
        throw Exception("Not supported kind of stats query kind.", ErrorCodes::SYNTAX_ERROR);
}

void ASTCreateStatsQuery::getStatsQueryIDSuffix(std::ostringstream & res, char delim) const
{
    if (buckets)
        res << delim << "buckets=" << *buckets;

    if (topn)
        res << delim << "topn=" << *topn;

    if (samples)
        res << delim << "samples=" << *samples;
}

ASTPtr ASTCreateStatsQuery::clone() const
{
    auto res = std::make_shared<ASTCreateStatsQuery>(*this);
    res->children.clear();

    if (partition)
    {
        res->partition = partition->clone();
        res->children.push_back(res->partition);
    }

    cloneOutputOptions(*res);
    return res;
}

void ASTCreateStatsQuery::formatStatsQuerySuffix(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (partition)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " PARTITION " << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);
    }

    bool printed = false;
    auto printWithIfNeeded = [&printed, &settings] {
        if (!printed)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH" << (settings.hilite ? hilite_none : "");
            printed = true;
        }
    };

    if (buckets)
    {
        printWithIfNeeded();
        settings.ostr << ' ' << *buckets << " BUCKETS";
    }

    if (topn)
    {
        printWithIfNeeded();
        settings.ostr << ' ' << *topn << " TOPN";
    }

    if (samples)
    {
        printWithIfNeeded();
        settings.ostr << ' ' << *samples << " SAMPLES";
    }
}

}
