#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ParserStatsQuery.h>

namespace DB
{

bool parseStatsQueryKind(IParser::Pos & pos, Expected & expected, StatsQueryKind & kind)
{
    ParserKeyword s_stats("STATS");
    ParserKeyword s_table_stats("TABLE_STATS");
    ParserKeyword s_column_stats("COLUMN_STATS");

    if (s_stats.ignore(pos, expected))
        kind = StatsQueryKind::ALL_STATS;
    else if (s_table_stats.ignore(pos, expected))
        kind = StatsQueryKind::TABLE_STATS;
    else if (s_column_stats.ignore(pos, expected))
        kind = StatsQueryKind::COLUMN_STATS;
    else
        return false;

    return true;
}

template <typename SimpleWithSpecifierInfo>
class ParserCreateStatsQuerySimpleSpecifier : public IParserBase
{
public:
    [[nodiscard]] const char * getName() const override { return SimpleWithSpecifierInfo::Name; }

protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        ParserUnsignedInteger number_p;
        ParserKeyword s_specifier(SimpleWithSpecifierInfo::Specifier);

        if (!number_p.parse(pos, node, expected))
            return false;

        if (!s_specifier.ignore(pos, expected))
        {
            node = nullptr;
            return false;
        }

        return true;
    }
};

struct ParserCreateStatsQueryBucketsSpecifierInfo
{
    static constexpr auto Name = "CreateStat query buckets specifier";
    static constexpr auto Specifier = "BUCKETS";
};

struct ParserCreateStatsQueryTopnSpecifierInfo
{
    static constexpr auto Name = "CreateStat query topn specifier";
    static constexpr auto Specifier = "TOPN";
};

struct ParserCreateStatsQuerySamplesSpecifierInfo
{
    static constexpr auto Name = "CreateStat query samples specifier";
    static constexpr auto Specifier = "SAMPLES";
};

using ParserCreateStatsQueryBucketsSpecifier = ParserCreateStatsQuerySimpleSpecifier<ParserCreateStatsQueryBucketsSpecifierInfo>;
using ParserCreateStatsQueryTopnSpecifier = ParserCreateStatsQuerySimpleSpecifier<ParserCreateStatsQueryTopnSpecifierInfo>;
using ParserCreateStatsQuerySamplesSpecifier = ParserCreateStatsQuerySimpleSpecifier<ParserCreateStatsQuerySamplesSpecifierInfo>;

Int64 getValueFromUInt64Literal(const ASTPtr & node)
{
    if (auto * literal = node->as<ASTLiteral>())
    {
        UInt64 val = literal->value.safeGet<UInt64>();

        if (val > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            throw Exception("Value exceed limit", ErrorCodes::SYNTAX_ERROR);

        return static_cast<Int64>(val);
    }
    else
        throw Exception("Not a literal node", ErrorCodes::SYNTAX_ERROR);
}

bool ParserCreateStatsQuery::parseSuffix(Pos & pos, Expected & expected, IAST & ast)
{
    ParserKeyword s_partition("PARTITION");
    ParserKeyword s_with("WITH");
    ParserPartition partition_p;
    ParserNothing dummy_p;

    auto & create_stats_ast = dynamic_cast<ASTCreateStatsQuery &>(ast);

    if (s_partition.ignore(pos, expected))
    {
        ASTPtr partition;

        if (!partition_p.parse(pos, partition, expected))
            return false;

        create_stats_ast.partition = partition;
        create_stats_ast.children.push_back(create_stats_ast.partition);
    }

    if (s_with.ignore(pos, expected))
    {
        auto parse_specifier = [&pos, &expected, &create_stats_ast] {
            ParserCreateStatsQueryBucketsSpecifier buckets_p;
            ParserCreateStatsQueryTopnSpecifier topn_p;
            ParserCreateStatsQuerySamplesSpecifier samples_p;
            ASTPtr node;

            if (buckets_p.parse(pos, node, expected))
            {
                create_stats_ast.buckets = getValueFromUInt64Literal(node);
            }
            else if (topn_p.parse(pos, node, expected))
            {
                create_stats_ast.topn = getValueFromUInt64Literal(node);
            }
            else if (samples_p.parse(pos, node, expected))
            {
                create_stats_ast.samples = getValueFromUInt64Literal(node);
            }
            else
                return false;

            return true;
        };

        if (!ParserList::parseUtil(pos, expected, parse_specifier, dummy_p, false))
            return false;
    }

    return true;
}

}
