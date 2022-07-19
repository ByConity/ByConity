#pragma once
#include <utility>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateStatsQuery.h>
#include <Optimizer/CardinalityEstimate/PlanNodeStatistics.h>
#include <Optimizer/CardinalityEstimate/SymbolStatistics.h>
#include <Protos/optimizer_statistics.pb.h>
#include <Statistics/Base64.h>
#include <Statistics/BlockParser.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/StatsColumnBasic.h>
#include <Statistics/StatsCpcSketch.h>
#include <Statistics/StatsKllSketch.h>
#include <Statistics/StatsNdvBuckets.h>
#include <Statistics/StatsTableBasic.h>
#include <common/logger_useful.h>

namespace DB::Statistics
{

namespace StatisticsImpl
{
    struct TableStats
    {
    public:
        std::shared_ptr<StatsTableBasic> basic;

    public:
        // TODO: use reflection to eliminate this manual code
        StatsCollection writeToCollection() const
        {
            std::unordered_map<StatisticsTag, StatisticsBasePtr> collection;
            if (basic)
                collection.emplace(basic->getTag(), basic);
            return collection;
        }

        // TODO: use reflection to eliminate this manual code
        void readFromCollection(const StatsCollection & collection)
        {
            auto tag = StatisticsTag::TableBasic;
            if (collection.count(tag))
                basic = std::static_pointer_cast<StatsTableBasic>(collection.at(tag));
        }
    };

    struct ColumnStats
    {
    public:
        std::shared_ptr<StatsNdvBucketsResult> ndv_buckets_result;
        // basic contains ndv and histogram bounds, a.k.a. cpc/kll sketch
        std::shared_ptr<StatsColumnBasic> basic;

    public:
        // TODO: use reflection to eliminate this manual code
        StatsCollection writeToCollection() const
        {
            std::unordered_map<StatisticsTag, StatisticsBasePtr> collection;

            auto list = std::vector<StatisticsBasePtr>{ndv_buckets_result, basic};

            for (auto & ptr : list)
            {
                if (ptr)
                    collection.emplace(ptr->getTag(), ptr);
            }
            return collection;
        }

        // TODO: use reflection to eliminate this manual code
        void readFromCollection(const StatsCollection & collection)
        {
            auto handle = [&]<typename T>(std::shared_ptr<T> & field) {
                using StatsType = T;
                constexpr auto tag = StatsType::tag;
                if (collection.count(tag))
                {
                    field = std::static_pointer_cast<StatsType>(collection.at(tag));
                }
            };

            handle(ndv_buckets_result);
            handle(basic);
        }
    };
    struct StatDesc
    {
        String db;
        String table;
        String column;
        String type;
        String value;

        StatDesc(String db_, String table_, String column_, String type_, String value_)
            : db(std::move(db_)), table(table_), column(column_), type(type_), value(value_)
        {
        }
    };
} // namespace StatisticsImpl

struct StatisticsSampleInfo
{
    UInt64 total_rows;
    UInt64 sampled_rows;
    String predicates;
    std::vector<String> partition_columns;
};


// uuid level
class StatisticsCollector
{
public:
    using StatDesc = StatisticsImpl::StatDesc;
    using TableStats = StatisticsImpl::TableStats;
    using ColumnStats = StatisticsImpl::ColumnStats;

    using UUID = String; // table uuid
    using ColumnName = String;
    StatisticsCollector(ContextPtr context_, CatalogAdaptorPtr catalog_, const StatsTableIdentifier & table_info_, TxnTimestamp timestamp_)
        : context(context_), catalog(catalog_), table_info(table_info_), timestamp(timestamp_)
    {
        String str;
        auto & settings = catalog->getSettingsRef();
        collect_debug_level = settings.statistics_collect_debug_level;
        logger = &Poco::Logger::get("StatisticsLogger" + getDatabaseDotTable());
    }

    void collectFull();
    void collectColumns(const ColumnDescVector & col_names);

    void writeToCatalog();
    void readAllFromCatalog();
    void readFromCatalog(const ColumnDescVector & cols_desc);
    void readFromCatalog(const std::vector<String> & cols_name);

    std::string getDatabaseDotTable() { return table_info.getDbTableName(); }

    std::optional<PlanNodeStatisticsPtr> toPlanNodeStatistics() const;

    const auto & getTableStats() const { return table_stats; }
    const auto & getColumnsStats() const { return columns_stats; }
    StatsData getStatsDataWithCache();
    void setTableStats(TableStats && stats) { table_stats = std::move(stats); }
    void setColumnStats(ColumnName col_name, ColumnStats && col_stats) { columns_stats[col_name] = std::move(col_stats); }

private:
    std::vector<FirstQueryResult> collectFirstStage(const ColumnDescVector & cols_desc, bool do_sample);

    void collectColumnsNdv(const std::vector<std::pair<NameAndTypePair, std::shared_ptr<BucketBounds>>> & cols_desc, bool do_sample);
    void collectTableStats();
    void collectTableStatsAndSample();
    void collectColumnStats(const ColumnDescVector & cols_desc, bool do_sample);
    void patchSampleResult(const ColumnDescVector & cols_desc);

private:
    ContextPtr context;
    Poco::Logger * logger;
    CatalogAdaptorPtr catalog;
    StatsTableIdentifier table_info;
    [[maybe_unused]] TxnTimestamp timestamp;

    // table stats
    TableStats table_stats;

    // column stats
    std::map<ColumnName, ColumnStats> columns_stats;

    // sampling info
    std::optional<StatisticsSampleInfo> sample_info_opt;
    int collect_debug_level = 0;
};

String constructGroupBySqlForStatsNdvBucket(const BucketBounds & bounds, const String & table_name, const String & column_name);
// only for test
std::shared_ptr<StatsNdvBuckets> StatsNdvBucketsFrom(SerdeDataType serde_data_type, BlockIO & io, const BucketBounds & bounds);

std::shared_ptr<StatsNdvBuckets> StatsNdvBucketsFrom(
    SerdeDataType serde_data_type, const std::vector<std::tuple<size_t, size_t, String>> & data, const BucketBounds & bounds);

template <class StatsType>
StatisticsBasePtr createStatisticsUntyped(StatisticsTag tag, std::string_view blob);

}
