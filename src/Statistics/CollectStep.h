#pragma once
#include <memory>
#include <Statistics/ParseUtils.h>
#include <Statistics/StatisticsCollectorObjects.h>
#include <Statistics/StatsTableIdentifier.h>

namespace DB::Statistics
{

class StatisticsCollector;


struct HandlerColumnData
{
    double nonnull_count = 0;
    // when scaleNdv output unreliable result, this is null
    std::optional<double> ndv_value_opt = std::nullopt;
    double min_as_double = 0;
    double max_as_double = 0;
    std::shared_ptr<BucketBounds> bucket_bounds; // RowCountHandler will write this
    std::optional<std::shared_ptr<StatsNdvBucketsResult>> ndv_buckets_result_opt;
};


struct HandlerContext : boost::noncopyable
{
    HandlerContext(const Settings & settings_) : settings(settings_) { }

    const Settings & settings;
    double full_count = 0;
    std::optional<double> query_row_count;
    std::unordered_map<String, HandlerColumnData> columns_data;
};
// count all row
class RowCountHandler : public ColumnHandlerBase
{
public:
    RowCountHandler(HandlerContext & handler_context_) : handler_context(handler_context_) { }

    std::vector<String> getSqls() override { return {"count(*)"}; }
    void parse(const Block & block, size_t index_offset) override
    {
        auto result = block.getByPosition(index_offset).column->getUInt(0);
        handler_context.query_row_count = result;
    }

    size_t size() override { return 1; }

private:
    HandlerContext & handler_context;
};

class CollectStep
{
public:
    using TableStats = StatisticsImpl::TableStats;
    using ColumnStats = StatisticsImpl::ColumnStats;
    using ColumnStatsMap = StatisticsImpl::ColumnStatsMap;
    using ColumnName = String;


    explicit CollectStep(StatisticsCollector & core_);

    void collectTable();

    virtual void collect(const ColumnDescVector & col_names) = 0;
    virtual ~CollectStep() = default;

    void writeResult(TableStats & core_table_stats, ColumnStatsMap & core_columns_stats);

protected:
    StatisticsCollector & core;
    StatsTableIdentifier table_info;
    CatalogAdaptorPtr catalog;
    Context & context;
    HandlerContext handler_context;
};


}
