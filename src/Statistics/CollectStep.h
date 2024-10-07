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
#include <Common/Logger.h>
#include <limits>
#include <memory>
#include <Statistics/CollectorSettings.h>
#include <Statistics/ParseUtils.h>
#include <Statistics/StatisticsCollectorObjects.h>
#include <Statistics/StatsTableIdentifier.h>
#include <Poco/Logger.h>

namespace DB::Statistics
{

class StatisticsCollector;


struct HandlerColumnData
{
    double nonnull_count = 0;

    // when scaleNdv output unreliable result, this is false
    bool is_ndv_reliable = false;
    double ndv_value = 0;

    double min_as_double = std::numeric_limits<Float64>::quiet_NaN();
    double max_as_double = std::numeric_limits<Float64>::quiet_NaN();
    std::shared_ptr<BucketBounds> bucket_bounds; // RowCountHandler will write this
    std::optional<std::shared_ptr<StatsNdvBucketsResult>> ndv_buckets_result_opt;
    std::optional<UInt64> length_opt;
};


struct HandlerContext : boost::noncopyable
{
    HandlerContext(const CollectorSettings & settings_) : settings(settings_) { }

    const CollectorSettings & settings;
    double full_count = 0;
    std::optional<double> query_row_count;
    std::unordered_map<String, HandlerColumnData> columns_data;
};
// count all row
class RowCountHandler : public ColumnHandlerBase
{
public:
    RowCountHandler(HandlerContext & handler_context_) : handler_context(handler_context_), sqls({"count(*)"}) { }

    const std::vector<String> & getSqls() override { return sqls; }

    void parse(const Block & block, size_t index_offset) override
    {
        auto result = block.getByPosition(index_offset).column->getUInt(0);
        handler_context.query_row_count = result;
    }

private:
    HandlerContext & handler_context;
    std::vector<String> sqls;
};

class CollectStep
{
public:
    using TableStats = StatisticsImpl::TableStats;
    using ColumnStats = StatisticsImpl::ColumnStats;
    using ColumnStatsMap = StatisticsImpl::ColumnStatsMap;

    explicit CollectStep(StatisticsCollector & core_);

    void collectTable();

    virtual void collect(const ColumnDescVector & col_names) = 0;
    virtual ~CollectStep() = default;

    void writeResult(TableStats & core_table_stats, ColumnStatsMap & core_columns_stats);

protected:
    StatisticsCollector & core;
    StatsTableIdentifier table_info;
    CatalogAdaptorPtr catalog;
    ContextPtr context;
    HandlerContext handler_context;
    LoggerPtr logger = getLogger("Statistics::CollectStep");
};


std::vector<ColumnDescVector> split(const ColumnDescVector & origin, UInt64 max_columns);
std::unique_ptr<CollectStep> createSampleCollectStep(StatisticsCollector & core);
std::unique_ptr<CollectStep> createFullCollectStep(StatisticsCollector & core);
std::unique_ptr<CollectStep> createFullPartitionedCollectStep(StatisticsCollector & core);
}
