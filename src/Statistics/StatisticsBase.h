#pragma once
#include <chrono>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <Core/Block.h>
#include <Core/Types.h>
#include <Statistics/CommonErrorCodes.h>
#include <Statistics/StatisticsCommon.h>

namespace DB::Statistics
{
enum class StatisticsTag : UInt64
{
    Invalid = 0,

    TableBasic = 1,
    CpcSketch = 2,
    KllSketch = 3,
    NdvBuckets = 4, // including bounds, min/max and ndv(cpc object) for each buckets
    NdvBucketsResult = 5, // including bounds, min/max and ndv(double value) for each buckets
    ColumnBasic = 6, // now put min/max here

    // for test only
    DummyAlpha = 2000,
    DummyBeta = 2001,
};

class StatisticsBase
{
public:
    // get the timestamp when stats is collected
    TxnTimestamp getTxnTimestamp() { return txn_timestamp_; }

    // set the timestamp when stats is collected
    void setTxnTimestamp(TxnTimestamp ts) { txn_timestamp_ = ts; }

    // get type of statistics
    virtual StatisticsTag getTag() const = 0;

    // serialize as binary blob
    // note: timestamp is not included
    virtual String serialize() const = 0;

    // deserialize from binary blob
    // note: timestamp is not included
    virtual void deserialize(std::string_view blob) = 0;

    StatisticsBase() = default;
    StatisticsBase(const StatisticsBase &) = default;
    StatisticsBase(StatisticsBase &&) = default;
    StatisticsBase & operator=(const StatisticsBase &) = default;
    StatisticsBase & operator=(StatisticsBase &&) = default;

    virtual ~StatisticsBase() = default;

private:
    // timestamp when stats is collected
    TxnTimestamp txn_timestamp_;
};

using StatisticsBasePtr = std::shared_ptr<StatisticsBase>;
using StatsCollection = std::unordered_map<StatisticsTag, StatisticsBasePtr>;

struct StatsData
{
    StatsCollection table_stats;
    std::unordered_map<String, StatsCollection> column_stats;
};

// helper function to create statistics object from binary blob
StatisticsBasePtr createStatisticsBase(StatisticsTag tag, TxnTimestamp ts, std::string_view blob);
}
