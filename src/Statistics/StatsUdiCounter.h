#pragma once
#include <Protos/optimizer_statistics.pb.h>
#include <Statistics/StatisticsBaseImpl.h>
#include <Common/Exception.h>

namespace DB::Statistics
{
// UdiCounter statistics at table level
// a wrapper of Protos::TableUdiCounter
// currently just row_count
class StatsUdiCounter : public StatisticsBase
{
public:
    static constexpr auto tag = StatisticsTag::UdiCounter;
    StatsUdiCounter() = default;
    String serialize() const override { return proto.SerializeAsString(); }
    void deserialize(std::string_view blob) override { proto.ParseFromArray(blob.data(), blob.size()); }
    StatisticsTag getTag() const override { return tag; }

    void setUdiRowCount(int64_t udi_count) { proto.set_udi_count(udi_count); }

    UInt64 getUdiCount() const { return proto.udi_count(); }

private:
    Protos::StatsUdiCounter proto;
};

}
