#pragma once
#include <Protos/optimizer_statistics.pb.h>
#include <Statistics/StatisticsBaseImpl.h>
#include <Common/Exception.h>

namespace DB::Statistics
{
// basic statistics at column level
// a wrapper of Protos::ColumnBasic
// currently just row_count
class StatsColumnBasic : public StatisticsBase
{
public:
    static constexpr auto tag = StatisticsTag::ColumnBasic;
    StatsColumnBasic() = default;
    String serialize() const override;
    void deserialize(std::string_view blob) override;
    StatisticsTag getTag() const override { return tag; }

    Protos::StatsColumnBasic & mutable_proto() { return proto; }
    const Protos::StatsColumnBasic & get_proto() const { return proto; }

    String serializeToJson() const override;
    void deserializeFromJson(std::string_view json) override;

private:
    Protos::StatsColumnBasic proto;
};
}
