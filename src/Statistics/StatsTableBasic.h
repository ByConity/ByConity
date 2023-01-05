#pragma once
#include <Protos/optimizer_statistics.pb.h>
#include <Statistics/StatisticsBaseImpl.h>
#include <Common/Exception.h>

namespace DB::Statistics
{
// basic statistics at table level
// a wrapper of Protos::TableBasic
// currently just row_count
class StatsTableBasic : public StatisticsBase
{
public:
    static constexpr auto tag = StatisticsTag::TableBasic;
    StatsTableBasic() = default;
    String serialize() const override;
    void deserialize(std::string_view blob) override;
    StatisticsTag getTag() const override { return tag; }

    void setRowCount(int64_t row_count);
    int64_t getRowCount() const;

    String serializeToJson() const override;
    void deserializeFromJson(std::string_view json) override;

private:
    Protos::StatsTableBasic table_basic_pb;
};

}
