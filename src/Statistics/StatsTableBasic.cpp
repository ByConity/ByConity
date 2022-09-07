#include <Optimizer/Dump/Json2Pb.h>
#include <Statistics/StatsTableBasic.h>

namespace DB::Statistics
{
String StatsTableBasic::serialize() const
{
    return table_basic_pb.SerializeAsString();
}
void StatsTableBasic::deserialize(std::string_view blob)
{
    table_basic_pb.ParseFromArray(blob.data(), static_cast<int>(blob.size()));
}
void StatsTableBasic::setRowCount(int64_t row_count)
{
    table_basic_pb.set_row_count(row_count);
}
int64_t StatsTableBasic::getRowCount() const
{
    return table_basic_pb.row_count();
}
String StatsTableBasic::serializeToJson() const
{
    String json_str;
    Json2Pb::pbMsg2JsonStr(table_basic_pb, json_str, false);
    return json_str;
}
void StatsTableBasic::deserializeFromJson(std::string_view json)
{
    Json2Pb::jsonStr2PbMsg({json.data(), json.size()}, table_basic_pb, false);
}

} // namespace DB
