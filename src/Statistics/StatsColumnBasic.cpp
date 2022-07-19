#include <Optimizer/Dump/Json2Pb.h>
#include <Statistics/StatsColumnBasic.h>

namespace DB::Statistics
{
String StatsColumnBasic::serialize() const
{
    return proto.SerializeAsString();
}
void StatsColumnBasic::deserialize(std::string_view blob)
{
    proto.ParseFromArray(blob.data(), blob.size());
}
String StatsColumnBasic::serializeToJson() const
{
    DB::String json_str;
    Json2Pb::pbMsg2JsonStr(proto, json_str, false);
    return json_str;
}
void StatsColumnBasic::deserializeFromJson(std::string_view json)
{
    Json2Pb::jsonStr2PbMsg({json.data(), json.size()}, proto, false);
}

} // namespace DB
