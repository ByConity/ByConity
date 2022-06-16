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

} // namespace DB
