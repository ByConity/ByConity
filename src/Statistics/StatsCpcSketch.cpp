#include <Statistics/StatsCpcSketch.h>

namespace DB::Statistics
{
String StatsCpcSketch::serialize() const
{
    std::ostringstream ss;
    data.serialize(ss);
    return ss.str();
}

void StatsCpcSketch::deserialize(std::string_view blob)
{
    if (blob.empty())
    {
        throw Exception("Empty Blob Data", ErrorCodes::LOGICAL_ERROR);
    }
    data = decltype(data)::deserialize(blob.data(), blob.size());
}

} // namespace DB
