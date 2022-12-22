#include <Statistics/StatsDummy.h>

namespace DB::Statistics
{
String StatsDummyAlpha::serialize() const
{
    return std::to_string(magic_num);
}

void StatsDummyAlpha::deserialize(std::string_view blob)
{
    auto strbuf = std::string(blob);
    auto ans = std::stoi(blob.data(), nullptr, 10);
    magic_num = ans;
}

String StatsDummyBeta::serialize() const
{
    return this->magic_str;
}

void StatsDummyBeta::deserialize(std::string_view blob)
{
    magic_str = blob;
}

} // namespace DB
