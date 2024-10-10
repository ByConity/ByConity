#include <Storages/MergeTree/GINStoreCommon.h>
#include <unordered_map>
#include <fmt/format.h>
#include <Poco/String.h>
#include <Common/Exception.h>

namespace DB
{

GINStoreVersion str2GINStoreVersion(const String& version_str_)
{
    static std::unordered_map<String, GINStoreVersion> mapping = {
        {"v0", GINStoreVersion::v0},
        {"v1", GINStoreVersion::v1},
        {"v2", GINStoreVersion::v2}
    };

    if (auto iter = mapping.find(Poco::toLower(version_str_)); iter != mapping.end())
    {
        return iter->second;
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Version string {} is not mapping to any "
        "valid version", version_str_);
}

String v2StoreSegmentDictName(UInt32 segment_id_)
{
    return fmt::format("segment_{}.sst", segment_id_);
}

}
