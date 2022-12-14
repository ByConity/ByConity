#include <Core/UUID.h>
#include <Storages/DiskCache/DiskCacheLRU.h>
#include <Storages/DiskCache/IDiskCacheSegment.h>
#include <gtest/gtest.h>

namespace DB
{
TEST(DiskCache, UnhexKeyTest)
{
    String table_uuid = UUIDHelpers::UUIDToString(UUIDHelpers::generateV4());
    String seg_key = IDiskCacheSegment::formatSegmentName(table_uuid, "part_1", "col", 0, ".bin");

    DiskCacheLRU::KeyType key = DiskCacheLRU::hash(seg_key);
    String hex_key = DiskCacheLRU::hexKey(key);
    auto unhex = DiskCacheLRU::unhexKey(hex_key);
    EXPECT_TRUE(unhex.has_value());
    EXPECT_TRUE(unhex == key);
}

TEST(DiskCache, DiskCachePathTest)
{
    String table_uuid = UUIDHelpers::UUIDToString(UUIDHelpers::generateV4());
    String seg_key1 = IDiskCacheSegment::formatSegmentName(table_uuid, "part_1", "col", 0, ".bin");
    String seg_key2 = IDiskCacheSegment::formatSegmentName(table_uuid, "part_1", "col", 0, ".mrk");

    auto path1 = DiskCacheLRU::getRelativePath(DiskCacheLRU::hash(seg_key1));
    auto path2 = DiskCacheLRU::getRelativePath(DiskCacheLRU::hash(seg_key2));

    EXPECT_EQ(path1.parent_path(), path2.parent_path());
    EXPECT_NE(path1.filename(), path2.filename());
}

}
