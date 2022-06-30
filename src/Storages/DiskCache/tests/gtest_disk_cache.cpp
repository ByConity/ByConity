#include <filesystem>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/DiskCache/DiskCacheLRU.h>
#include <gtest/gtest.h>
#include "Common/filesystemHelpers.h"
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>
#include <Common/filesystemHelpers.h>

namespace fs = std::filesystem;

void generateData(const String & base_path, int depth, int num_per_level, std::string partial_name, std::vector<std::string> & names)
{
    static int counter = 0;
    if (depth == 0)
    {
        for (int i = 0; i < num_per_level; i++)
        {
            std::string cache_name = partial_name + std::to_string(counter++) + ".bin";
            FS::createFile(base_path + cache_name);
            names.push_back(cache_name);
        }
    }
    else
    {
        for (int i = 0; i < num_per_level; i++)
        {
            String dir_name = partial_name + std::to_string(counter++) + "/";
            fs::create_directories(base_path + dir_name);
            generateData(base_path, depth - 1, num_per_level, dir_name, names);
        }
    }
}

DB::VolumePtr newSingleDiskVolume()
{
    fs::create_directory("tmp/local1/");
    auto disk = std::make_shared<DB::DiskLocal>("local1", "tmp/local1/", 0);
    return std::make_shared<DB::SingleDiskVolume>("single_disk", std::move(disk), 0);
}

// TODO: more volume

class DiskCacheTest : public testing::Test
{
public:
    void SetUp() override
    {
        fs::remove_all("tmp/");
        fs::create_directories("tmp/");
        UnitTest::initLogger();
    }

    void TearDown() override
    {
        fs::remove_all("tmp/");
    }

    static constexpr const UInt32 segment_size = 8192;
};

TEST_F(DiskCacheTest, Collect)
{
    auto volume = newSingleDiskVolume();
    int total_cache_num = 0;
    std::vector<std::pair<std::string, std::vector<std::string>>> metas;
    for (const DB::DiskPtr & disk : volume->getDisks())
    {
        String disk_cache_dir = disk->getPath() + "disk_cache/";
        std::vector<std::string> metas_in_disk;
        generateData(disk_cache_dir, 3, 2, {}, metas_in_disk);
        metas.push_back({disk->getName(), metas_in_disk});
        total_cache_num += metas_in_disk.size();
    }

    DB::DiskCacheSettings settings{
        .lru_max_size = 100000000,
        .random_drop_threshold = 80,
        .mapping_bucket_size = 8,
        .lru_update_interval = 5,
        .cache_base_path = "disk_cache/"};
    DB::DiskCacheLRU cache(*getContext().context, volume, settings);
    cache.load();
    EXPECT_EQ(cache.getKeyCount(), total_cache_num);

    // for (const auto& meta_in_disk : metas) {
    //     for (const String& name : meta_in_disk.second) {
    //         EXPECT_EQ(cache.get(name), disk->getPath() + "disk_cache/" + name);
    //     }
    // }
}
