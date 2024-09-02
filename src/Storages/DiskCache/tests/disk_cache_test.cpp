/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>
#include <filesystem>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/DiskCache/DiskCacheLRU.h>
#include <gtest/gtest.h>
#include "Common/filesystemHelpers.h"
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>
#include <Common/filesystemHelpers.h>
#include "Core/Defines.h"
#include "Disks/IDisk.h"
#include "Disks/VolumeJBOD.h"
#include "IO/LimitReadBuffer.h"
#include "IO/ReadBufferFromFile.h"
#include "IO/copyData.h"
#include <Poco/Util/XMLConfiguration.h>
#include <Storages/DiskCache/FileDiskCacheSegment.h>
#include <Storages/DiskCache/DiskCacheFactory.h>
#include <Common/tests/gtest_global_context.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Disks/registerDisks.h>

namespace fs = std::filesystem;

namespace DB
{

template<bool IS_V1_FORMAT>
void generateData(const DiskPtr & disk, int depth, int num_per_level, Strings & names, Strings & partial_key) {
    static int counter = 0;
    if (depth == 0) {
        for (int i = 0; i < num_per_level; i++) {
            partial_key.push_back(std::to_string(counter++));

            String cache_name;
            std::filesystem::path rel_path;
            cache_name = fmt::format("{}.{}", fmt::join(partial_key, "/"), "bin");
            if constexpr (IS_V1_FORMAT)
            {
                rel_path = DiskCacheLRU::getPath(DiskCacheLRU::hash(cache_name), "disk_cache_v1", "", "");
            }
            else
            {
                rel_path = std::filesystem::path("disk_cache") / cache_name;
            }
            partial_key.pop_back();

            disk->createDirectories(rel_path.parent_path());
            WriteBufferFromFile writer(std::filesystem::path(disk->getPath()) / rel_path);
            String content = String(std::abs(random()) % 100, 'a');
            writer.write(content.data(), content.size());
            names.push_back(cache_name);
        }
    } else {
        for (int i = 0; i < num_per_level; i++) {
            partial_key.push_back(std::to_string(counter++));
            generateData<IS_V1_FORMAT>(disk, depth - 1, num_per_level, names, partial_key);
            partial_key.pop_back();
        }
    }
}

Strings generateData(const DiskPtr & disk, int depth, int num_per_level)
{
    Strings seg_names;
    Strings partial_name;
    generateData<true>(disk, depth, num_per_level, seg_names, partial_name);
    return seg_names;
}

Strings generateOldData(const DiskPtr & disk, int depth, int num_per_level)
{
    Strings seg_names;
    Strings partial_name;
    generateData<false>(disk, depth, num_per_level, seg_names, partial_name);
    return seg_names;
}
}

DB::VolumePtr newSingleDiskVolume()
{
    fs::create_directory("tmp/local1/");
    auto disk = std::make_shared<DB::DiskLocal>("local1", "tmp/local1/", 0);
    return std::make_shared<DB::SingleDiskVolume>("single_disk", std::move(disk), 0);
}

DB::VolumePtr newDualDiskVolume()
{
    fs::create_directory("tmp/local1/");
    fs::create_directory("tmp/local2/");
    DB::Disks disks;
    disks.emplace_back(std::make_shared<DB::DiskLocal>("local1", "tmp/local1/", DB::DiskStats{}));
    disks.emplace_back(std::make_shared<DB::DiskLocal>("local2", "tmp/local2/", DB::DiskStats{}));
    return std::make_shared<DB::VolumeJBOD>("dual_disk", disks, disks.front()->getName(), 0, false);
}
// TODO: more volume

using namespace DB;

class DiskCacheTest : public testing::Test
{
public:
    static void SetUpTestCase() {
        Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter("%Y.%m.%d %H:%M:%S.%F <%p> %s: %t"));
        Poco::AutoPtr<Poco::ConsoleChannel> console_chanel(new Poco::ConsoleChannel);
        Poco::AutoPtr<Poco::FormattingChannel> channel(new Poco::FormattingChannel(formatter, console_chanel));
        Poco::Logger::root().setLevel("trace");
        Poco::Logger::root().setChannel(channel);

        ctx = getContext().context;

        fs::create_directories("./disks/");

        fs::create_directory("./disk_cache/");
        fs::create_directory("./disk_cache_v1/");
        fs::create_directory("/tmp/disk_cache/");
        fs::create_directory("/tmp/disk_cache_v1/");
        registerDisks();
    }

    static void TearDownTestCase() {
        ctx->shutdown();
        // ctx->getLocalDiskCacheThreadPool().finalize();
        // ctx->getLocalDiskCacheEvictThreadPool().finalize();
    }

    void SetUp() override
    {
        fs::remove_all("tmp/");
        fs::create_directories("tmp/");
        UnitTest::initLogger();
        DB::IDiskCache::init(*getContext().context);
    }

    void TearDown() override
    {
        fs::remove_all("tmp/");
        DB::IDiskCache::close();
    }

    static std::shared_ptr<Context> ctx;
    static constexpr const UInt32 segment_size = 8192;
};

std::shared_ptr<Context> DiskCacheTest::ctx = nullptr;

TEST_F(DiskCacheTest, Collect)
{
    auto volume = newDualDiskVolume();
    int total_cache_num = 0;
    std::vector<std::pair<DB::DiskPtr, std::vector<std::string>>> metas;
    for (const DB::DiskPtr & disk : volume->getDisks())
    {
        std::vector<String> metas_in_disk = generateData(disk, 3, 4);
        metas.push_back({disk, metas_in_disk});
        total_cache_num += metas_in_disk.size();
    }

    DB::DiskCacheSettings settings;
    DB::DiskCacheLRU cache(volume, getContext().context->getDiskCacheThrottler(), settings);
    cache.load();
    EXPECT_EQ(cache.getKeyCount(), total_cache_num);

    for (const auto & meta_in_disk : metas) {
        auto disk = meta_in_disk.first;
        for (const String & name : meta_in_disk.second) {
            auto [cache_disk, cached_file] = cache.get(name);
            std::cout << "cur cached_file: " << cached_file << std::endl;
            ASSERT_TRUE(!cached_file.empty());
            ASSERT_TRUE(cache_disk->getName() == disk->getName());
            ASSERT_TRUE(cache_disk->exists(cached_file));
        }
    }
}

/*

TEST_F(DiskCacheTest, CompatibleConfig)
{

    DB::IDiskCache::close();
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration("disk_cache_test_old_config.xml");
    ctx->setConfig(config);
    DiskCacheFactory::instance().init(*ctx);
    ASSERT_TRUE(DiskCacheFactory::instance().get(DiskCacheType::MergeTree));
    ASSERT_TRUE(DiskCacheFactory::instance().get(DiskCacheType::Hive));
}
/// todo(jiashuo): not pass
TEST_F(DiskCacheTest, File)
{
    DB::IDiskCache::close();
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration("disk_cache_test_new_config.xml");
    auto ctx = getContext().context;
    ctx->setConfig(config);
    DiskCacheFactory::instance().init(*ctx);
    ASSERT_TRUE(DiskCacheFactory::instance().get(DiskCacheType::MergeTree));
    ASSERT_TRUE(DiskCacheFactory::instance().get(DiskCacheType::Hive));

    auto disk_cache = DiskCacheFactory::instance().get(DiskCacheType::File);
    ASSERT_TRUE(disk_cache);

    const auto & test_file = "/home/byte_dp_cnch_lf/titan/ci/cnch-disk-cache-test";

    const auto & key1 = FileDiskCacheSegment::getSegmentKey("test_file_uuid", "part_name");
    const auto & localFilePathEmpty = disk_cache->get(key1);
    ASSERT_TRUE(localFilePathEmpty.second.empty());

    HDFSConnectionParams hdfs_params = HDFSConnectionParams::parseHdfsFromConfig(*config);
    hdfs_params.conn_type = HDFSConnectionParams::HDFSConnectionType::CONN_NNPROXY;
    hdfs_params.lookupOnNeed();
    ctx->setHdfsConnectionParams(hdfs_params);
    registerDefaultHdfsFileSystem(hdfs_params, 100000, 100, 10);
    if (!HDFSCommon::exists(test_file))
    {
        HDFSCommon::createFile(test_file);
    }

    FileRanges all_file_ranges{FileRange()};
    const auto & parquet_segments = disk_cache->getStrategy()->transferRangesToSegments<FileDiskCacheSegment>(
        all_file_ranges, "uuid", test_file, ctx->getHdfsConnectionParams());
    ASSERT_EQ(parquet_segments.size(), 1);
    disk_cache->cacheSegmentsToLocalDisk(parquet_segments);
    // ctx->getLocalDiskCacheThreadPool().wait();
    Poco::File dir(disk_cache->getDataDir());
    ASSERT_TRUE(dir.exists());
    const auto & key2 = FileDiskCacheSegment::getSegmentKey("uuid", test_file);
    const auto & localParquetPath = disk_cache->get(key2);
    ASSERT_FALSE(localParquetPath.second.empty());

    Poco::File(disk_cache->getDataDir()).remove(true);
}
*/