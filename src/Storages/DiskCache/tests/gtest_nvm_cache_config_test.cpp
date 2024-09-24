#include <filesystem>

#include <gtest/gtest.h>

#include <Storages/DiskCache/BlockCacheReinsertionPolicy.h>
#include <Storages/DiskCache/NvmCacheConfig.h>
#include <Common/Exception.h>
#include <common/scope_guard.h>

namespace DB
{
namespace
{
    inline NvmCacheConfig getNvmTestConfig(const std::string & dir)
    {
        NvmCacheConfig config{};
        config.setSimpleFile(dir + "/nvm_cache", 200 * 1024ULL * 1024ULL);
        config.setDeviceMetadataSize(4 * 1024 * 1024);
        config.setBlockSize(1024);
        config.setReqOrderingShards(10);
        config.blockCache().setRegionSize(4 * 1024 * 1024);
        config.bigHash().setSizePctAndMaxItemSize(50, 100).setBucketSize(1024).setBucketBfSize(8);
        return config;
    }

    const UInt64 blockSize = 1024;
    const std::string file_name = "test";
    const std::vector<std::string> raidPaths = {"test1", "test2"};
    const std::vector<std::string> raidPathsInvalid = {"test1"};
    const UInt64 deviceMetadataSize = 1024 * 1024 * 1024;
    const UInt64 fileSize = 10 * 1024 * 1024;
    const bool truncateFile = false;

    const UInt32 blockCacheRegionSize = 16 * 1024 * 1024;
    const UInt8 blockCacheReinsertionHitsThreshold = 111;
    const UInt32 blockCacheCleanRegions = 4;
    const bool blockCacheDataChecksum = true;
    const std::vector<unsigned int> blockCacheSegmentedFifoSegmentRatio = {111, 222, 333};

    class DummyReinsertionPolicy : public HybridCache::BlockCacheReinsertionPolicy
    {
    public:
        bool shouldReinsert(StringRef) override { return true; }
    };

    const unsigned int bigHashSizePct = 50;
    const UInt32 bigHashBucketSize = 1024;
    const UInt64 bigHashBucketBfSize = 4;
    const UInt64 bigHashSmallItemMaxSize = 512;

    const UInt32 maxConcurrentInserts = 50000;
    const UInt64 maxParcelMemoryMB = 512;

    const unsigned int readerThreads = 40;
    const unsigned int writerThreads = 40;
    const UInt64 reqOrderingShards = 30;
}

TEST(NvmCacheConfigTest, RAID0DeviceSize)
{
    auto file_path = fmt::format("/tmp/navy_device_raid0io_test-{}", ::getpid());
    std::filesystem::create_directory(file_path);
    SCOPE_EXIT(std::filesystem::remove_all(file_path));

    std::vector<std::string> files = {file_path + "/CACHE0", file_path + "/CACHE1", file_path + "/CACHE2", file_path + "/CACHE3"};

    int size = 9 * 1024 * 1024;
    int io_align_size = 4096;
    int stripe_size = 8 * 1024 * 1024;

    std::vector<std::string> file_array;
    file_array.reserve(files.size());
    for (const auto & file : files)
        file_array.push_back(file);

    NvmCacheConfig cfg{};
    cfg.setRaidFiles(file_array, size, true);
    cfg.blockCache().setRegionSize(stripe_size);
    cfg.setBlockSize(io_align_size);

    auto device = createDevice(cfg);
    EXPECT_GT(size * files.size(), device->getSize());
    EXPECT_EQ(files.size() * 8 * 1024 * 1024, device->getSize());
}

TEST(NvmCacheConfigTest, EnginesSetup)
{
    {
        NvmCacheConfig cfg = getNvmTestConfig("/tmp");

        cfg.blockCache().setSize(20 * 1024 * 1024);
        EnginesConfig pair1;
        pair1.blockCache().setRegionSize(0);
        pair1.bigHash().setSizePctAndMaxItemSize(0, 640);
        cfg.addEnginePair(std::move(pair1));

        cfg.setEnginesSelector([](HybridCache::EngineTag tag) { return static_cast<size_t>(tag) % 2; });

        EXPECT_NE(nullptr, createNvmCache(cfg, {}, {}, true, false));
    }

    {
        NvmCacheConfig cfg = getNvmTestConfig("/tmp");

        cfg.blockCache().setSize(20 * 1024 * 1024);
        EnginesConfig pair1;
        pair1.blockCache().setRegionSize(4 * 1024 * 1024);
        pair1.bigHash().setSizePctAndMaxItemSize(5, 640);
        cfg.addEnginePair(std::move(pair1));

        cfg.setEnginesSelector([](HybridCache::EngineTag tag) { return static_cast<size_t>(tag) % 2; });

        EXPECT_NE(nullptr, createNvmCache(cfg, {}, {}, true, false));
    }

    {
        NvmCacheConfig cfg = getNvmTestConfig("/tmp");

        cfg.bigHash().setSizePctAndMaxItemSize(1, 40960);

        cfg.blockCache().setSize(20 * 1024 * 1024);
        EnginesConfig pair1;
        pair1.blockCache().setRegionSize(4 * 1024 * 1024);
        pair1.bigHash().setSizePctAndMaxItemSize(5, 640);
        cfg.addEnginePair(std::move(pair1));

        cfg.setEnginesSelector([](HybridCache::EngineTag tag) { return static_cast<size_t>(tag) % 2; });

        EXPECT_THROW({ createNvmCache(cfg, {}, {}, true, false); }, Exception);
    }

    {
        NvmCacheConfig cfg = getNvmTestConfig("/tmp");

        cfg.blockCache().setSize(20 * 1024 * 1024);
        EnginesConfig pair1;
        pair1.blockCache().setRegionSize(4 * 1024 * 1024);
        pair1.bigHash().setSizePctAndMaxItemSize(5, 640);
        cfg.addEnginePair(std::move(pair1));

        EXPECT_THROW({ createNvmCache(cfg, {}, {}, true, false); }, Exception);
    }

    {
        NvmCacheConfig cfg = getNvmTestConfig("/tmp");

        cfg.blockCache().setSize(20 * 1024 * 1024);
        EnginesConfig pair1;
        pair1.blockCache().setRegionSize(4 * 1024 * 1024);
        pair1.bigHash().setSizePctAndMaxItemSize(5, 640);
        pair1.blockCache().setSize(200 * 1024 * 1024);
        cfg.addEnginePair(std::move(pair1));
        cfg.setEnginesSelector([](HybridCache::EngineTag tag) { return static_cast<size_t>(tag) % 2; });

        EXPECT_THROW({ createNvmCache(cfg, {}, {}, true, false); }, Exception);
    }
}

TEST(NvmCacheConfigTest, Device)
{
    {
        NvmCacheConfig config{};
        config.setBlockSize(blockSize);
        config.setDeviceMetadataSize(deviceMetadataSize);
        EXPECT_EQ(config.getBlockSize(), blockSize);
        EXPECT_EQ(config.getDeviceMetadataSize(), deviceMetadataSize);

        config.setSimpleFile(file_name, fileSize, truncateFile);
        EXPECT_EQ(config.getFileName(), file_name);
        EXPECT_EQ(config.getFileSize(), fileSize);
        EXPECT_EQ(config.isTruncateFile(), truncateFile);
        EXPECT_THROW(config.setRaidFiles(raidPaths, fileSize, truncateFile), Exception);
    }

    {
        NvmCacheConfig config{};
        EXPECT_THROW(config.setRaidFiles(raidPathsInvalid, fileSize, truncateFile), Exception);
        config.setRaidFiles(raidPaths, fileSize, truncateFile);
        EXPECT_EQ(config.getRaidPaths(), raidPaths);
        EXPECT_EQ(config.getFileSize(), fileSize);
        EXPECT_EQ(config.isTruncateFile(), truncateFile);
        EXPECT_THROW(config.setSimpleFile(file_name, fileSize, truncateFile), Exception);
    }
    {
        NvmCacheConfig config{};
        EXPECT_EQ(config.getIoEngine(), HybridCache::IoEngine::Sync);
        EXPECT_EQ(config.getQDepth(), 0);
        EXPECT_THROW(config.enableAsyncIo(0), Exception);
        config.enableAsyncIo(64);
        EXPECT_EQ(config.getIoEngine(), HybridCache::IoEngine::IoUring);
        EXPECT_EQ(config.getQDepth(), 64);
    }
}

TEST(NvmCacheConfigTest, BlockCache)
{
    NvmCacheConfig config{};
    config.blockCache().setRegionSize(blockCacheRegionSize).setCleanRegions(blockCacheCleanRegions).setDataChecksum(blockCacheDataChecksum);
    EXPECT_EQ(config.blockCache().getRegionSize(), blockCacheRegionSize);
    EXPECT_EQ(config.blockCache().getCleanRegions(), blockCacheCleanRegions);
    EXPECT_EQ(config.blockCache().getNumInMemBuffers(), blockCacheCleanRegions * 2);
    EXPECT_EQ(config.blockCache().isDataChecksum(), blockCacheDataChecksum);

    config.blockCache().enableFifo();
    EXPECT_EQ(config.blockCache().isLruEnabled(), false);
    EXPECT_TRUE(config.blockCache().getFifoSegmentRatio().empty());
    config.blockCache().enableSegmentedFifo(blockCacheSegmentedFifoSegmentRatio);
    EXPECT_EQ(config.blockCache().isLruEnabled(), false);
    EXPECT_EQ(config.blockCache().getFifoSegmentRatio(), blockCacheSegmentedFifoSegmentRatio);

    auto custom_policy = std::make_shared<DummyReinsertionPolicy>();

    config = NvmCacheConfig{};
    EXPECT_THROW(
        config.blockCache().enableHitsBasedReinsertion(blockCacheReinsertionHitsThreshold).enablePctBasedReinsertion(50), Exception);
    EXPECT_THROW(config.blockCache().enableCustomReinsertion(custom_policy), Exception);
    EXPECT_EQ(config.blockCache().getReinsertionConfig().getHitsThreshold(), blockCacheReinsertionHitsThreshold);
    EXPECT_EQ(config.blockCache().getReinsertionConfig().getPctThreshold(), 0);
    EXPECT_EQ(config.blockCache().getReinsertionConfig().getCustomPolicy(), nullptr);

    config = NvmCacheConfig{};
    EXPECT_THROW(
        config.blockCache().enablePctBasedReinsertion(50).enableHitsBasedReinsertion(blockCacheReinsertionHitsThreshold), Exception);
    EXPECT_THROW(config.blockCache().enableCustomReinsertion(custom_policy), Exception);
    EXPECT_EQ(config.blockCache().getReinsertionConfig().getPctThreshold(), 50);
    EXPECT_EQ(config.blockCache().getReinsertionConfig().getHitsThreshold(), 0);
    EXPECT_EQ(config.blockCache().getReinsertionConfig().getCustomPolicy(), nullptr);

    config = NvmCacheConfig{};
    EXPECT_THROW(config.blockCache().enablePctBasedReinsertion(200), Exception);

    config = NvmCacheConfig{};
    EXPECT_THROW(config.blockCache().enableCustomReinsertion(custom_policy).enablePctBasedReinsertion(50), Exception);
    EXPECT_THROW(config.blockCache().enableHitsBasedReinsertion(blockCacheReinsertionHitsThreshold), Exception);
    EXPECT_EQ(config.blockCache().getReinsertionConfig().getPctThreshold(), 0);
    EXPECT_EQ(config.blockCache().getReinsertionConfig().getHitsThreshold(), 0);
    EXPECT_EQ(config.blockCache().getReinsertionConfig().getCustomPolicy(), custom_policy);
}

TEST(NvmCacheConfigTest, BigHash)
{
    NvmCacheConfig config{};
    EXPECT_THROW(config.bigHash().setSizePctAndMaxItemSize(200, bigHashSmallItemMaxSize), Exception);
    config.bigHash()
        .setSizePctAndMaxItemSize(bigHashSizePct, bigHashSmallItemMaxSize)
        .setBucketSize(bigHashBucketSize)
        .setBucketBfSize(bigHashBucketBfSize);

    EXPECT_EQ(config.bigHash().getSizePct(), bigHashSizePct);
    EXPECT_EQ(config.bigHash().getBucketSize(), bigHashBucketSize);
    EXPECT_EQ(config.bigHash().getBucketBfSize(), bigHashBucketBfSize);
    EXPECT_EQ(config.bigHash().getSmallItemMaxSize(), bigHashSmallItemMaxSize);
}

TEST(NvmCacheConfigTest, JobScheduler)
{
    NvmCacheConfig config{};
    config.setReaderAndWriterThreads(readerThreads, writerThreads);
    ASSERT_THROW(config.setReqOrderingShards(0), Exception);
    config.setReqOrderingShards(reqOrderingShards);
    EXPECT_EQ(config.getReaderThreads(), readerThreads);
    EXPECT_EQ(config.getWriterThreads(), writerThreads);
    EXPECT_EQ(config.getReqOrderingShards(), reqOrderingShards);
}

TEST(NvmCacheConfigTest, OtherSettings)
{
    NvmCacheConfig config{};
    config.setMaxConcurrentInserts(maxConcurrentInserts);
    config.setMaxParcelMemoryMB(maxParcelMemoryMB);
    EXPECT_EQ(config.getMaxConcurrentInserts(), maxConcurrentInserts);
    EXPECT_EQ(config.getMaxParcelMemoryMB(), maxParcelMemoryMB);
}
}
