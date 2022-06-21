#include <gtest/gtest.h>
#include <Poco/File.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Util/JSONConfiguration.h>
#include <Core/UUIDHelpers.h>
#include <Disks/DiskSpaceMonitor.h>
#include <Disks/registerDisks.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/DiskCache/DiskCacheLRU.h>

using namespace DB;

class DiskCacheTest: public ::testing::Test {
public:
    static void SetUpTestCase() {
        Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter("%Y.%m.%d %H:%M:%S.%F <%p> %s: %t"));
        Poco::AutoPtr<Poco::ConsoleChannel> console_chanel(new Poco::ConsoleChannel);
        Poco::AutoPtr<Poco::FormattingChannel> channel(new Poco::FormattingChannel(formatter, console_chanel));
        Poco::Logger::root().setLevel("trace");
        Poco::Logger::root().setChannel(channel);

        ctx = std::make_unique<Context>(Context::createGlobal());

        registerDisks();
    }

    static void TearDownTestCase() {
    }

    virtual void SetUp() override {
        test_base_dir = "./" + UUIDHelpers::UUIDToString(UUIDHelpers::generateV4()) + "/";
        Poco::File(test_base_dir).createDirectories();
    }

    virtual void TearDown() override {
        Poco::File(test_base_dir).remove(true);
    }

    static void generateData(const String& base_path, int depth, int num_per_level,
            const String& partial_name, std::vector<String>& names) {
        static int counter = 0;
        if (depth == 0) {
            for (int i = 0; i < num_per_level; i++) {
                String cache_name = partial_name + std::to_string(counter++) + ".bin";
                Poco::File(base_path + cache_name).createFile();
                names.push_back(cache_name);
            }
        } else {
            for (int i = 0; i < num_per_level; i++) {
                String name = partial_name + std::to_string(counter++) + "/";
                Poco::File(base_path + name).createDirectories();
                generateData(base_path, depth - 1, num_per_level, name,
                    names);
            }
        }
    }

    static void writeV1Meta(const String& meta_path, const std::vector<String>& metas) {
        WriteBufferFromFile writer(meta_path);

        UInt8 version = 1;
        writeString("LRU0", writer);
        writeIntBinary(version, writer);
        writeIntBinary(segment_size, writer);
        writeNull(256 - writer.count(), writer);

        for (const String& meta : metas) {
            writeStringBinary(meta, writer);
        }
    }

    static void writeV2Meta(const String& meta_path,
            const std::vector<std::pair<String, std::vector<String>>>& metas) {
        WriteBufferFromFile writer(meta_path);

        UInt8 version = 2;
        writeString("LRU0", writer);
        writeIntBinary(version, writer);
        writeIntBinary(segment_size, writer);
        writeNull(256 - writer.count(), writer);

        for (const auto& meta : metas) {
            const String& disk_name = meta.first;
            for (const String& name : meta.second) {
                writeStringBinary(name, writer);
                writeStringBinary(disk_name, writer);
            }
        }
    }

    static VolumePtr newVolume(const String& json_cfg) {
        std::stringstream ss(json_cfg);
        std::unique_ptr<Poco::Util::JSONConfiguration> cfg = std::make_unique<Poco::Util::JSONConfiguration>();
        cfg->load(ss);

        DiskSelectorPtr disk_selector = std::make_shared<DiskSelector>(*cfg, "disks");
        return std::make_shared<Volume>(VolumeType::LOCAL_VOLUME, *cfg, "volume",
            disk_selector);
    }

    String singleDiskVolumeStr() {
        String cfg = "{\n"
            "    \"disks\": {\n"
            "        \"disk_0\": {\n"
            "            \"path\": \"BASE_DIR/disk_0/\"\n"
            "        }\n"
            "    },\n"
            "    \"volume\": {\n"
            "        \"default\": \"disk_0\",\n"
            "        \"disk\": \"disk_0\"\n"
            "    }\n"
            "}";

        replaceStr(cfg, "BASE_DIR", test_base_dir);

        return cfg;
    }

    String dualDiskVolumeStr() {
        String cfg = "{\n"
            "    \"disks\": {\n"
            "        \"disk_0\": {\n"
            "            \"path\": \"BASE_DIR/disk_0/\"\n"
            "        },\n"
            "        \"disk_1\": {\n"
            "            \"path\": \"BASE_DIR/disk_1/\"\n"
            "        }\n"
            "    },\n"
            "    \"volume\": {\n"
            "        \"default\": \"disk_0\",\n"
            "        \"disk0\": \"disk_0\",\n"
            "        \"disk1\": \"disk_1\"\n"
            "    }\n"
            "}";

        replaceStr(cfg, "BASE_DIR", test_base_dir);

        return cfg;
    }

    void replaceStr(String& str, const String& old_str, const String& new_str) {
        std::string::size_type pos = 0u;
        while((pos = str.find(old_str, pos)) != std::string::npos){
            str.replace(pos, old_str.length(), new_str);
            pos += new_str.length();
        }
    }

    String test_base_dir;

    static std::unique_ptr<Context> ctx;
    static constexpr const UInt32 segment_size = 8192;
};

std::unique_ptr<Context> DiskCacheTest::ctx = nullptr;

TEST_F(DiskCacheTest, Collect) {
    VolumePtr volume = newVolume(dualDiskVolumeStr());

    int total_cache_num = 0;
    std::vector<std::pair<String, std::vector<String>>> metas;
    for (const DiskPtr& disk : volume->getDisks()) {
        String disk_cache_dir = disk->getPath() + "disk_cache/";

        std::vector<String> metas_in_disk;
        generateData(disk_cache_dir, 3, 2, "", metas_in_disk);

        metas.push_back({disk->getName(), metas_in_disk});

        total_cache_num += metas_in_disk.size();
    }

    DiskCacheLRU cache(*ctx, 100000000, segment_size, 100, volume, 2, 1);
    
    cache.loadSegmentFromVolume(*volume);
    ASSERT_EQ(cache.count(), total_cache_num);

    for (const auto& meta_in_disk : metas) {
        DiskPtr disk = volume->getDiskByName(meta_in_disk.first);
        for (const String& name : meta_in_disk.second) {
            ASSERT_EQ(cache.get(name), disk->getPath() + "disk_cache/" + name);
        }
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}