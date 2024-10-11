#include <memory>
#include <random>
#include <vector>
#include <fcntl.h>
#include <string.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/MapConfiguration.h>
#include <gtest/gtest.h>
#include <Common/tests/gtest_utils.h>
#include <IO/ReadBufferFromNexusFS.h>
#include <Storages/NexusFS/NexusFS.h>
#include <IO/ReadBufferFromString.h>

using namespace DB;

using namespace Poco;
using namespace Poco::Util;
using namespace std;

class ReadIndirectBuffer final : public ReadBufferFromFileBase
{
public:
    ReadIndirectBuffer(String path_, const String & data_)
        : impl(ReadBufferFromString(data_)), path(std::move(path_))
    {
        internal_buffer = impl.buffer();
        working_buffer = internal_buffer;
        pos = working_buffer.begin();
    }

    std::string getFileName() const override { return path; }

    off_t getPosition() override { return pos - working_buffer.begin(); }

    size_t getFileSize() override { return working_buffer.size(); }

    off_t seek(off_t off, int whence) override
    {
        impl.swap(*this);
        off_t result = impl.seek(off, whence);
        impl.swap(*this);
        return result;
    }

    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) override
    {
        if (n == 0)
            return 0;

        size_t bytes_read = std::min(n, working_buffer.size() - range_begin);
        std::memcpy(to, working_buffer.begin() + range_begin, bytes_read);

        if (bytes_read && progress_callback)
            progress_callback(bytes_read);
        return bytes_read;
    }

private:
    ReadBufferFromString impl;
    const String path;
};

TEST(ReadBufferFromNexusFSTest, Read)
{
    const UInt32 segment_size = 128;
    AutoPtr<AbstractConfiguration> conf(new MapConfiguration());
    conf->setBool("nexus_fs.use_memory_device", true);
    conf->setUInt64("nexus_fs.cache_size", 512 * 10);
    conf->setUInt64("nexus_fs.region_size", 512);
    conf->setUInt64("nexus_fs.segment_size", segment_size);
    conf->setUInt("nexus_fs.alloc_align_size", 32);
    conf->setUInt("nexus_fs.io_align_size", 32);
    conf->setUInt("nexus_fs.clean_regions_pool", 3);
    conf->setUInt("nexus_fs.clean_region_threads", 2);
    conf->setUInt("nexus_fs.num_in_mem_buffers", 6);
    conf->setBool("nexus_fs.enable_memory_buffer", true);
    conf->setUInt("nexus_fs.reader_threads", 8);
    conf->setUInt64("nexus_fs.memory_buffer_size", 128 * 6);
    conf->setDouble("nexus_fs.memory_buffer_cooling_percent", 0.4);
    conf->setDouble("nexus_fs.memory_buffer_freed_percent", 0.2);

    NexusFSConfig nexusfs_conf;
    nexusfs_conf.loadFromConfig(*conf);
    auto nexus_fs = std::make_unique<NexusFS>(std::move(nexusfs_conf));

    constexpr int small_len = 10;
    String small_data = "1234567890";

    constexpr int large_len = 1030;
    String large_data;
    for (int i = 0; i < large_len; i++)
        large_data.push_back(i % 26 + 'a');
    for (int i = 0; i < large_len; i += segment_size)
    {
        auto s = fmt::format("seg#{}", i / segment_size);
        for (int j = 0; j < s.size(); j++)
        {
            if (i + j < large_len)
                large_data[i + j] = s[j];
        }
    }

    // small read
    {
        auto source = std::make_unique<ReadIndirectBuffer>("file1", small_data);
        ReadBufferFromNexusFS read_buffer(segment_size, true, std::move(source), *nexus_fs);

        char buffer[small_len + 5];
        memset(buffer, 0, small_len + 5);
        auto bytes_read = read_buffer.readBig(buffer, small_len);

        ASSERT_EQ(bytes_read, small_len);
        EXPECT_STREQ(buffer, small_data.c_str()); 
    }

    // large read
    {
        auto source = std::make_unique<ReadIndirectBuffer>("file2", large_data);
        ReadBufferFromNexusFS read_buffer(segment_size, false, std::move(source), *nexus_fs);

        char buffer[large_len + 5];
        memset(buffer, 0, large_len + 5);
        auto bytes_read = read_buffer.readBig(buffer, large_len);

        ASSERT_EQ(bytes_read, large_len);
        EXPECT_STREQ(buffer, large_data.c_str()); 
    }

    // with seek
    {
        constexpr int off = 200;
        auto source = std::make_unique<ReadIndirectBuffer>("file3", large_data);
        ReadBufferFromNexusFS read_buffer(segment_size, false, std::move(source), *nexus_fs);

        char buffer[large_len - off + 5];
        memset(buffer, 0, large_len - off + 5);
        read_buffer.seek(off, SEEK_SET);
        auto bytes_read = read_buffer.readBig(buffer, large_len - off);

        ASSERT_EQ(bytes_read, large_len - off);
        EXPECT_STREQ(buffer, large_data.substr(off).c_str()); 
    }

    // read nexus_fs disk cache
    {
        String data;
        auto fake_source = std::make_unique<ReadIndirectBuffer>("file2", data);
        ReadBufferFromNexusFS read_buffer(segment_size, false, std::move(fake_source), *nexus_fs);

        char buffer[large_len + 5];
        memset(buffer, 0, large_len + 5);
        auto bytes_read = read_buffer.readBig(buffer, large_len);

        ASSERT_EQ(bytes_read, large_len);
        EXPECT_STREQ(buffer, large_data.c_str()); 
    }

    // multi thread
    {
        constexpr int n = 20;
        std::vector<std::thread> threads(n);
        for (int i = 0; i < n; i++)
            threads[i] = std::thread([&](){
                auto source = std::make_unique<ReadIndirectBuffer>("file4", large_data);
                ReadBufferFromNexusFS read_buffer(segment_size, true, std::move(source), *nexus_fs);

                char buffer[large_len + 5];
                memset(buffer, 0, large_len + 5);
                auto bytes_read = read_buffer.readBig(buffer, large_len);

                ASSERT_EQ(bytes_read, large_len);
                EXPECT_STREQ(buffer, large_data.c_str()); 
            });
        
        for (int i = 0; i < n; i++)
            threads[i].join();
    }

    // multi thread, non-aligned buffer size
    {
        constexpr int n = 20;
        std::vector<std::thread> threads(n);
        for (int i = 0; i < n; i++)
            threads[i] = std::thread([&](){
                auto source = std::make_unique<ReadIndirectBuffer>("file5", large_data);
                ReadBufferFromNexusFS read_buffer(93, true, std::move(source), *nexus_fs);

                char buffer[large_len + 5];
                memset(buffer, 0, large_len + 5);
                auto bytes_read = read_buffer.readBig(buffer, large_len);

                ASSERT_EQ(bytes_read, large_len);
                EXPECT_STREQ(buffer, large_data.c_str()); 
            });
        
        for (int i = 0; i < n; i++)
            threads[i].join();
    }

    // multi thread, with random seek
    {
        constexpr int n = 20;
        constexpr int round = 20;
        constexpr int local_buffer_size = 500;
        std::vector<std::thread> threads(n);
        for (int i = 0; i < n; i++)
            threads[i] = std::thread([&](){
                auto source = std::make_unique<ReadIndirectBuffer>("file6", large_data);
                ReadBufferFromNexusFS read_buffer(segment_size, false, std::move(source), *nexus_fs);
                std::default_random_engine local_generator;
                local_generator.seed(i);

                for (int j = 0; j < round; j++)
                {
                    char buffer[local_buffer_size + 5];
                    memset(buffer, 0, local_buffer_size + 5);

                    off_t offset = local_generator() % (large_len - local_buffer_size - 10);
                    read_buffer.seek(offset, SEEK_SET);
                    auto bytes_read = read_buffer.read(buffer, local_buffer_size);

                    ASSERT_EQ(bytes_read, local_buffer_size);
                    EXPECT_STREQ(buffer, large_data.substr(offset, local_buffer_size).c_str());
                }
            });
        
        for (int i = 0; i < n; i++)
            threads[i].join();
    }

    // read until pos
    {
        constexpr int until_pos = 678;
        constexpr int offset = 123;
        auto source = std::make_unique<ReadIndirectBuffer>("file2", large_data);
        ReadBufferFromNexusFS read_buffer(segment_size, true, std::move(source), *nexus_fs);

        char buffer[until_pos - offset + 5];
        memset(buffer, 0, until_pos - offset + 5);
        read_buffer.seek(offset, SEEK_SET);
        read_buffer.setReadUntilPosition(until_pos);
        auto bytes_read = read_buffer.read(buffer, large_len);

        ASSERT_EQ(bytes_read, until_pos - offset);
        EXPECT_STREQ(buffer, large_data.substr(offset, bytes_read).c_str()); 
    }

    // read until end
    {
        constexpr int offset = 200;
        auto source = std::make_unique<ReadIndirectBuffer>("file5", large_data);
        ReadBufferFromNexusFS read_buffer(segment_size, true, std::move(source), *nexus_fs);

        char buffer[large_len - offset + 5];
        memset(buffer, 0, large_len - offset + 5);
        read_buffer.seek(offset, SEEK_SET);
        read_buffer.setReadUntilEnd();
        auto bytes_read = read_buffer.read(buffer, large_len);

        ASSERT_EQ(bytes_read, large_len - offset);
        EXPECT_STREQ(buffer, large_data.substr(offset).c_str());
    }

    // readInto
    {
        constexpr int off = 256;
        String data;
        auto fake_source = std::make_unique<ReadIndirectBuffer>("file5", data);
        ReadBufferFromNexusFS read_buffer(segment_size, false, std::move(fake_source), *nexus_fs);

        char buffer[large_len - off + 5];
        memset(buffer, 0, large_len - off + 5);

        size_t bytes_read = 0;
        while (true)
        {
            auto ret = read_buffer.readInto(buffer + bytes_read, large_len, off + bytes_read, 0);
            if (ret.size == 0)
                break;
            else
                bytes_read += ret.size;
        }

        ASSERT_EQ(bytes_read, large_len - off);
        EXPECT_STREQ(buffer, large_data.substr(off).c_str()); 
    }

    conf.reset();
}
