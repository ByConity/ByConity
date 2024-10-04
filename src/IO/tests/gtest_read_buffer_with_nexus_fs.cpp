#include <memory>
#include <random>
#include <vector>
#include <fcntl.h>
#include <string.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/MapConfiguration.h>
#include <gtest/gtest.h>
#include <Common/tests/gtest_utils.h>
#include <IO/ReadBufferFromFileWithNexusFS.h>
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

TEST(ReadBufferFromFileWithNexusFSTest, Read)
{
    AutoPtr<AbstractConfiguration> conf(new MapConfiguration());
    conf->setBool("nexus_fs.use_memory_device", true);
    conf->setUInt64("nexus_fs.cache_size", 64 * MiB);
    conf->setUInt64("nexus_fs.region_size", 512);
    conf->setUInt64("nexus_fs.segment_size", 128);
    conf->setUInt("nexus_fs.alloc_align_size", 32);
    conf->setUInt("nexus_fs.io_align_size", 32);

    NexusFSConfig nexusfs_conf;
    nexusfs_conf.loadFromConfig(*conf);
    auto nexus_fs = std::make_unique<NexusFS>(std::move(nexusfs_conf));

    constexpr int small_len = 10;
    String small_data = "1234567890";

    constexpr int large_len = 1030;
    String large_data;
    for (int i = 0; i < large_len; i++)
        large_data.push_back(i % 26 + 'a');

    // small read
    {
        auto source = std::make_unique<ReadIndirectBuffer>("file1", small_data);
        ReadBufferFromFileWithNexusFS read_buffer(128, std::move(source), *nexus_fs);

        char buffer[small_len + 5];
        memset(buffer, 0, small_len + 5);
        auto bytes_read = read_buffer.readBig(buffer, small_len);

        ASSERT_EQ(bytes_read, small_len);
        ASSERT_TRUE(strcmp(buffer, small_data.c_str()) == 0); 
    }

    // large read
    {
        auto source = std::make_unique<ReadIndirectBuffer>("file2", large_data);
        ReadBufferFromFileWithNexusFS read_buffer(128, std::move(source), *nexus_fs);

        char buffer[large_len + 5];
        memset(buffer, 0, large_len + 5);
        auto bytes_read = read_buffer.readBig(buffer, large_len);

        ASSERT_EQ(bytes_read, large_len);
        ASSERT_TRUE(strcmp(buffer, large_data.c_str()) == 0); 
    }

    // with seek
    {
        constexpr int off = 200;
        auto source = std::make_unique<ReadIndirectBuffer>("file3", large_data);
        ReadBufferFromFileWithNexusFS read_buffer(128, std::move(source), *nexus_fs);

        char buffer[large_len - off + 5];
        memset(buffer, 0, large_len - off + 5);
        read_buffer.seek(off, SEEK_SET);
        auto bytes_read = read_buffer.readBig(buffer, large_len - off);

        ASSERT_EQ(bytes_read, large_len - off);
        ASSERT_TRUE(strcmp(buffer, large_data.substr(off).c_str()) == 0); 
    }

    // multi thread
    {
        constexpr int n = 20;
        std::vector<std::thread> threads(n);
        for (int i = 0; i < n; i++)
            threads[i] = std::thread([&](){
                auto source = std::make_unique<ReadIndirectBuffer>("file4", large_data);
                ReadBufferFromFileWithNexusFS read_buffer(128, std::move(source), *nexus_fs);

                char buffer[large_len + 5];
                memset(buffer, 0, large_len + 5);
                auto bytes_read = read_buffer.readBig(buffer, large_len);

                ASSERT_EQ(bytes_read, large_len);
                ASSERT_TRUE(strcmp(buffer, large_data.c_str()) == 0); 
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
                auto source = std::make_unique<ReadIndirectBuffer>("file5", large_data);
                ReadBufferFromFileWithNexusFS read_buffer(128, std::move(source), *nexus_fs);
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
                    ASSERT_TRUE(strcmp(buffer, large_data.substr(offset, local_buffer_size).c_str()) == 0); 
                }
            });
        
        for (int i = 0; i < n; i++)
            threads[i].join();
    }

    // read nexus_fs disk cache
    {
        String data;
        auto fake_source = std::make_unique<ReadIndirectBuffer>("file2", data);
        ReadBufferFromFileWithNexusFS read_buffer(128, std::move(fake_source), *nexus_fs);

        char buffer[large_len + 5];
        memset(buffer, 0, large_len + 5);
        auto bytes_read = read_buffer.readBig(buffer, large_len);

        ASSERT_EQ(bytes_read, large_len);
        ASSERT_TRUE(strcmp(buffer, large_data.c_str()) == 0); 
    }

    // read until pos
    {
        constexpr int until_pos = 678;
        constexpr int offset = 123;
        String data;
        auto fake_source = std::make_unique<ReadIndirectBuffer>("file2", data);
        ReadBufferFromFileWithNexusFS read_buffer(128, std::move(fake_source), *nexus_fs);

        char buffer[until_pos - offset + 5];
        memset(buffer, 0, until_pos - offset + 5);
        read_buffer.seek(offset, SEEK_SET);
        read_buffer.setReadUntilPosition(until_pos);
        auto bytes_read = read_buffer.read(buffer, large_len);

        ASSERT_EQ(bytes_read, until_pos - offset);
        ASSERT_TRUE(strcmp(buffer, large_data.substr(offset, bytes_read).c_str()) == 0); 
    }

    // read until end
    {
        constexpr int offset = 256;
        String data;
        auto fake_source = std::make_unique<ReadIndirectBuffer>("file3", data);
        ReadBufferFromFileWithNexusFS read_buffer(128, std::move(fake_source), *nexus_fs);

        char buffer[large_len - offset + 5];
        memset(buffer, 0, large_len - offset + 5);
        read_buffer.seek(offset, SEEK_SET);
        read_buffer.setReadUntilEnd();
        auto bytes_read = read_buffer.read(buffer, large_len);

        ASSERT_EQ(bytes_read, large_len - offset);
        ASSERT_TRUE(strcmp(buffer, large_data.substr(offset).c_str()) == 0);
    }

    // readInto
    {
        constexpr int off = 200;
        String data;
        auto fake_source = std::make_unique<ReadIndirectBuffer>("file2", data);
        ReadBufferFromFileWithNexusFS read_buffer(128, std::move(fake_source), *nexus_fs);

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
        ASSERT_TRUE(strcmp(buffer, large_data.substr(off).c_str()) == 0); 
    }

    conf.reset();
}
