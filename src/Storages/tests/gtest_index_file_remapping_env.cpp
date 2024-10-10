#include <filesystem>
#include <memory>
#include <unordered_map>
#include <fcntl.h>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <Storages/IndexFile/RemappingEnv.h>
#include "common/types.h"
#include "Core/Defines.h"
#include "Core/UUID.h"
#include "Disks/IDisk.h"
#include "Storages/IndexFile/Env.h"

using namespace DB;
using namespace DB::IndexFile;

namespace
{

String randomString(size_t length)
{
    static const char alphanum[] = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";

    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<int> distribution(0, sizeof(alphanum) - 2);

    //    srand((unsigned) time(NULL) * getpid());

    String str(length, '\0');
    for (size_t i = 0; i < length; i++)
    {
        str[i] = alphanum[distribution(generator)];
    }
    return str;
}

}

class IndexFileRemappingEnvTest: public testing::Test
{
public:
    static void SetUpTestSuite()
    {
        store_path = "./" + UUIDHelpers::UUIDToString(UUIDHelpers::generateV4()) + "/";
        fs::create_directories(store_path);
    }

    static void TearDownTestSuite()
    {
        fs::remove_all(store_path);
    }

    static String filePath(const String& file_name_)
    {
        return fs::path(store_path) / file_name_;
    }

    static std::unique_ptr<WriteBufferFromFileBase> writeFile(const String& file_name_)
    {
        return std::make_unique<WriteBufferFromFile>(filePath(file_name_),
            DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);
    }

private:
    static String store_path;
};

String IndexFileRemappingEnvTest::store_path;

TEST_F(IndexFileRemappingEnvTest, Basic)
{
    WriteRemappingEnv env(writeFile("basic"));

    ASSERT_TRUE(env.NewRandomAccessFile("", nullptr).IsNotSupportedError());
    ASSERT_TRUE(env.NewRandomAccessRemoteFileWithCache({}, nullptr, nullptr)
        .IsNotSupportedError());
    ASSERT_TRUE(env.DeleteFile("").IsNotSupportedError());

    /// Write something
    {
        std::unique_ptr<WritableFile> file = nullptr;
        ASSERT_TRUE(env.NewWritableFile("file0", &file).ok());

        ASSERT_TRUE(env.NewWritableFile("file1", nullptr).IsInvalidArgument());

        String data = "0123";
        file->Append(data);
        file->Flush();
        file->Sync(true);
        file->Close();
        file = nullptr;

        ASSERT_TRUE(env.FileExists("file0"));
        uint64_t file_size = 0;
        ASSERT_TRUE(env.GetFileSize("file0", &file_size).ok());
        ASSERT_EQ(file_size, data.size());
    }

    ASSERT_TRUE(env.NewWritableFile("file0", nullptr).IsInvalidArgument());
    ASSERT_TRUE(env.finalize().ok());
    ASSERT_TRUE(env.NewWritableFile("file0", nullptr).IsInvalidArgument());
}

TEST_F(IndexFileRemappingEnvTest, ReadWriteMultipleFiles)
{
    std::unordered_map<String, String> file_content;
    {
        WriteRemappingEnv env(writeFile("multi_files"));

        /// Write something
        for (size_t i = 0; i < 100; ++i)
        {
            String file_name = fmt::format("file_{}", i);
            std::unique_ptr<WritableFile> file = nullptr;
            ASSERT_TRUE(env.NewWritableFile(file_name, &file).ok());

            String data0 = randomString(64);
            String data1 = randomString(64);
            file->Append(data0);
            file->Append(data1);
            file->Close();

            uint64_t file_size = 0;
            ASSERT_TRUE(env.GetFileSize(file_name, &file_size).ok());
            ASSERT_EQ(file_size, 128);

            file_content[file_name] = data0 + data1;
        }

        ASSERT_TRUE(env.finalize().ok());
    }

    {
        ReadRemappingEnv env([](size_t buf_size) {
            return std::make_unique<ReadBufferFromFile>(filePath("multi_files"), buf_size);
        }, fs::file_size(filePath("multi_files")));

        for (const auto& entry : file_content)
        {
            std::unique_ptr<RandomAccessFile> file = nullptr;
            ASSERT_TRUE(env.NewRandomAccessFile(entry.first, &file).ok());

            String data(entry.second.size() + 10, '\0');
            Slice result;
            ASSERT_TRUE(file->Read(0, data.size(), &result, data.data(), nullptr).ok());

            data.resize(result.size());
            ASSERT_EQ(data, entry.second);
        }
    }
}
