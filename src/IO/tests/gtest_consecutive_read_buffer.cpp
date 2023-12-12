#include <sstream>
#include <vector>
#include <gtest/gtest.h>
#include <Core/Types.h>
#include <IO/ConsecutiveReadBuffer.h>
#include <IO/ReadBufferFromIStream.h>

using namespace DB;

void testForSequence(const std::vector<String>& data, bool big_read, size_t page_size)
{
    String merged_data;
    std::vector<std::stringstream> sss;
    std::vector<std::shared_ptr<ReadBufferFromIStream>> readers;
    sss.reserve(data.size());
    for (const String& str : data)
    {
        sss.push_back(std::stringstream(str));
        readers.emplace_back(std::make_shared<ReadBufferFromIStream>(sss.back(), page_size));

        merged_data += str;
    }

    std::vector<ReadBuffer*> reader_ptrs;
    for (auto& reader : readers)
    {
        reader_ptrs.push_back(reader.get());
    }

    ConsecutiveReadBuffer buffer(reader_ptrs);

    String readed_str(merged_data.size(), '\0');

    if (big_read)
    {
        size_t readed = 0;
        ASSERT_NO_THROW(readed = buffer.readBig(readed_str.data(), merged_data.size()));
        ASSERT_EQ(readed, merged_data.size());
    }
    else
    {
        ASSERT_NO_THROW(buffer.readStrict(readed_str.data(), merged_data.size()));
    }

    ASSERT_TRUE(buffer.eof());
}

TEST(ConsecutiveReadBufferTest, Basic)
{
    ASSERT_NO_FATAL_FAILURE(testForSequence({"123", "", "456"}, false, 6));
    ASSERT_NO_FATAL_FAILURE(testForSequence({"123", "", "456"}, true, 6));

    ASSERT_NO_FATAL_FAILURE(testForSequence({"123", "", "", "456"}, false, 6));
    ASSERT_NO_FATAL_FAILURE(testForSequence({"123", "", "", "456"}, true, 6));

    ASSERT_NO_FATAL_FAILURE(testForSequence({"123", "", "456"}, false, 4));
    ASSERT_NO_FATAL_FAILURE(testForSequence({"123", "", "", "456"}, false, 4));

    ASSERT_NO_FATAL_FAILURE(testForSequence({"123", "", "456"}, false, 2));
    ASSERT_NO_FATAL_FAILURE(testForSequence({"123", "", "456"}, true, 1));

    ASSERT_NO_FATAL_FAILURE(testForSequence({"123", "", "", "456"}, false, 2));
    ASSERT_NO_FATAL_FAILURE(testForSequence({"123", "", "", "456"}, true, 1));
}
