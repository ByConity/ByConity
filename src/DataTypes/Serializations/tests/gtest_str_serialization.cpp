#include <gtest/gtest.h>
#include <cstdlib>
#include <Columns/ColumnString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <DataTypes/Serializations/SerializationBigString.h>
#include <DataTypes/DataTypeString.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressedReadBuffer.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>
#include <Compression/CompressedDataIndex.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>

using namespace DB;

TEST(SerializationBigString, SerDeMultiStream)
{
    String offset_buf;
    String element_buf;

    {
        WriteBufferFromString offset_out(offset_buf);
        WriteBufferFromString element_out(element_buf);

        auto col_str = ColumnString::create();
        col_str->insert("abc");
        col_str->insert("def");

        ISerialization::SerializeBinaryBulkSettings settings;
        ISerialization::SerializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = true;
        settings.getter = [&offset_out, &element_out](const ISerialization::SubstreamPath& path) {
            if (path.back().type == ISerialization::Substream::StringOffsets)
                return &offset_out;
            else if (path.back().type == ISerialization::Substream::StringElements)
                return &element_out;
            __builtin_unreachable();
        };

        SerializationBigString serialization;
        serialization.serializeBinaryBulkWithMultipleStreams(*col_str, 0, 2, settings, state);
    }

    {
        ReadBufferFromString offset_in(offset_buf);
        ReadBufferFromString element_in(element_buf);

        ColumnPtr col_str = ColumnString::create();

        ISerialization::DeserializeBinaryBulkSettings settings;
        ISerialization::DeserializeBinaryBulkStatePtr state;
        settings.getter = [&offset_in, &element_in] (const ISerialization::SubstreamPath& path) {
            if (path.back().type == ISerialization::Substream::StringOffsets)
                return &offset_in;
            else if (path.back().type == ISerialization::Substream::StringElements)
                return &element_in;
            __builtin_unreachable();
        };

        SerializationBigString serialization;
        serialization.deserializeBinaryBulkWithMultipleStreams(col_str, 2, settings,
            state, nullptr);

        ASSERT_EQ(col_str->size(), 2);
        ASSERT_EQ(col_str->getDataAt(0), String("abc"));
        ASSERT_EQ(col_str->getDataAt(1), String("def"));
    }
}

TEST(SerializationBigString, SerDeSingleStream)
{
    String buffer;

    {
        WriteBufferFromString out(buffer);

        auto col_str = ColumnString::create();
        col_str->insert("abc");
        col_str->insert("def");

        ISerialization::SerializeBinaryBulkSettings settings;
        ISerialization::SerializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = true;
        settings.getter = [&out](const auto&) { return &out; };

        SerializationBigString serialization;
        serialization.serializeBinaryBulkWithMultipleStreams(*col_str, 0, 2, settings, state);
    }

    {
        ReadBufferFromString in(buffer);

        ColumnPtr col_str = ColumnString::create();

        ISerialization::DeserializeBinaryBulkSettings settings;
        ISerialization::DeserializeBinaryBulkStatePtr state;
        settings.getter = [&](const auto&) { return &in; };

        SerializationBigString serialization;
        serialization.deserializeBinaryBulkWithMultipleStreams(col_str, 2, settings,
            state, nullptr);

        ASSERT_EQ(col_str->size(), 2);
        ASSERT_EQ(col_str->getDataAt(0), String("abc"));
        ASSERT_EQ(col_str->getDataAt(1), String("def"));
    }
}

struct BigStrDeserializeCtx
{
    BigStrDeserializeCtx(const String& offset_buf, const String& chars_buf,
        const String& idx_buf_): offset_in(offset_buf), chars_in(chars_buf),
            compressed_chars_in(chars_in), idx_buf(idx_buf_)
    {
        settings.getter = [this](const ISerialization::SubstreamPath& path) -> ReadBuffer* {
            if (path.back().type == ISerialization::Substream::StringOffsets)
                return &offset_in;
            else if (path.back().type == ISerialization::Substream::StringElements)
                return &compressed_chars_in;
            __builtin_unreachable();
        };
        settings.compressed_idx_getter = [this](const ISerialization::SubstreamPath&) {
            ReadBufferFromString idx_in(idx_buf);
            return CompressedDataIndex::openForRead(&idx_in);
        };
    }

    ReadBufferFromString offset_in;
    ReadBufferFromString chars_in;
    CompressedReadBuffer compressed_chars_in;

    const String& idx_buf;

    ISerialization::DeserializeBinaryBulkSettings settings;
};

void bigStrSerialize(const std::vector<String>& data, String& offset_buf,
    String& chars_buf, String& idx_buf)
{
    WriteBufferFromString offset_out(offset_buf);
    WriteBufferFromString chars_out(chars_buf);
    WriteBufferFromString idx_out(idx_buf);

    std::unique_ptr<CompressedDataIndex> idx = CompressedDataIndex::openForWrite(&idx_out);

    CompressedWriteBuffer compressed_chars_out(chars_out,
        CompressionCodecFactory::instance().getDefaultCodec(), 2);
    compressed_chars_out.setStatisticsCollector(idx.get());

    auto col_str = ColumnString::create();
    for (const String& str : data)
    {
        col_str->insert(str);
    }

    ISerialization::SerializeBinaryBulkSettings settings;
    ISerialization::SerializeBinaryBulkStatePtr state;
    settings.position_independent_encoding = true;
    settings.getter = [&offset_out, &compressed_chars_out](const ISerialization::SubstreamPath& path) -> WriteBuffer* {
        if (path.back().type == ISerialization::Substream::StringOffsets)
            return &offset_out;
        else if (path.back().type == ISerialization::Substream::StringElements)
            return &compressed_chars_out;
        __builtin_unreachable();
    };

    SerializationBigString serialization;
    serialization.serializeBinaryBulkWithMultipleStreams(*col_str, 0, data.size(),
        settings, state);
}

TEST(SerializationBigString, CompressedMultiStream)
{
    String offset_buf;
    String chars_buf;
    String idx_buf;

    bigStrSerialize({"abc", "def", "ghij"}, offset_buf, chars_buf, idx_buf);

    BigStrDeserializeCtx ctx(offset_buf, chars_buf, idx_buf);

    ISerialization::DeserializeBinaryBulkStatePtr state;

    ColumnPtr col_str = ColumnString::create();

    SerializationBigString serialization;
    serialization.deserializeBinaryBulkStatePrefix(ctx.settings, state);
    serialization.deserializeBinaryBulkWithMultipleStreams(col_str, 3, ctx.settings, state, nullptr);

    ASSERT_EQ(col_str->size(), 3);
    ASSERT_EQ(col_str->getDataAt(0), "abc");
    ASSERT_EQ(col_str->getDataAt(1), "def");
    ASSERT_EQ(col_str->getDataAt(2), "ghij");
}

TEST(SerializationBigString, CompressedSkip)
{
    String offset_buf;
    String chars_buf;
    String idx_buf;

    bigStrSerialize({"abc", "def", "ghij"}, offset_buf, chars_buf, idx_buf);

    BigStrDeserializeCtx ctx(offset_buf, chars_buf, idx_buf);
    std::vector<UInt8> filter = {1, 0, 1};
    ctx.settings.filter = filter.data();

    ColumnPtr col_str = ColumnString::create();

    SerializationBigString serialization;

    ISerialization::DeserializeBinaryBulkStatePtr state;
    serialization.deserializeBinaryBulkStatePrefix(ctx.settings, state);
    serialization.deserializeBinaryBulkWithMultipleStreams(col_str, 3, ctx.settings, state, nullptr);

    ASSERT_EQ(col_str->size(), 2);
    ASSERT_EQ(col_str->getDataAt(0), "abc");
    ASSERT_EQ(col_str->getDataAt(1), "ghij");
}

TEST(SerializationBigString, CompressedFilterAny)
{
    String offset_buf;
    String chars_buf;
    String idx_buf;

    size_t data_size = 10000;
    std::vector<String> data;
    for (size_t i = 0; i < data_size; ++i)
    {
        data.push_back("Value_" + std::to_string(i));
    }

    bigStrSerialize(data, offset_buf, chars_buf, idx_buf);

    BigStrDeserializeCtx ctx(offset_buf, chars_buf, idx_buf);
    std::vector<UInt8> filter(data_size, 0);
    ctx.settings.filter = filter.data();

    ColumnPtr col_str = ColumnString::create();

    SerializationBigString serialization;

    ISerialization::DeserializeBinaryBulkStatePtr state;
    serialization.deserializeBinaryBulkStatePrefix(ctx.settings, state);
    serialization.deserializeBinaryBulkWithMultipleStreams(col_str, data_size, ctx.settings, state, nullptr);

    ASSERT_EQ(col_str->size(), 0);
}

TEST(SerializationBigString, CompressedFilterAll)
{
    String offset_buf;
    String chars_buf;
    String idx_buf;

    size_t data_size = 10000;
    std::vector<String> data;
    for (size_t i = 0; i < data_size; ++i)
    {
        data.push_back("Value_" + std::to_string(i));
    }

    bigStrSerialize(data, offset_buf, chars_buf, idx_buf);

    BigStrDeserializeCtx ctx(offset_buf, chars_buf, idx_buf);
    std::vector<UInt8> filter(data_size, 1);
    ctx.settings.filter = filter.data();

    ColumnPtr col_str = ColumnString::create();

    SerializationBigString serialization;

    ISerialization::DeserializeBinaryBulkStatePtr state;
    serialization.deserializeBinaryBulkStatePrefix(ctx.settings, state);
    serialization.deserializeBinaryBulkWithMultipleStreams(col_str, data_size, ctx.settings, state, nullptr);

    ASSERT_EQ(col_str->size(), data_size);
    for (size_t i = 0; i < data_size; ++i)
    {
        ASSERT_EQ(col_str->getDataAt(i), data[i]);
    }
}

TEST(SerializationBigString, CompressedFilterRandom)
{
    String offset_buf;
    String chars_buf;
    String idx_buf;

    size_t data_size = 10000;
    std::vector<String> data;
    for (size_t i = 0; i < data_size; ++i)
    {
        data.push_back("Value_" + std::to_string(i));
    }

    bigStrSerialize(data, offset_buf, chars_buf, idx_buf);

    BigStrDeserializeCtx ctx(offset_buf, chars_buf, idx_buf);
    std::vector<UInt8> filter(data_size, 0);
    size_t expect_readed = 0;
    for (size_t i = 0; i < data_size; ++i)
    {
        if (rand() % 2 == 0)
        {
            filter[i] = 1;
            ++expect_readed;
        }
    }
    ctx.settings.filter = filter.data();

    ColumnPtr col_str = ColumnString::create();

    SerializationBigString serialization;

    ISerialization::DeserializeBinaryBulkStatePtr state;
    serialization.deserializeBinaryBulkStatePrefix(ctx.settings, state);
    serialization.deserializeBinaryBulkWithMultipleStreams(col_str, data_size, ctx.settings, state, nullptr);

    ASSERT_EQ(col_str->size(), expect_readed);
    for (size_t i = 0, val_idx = 0; i < data_size; ++i)
    {
        if (filter[i] == 1)
        {
            ASSERT_EQ(col_str->getDataAt(val_idx), data[i]);
            ++val_idx;
        }
    }
}
