#include <fmt/core.h>
#include <gtest/gtest.h>

#include <Columns/IColumn.h>
#include <Core/Types.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <DataTypes/DataTypeHelper.h>
#include <IO/BufferBase.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>

namespace DB::HybridCache
{
TEST(Buffer, Test)
{
    auto b1 = Buffer{makeView("hello world")};
    auto v1 = b1.view().slice(3, 5);
    EXPECT_EQ(makeView("lo wo"), v1);
    auto b2 = Buffer{v1};
    EXPECT_EQ(makeView("lo wo"), b2.view());
    b1 = Buffer{makeView("12345")};
    EXPECT_EQ(makeView("12345"), b1.view());
    auto b3 = std::move(b1);
    EXPECT_TRUE(b1.isNull());
    EXPECT_EQ(makeView("12345"), b3.view());
    b3.shrink(3);
    b1 = std::move(b3);
    EXPECT_EQ(makeView("123"), b1.view());
    auto b1_copy = b1.copy();
    EXPECT_EQ(makeView("123"), b1.view());
    EXPECT_EQ(makeView("123"), b1_copy.view());
}

TEST(Buffer, TestWithTrim)
{
    auto b1 = Buffer{makeView("123hello world")};
    b1.trimStart(3);
    auto v1 = b1.view().slice(3, 5);
    EXPECT_EQ(makeView("lo wo"), v1);
    auto b2 = Buffer{v1};
    EXPECT_EQ(makeView("lo wo"), b2.view());
    b1 = Buffer{makeView("12345")};
    EXPECT_EQ(makeView("12345"), b1.view());
    auto b3 = std::move(b1);
    EXPECT_TRUE(b1.isNull());
    EXPECT_EQ(makeView("12345"), b3.view());
    b3.shrink(3);
    b1 = std::move(b3);
    EXPECT_EQ(makeView("123"), b1.view());
    auto b1_copy = b1.copy();
    EXPECT_EQ(makeView("123"), b1.view());
    EXPECT_EQ(makeView("123"), b1_copy.view());
}

TEST(Buffer, Alignment)
{
    // Save buffers in vector to not let the system reuse memory
    std::vector<Buffer> buffers;
    for (int i = 0; i < 5; i++)
    {
        Buffer buf{6 * 1024, 1024};
        EXPECT_EQ(0, reinterpret_cast<uintptr_t>(buf.data()) % 1024);
        buffers.push_back(std::move(buf));
    }
    auto copy = buffers[0].copy(2048);
    EXPECT_EQ(0, reinterpret_cast<uintptr_t>(copy.data()) % 2048);
}

TEST(Buffer, Equals)
{
    EXPECT_EQ(makeView("abc"), makeView("abc"));
    EXPECT_NE(makeView("abc"), makeView("abx"));
    EXPECT_NE(BufferView{}, makeView("abc"));
    EXPECT_NE(makeView("abc"), BufferView{});
    EXPECT_EQ(BufferView{}, BufferView{});
}

TEST(Buffer, CopyFrom)
{
    Buffer buf{makeView("12345")};
    buf.copyFrom(1, makeView("abc"));
    EXPECT_EQ(makeView("1abc5"), buf.view());
}

TEST(Buffer, CopyFromWithTrim)
{
    Buffer buf{makeView("000012345")};
    buf.trimStart(4);
    buf.copyFrom(1, makeView("abc"));
    EXPECT_EQ(makeView("1abc5"), buf.view());
}

TEST(Buffer, MutableView)
{
    Buffer buf{makeView("12345")};

    auto mutable_view = buf.mutableView();
    std::fill(mutable_view.data(), mutable_view.dataEnd(), 'b');

    for (size_t i = 0; i < buf.size(); i++)
    {
        EXPECT_EQ('b', buf.data()[i]) << i;
    }
}

TEST(Buffer, MutableViewWithTrim)
{
    Buffer buf{makeView("aa12345")};

    buf.trimStart(2);
    auto mutable_view = buf.mutableView();
    std::fill(mutable_view.data(), mutable_view.dataEnd(), 'c');

    for (size_t i = 0; i < buf.size(); i++)
    {
        EXPECT_EQ('c', buf.data()[i]) << i;
    }
}

TEST(Buffer, CopyTo)
{
    auto view = makeView("12345");
    char dst[]{"hello world."};

    view.copyTo(dst + 6);
    EXPECT_STREQ("hello 12345.", dst);
}

TEST(Buffer, MarksInCompressedFile)
{
    MarksInCompressedFile marks(2);
    marks[0].offset_in_compressed_file = 1;
    marks[0].offset_in_decompressed_block = 2;
    marks[1].offset_in_compressed_file = 3;
    marks[1].offset_in_decompressed_block = 4;

    auto view = HybridCache::BufferView{marks.size() * sizeof(MarkInCompressedFile), reinterpret_cast<const UInt8 *>(marks.raw_data())};

    HybridCache::Buffer buffer(view);
    auto * mark0 = reinterpret_cast<MarkInCompressedFile *>(buffer.data());
    ASSERT_EQ(1, mark0->offset_in_compressed_file);
    ASSERT_EQ(2, mark0->offset_in_decompressed_block);

    MarksInCompressedFile marks2;
    MarkInCompressedFile * begin = reinterpret_cast<MarkInCompressedFile *>(buffer.data());
    MarkInCompressedFile * end = reinterpret_cast<MarkInCompressedFile *>(buffer.data() + buffer.size());
    marks2.assign(begin, end);
    ASSERT_EQ(1, marks2[0].offset_in_compressed_file);
    ASSERT_EQ(2, marks2[0].offset_in_decompressed_block);
    ASSERT_EQ(3, marks2[1].offset_in_compressed_file);
    ASSERT_EQ(4, marks2[1].offset_in_decompressed_block);
}

TEST(Buffer, MergeTreeDataPartChecksum)
{
    MergeTreeDataPartChecksums checksums;
    for (int i = 0; i < 10; i++)
        checksums.addFile(fmt::format("test{}", i), 100, {100, 100});

    Buffer buffer(4096);
    auto write_buffer = buffer.asWriteBuffer();
    checksums.write(write_buffer);

    MergeTreeDataPartChecksums checksums2;
    auto read_buffer = buffer.asReadBuffer();
    checksums2.read(read_buffer);

    ASSERT_EQ(checksums.getTotalChecksumUInt128(), checksums2.getTotalChecksumUInt128());
}

TEST(Buffer, PrimaryIndex)
{
    // prepare
    MutableColumns primary_key;
    primary_key.resize(1);
    auto data_type = createBaseDataTypeFromTypeIndex(TypeIndex::Int32);
    primary_key[0] = data_type->createColumn();
    for (UInt32 i = 0; i < 10; i++)
    {
        primary_key[0]->insert(i);
    }

    // serialize
    std::shared_ptr<Memory<>> mem = std::make_shared<Memory<>>(DBMS_DEFAULT_BUFFER_SIZE);
    BufferWithOutsideMemory<WriteBuffer> write_buffer(*mem);

    writeVarUInt(primary_key.size(), write_buffer);
    writeVarUInt(primary_key[0]->size(), write_buffer); // row size

    for (const auto & col : primary_key)
    {
        writeVarUInt(static_cast<UInt64>(col->getDataType()), write_buffer); // write type_index
        auto serializer = createBaseDataTypeFromTypeIndex(col->getDataType())->getDefaultSerialization();
        serializer->serializeBinaryBulk(*col, write_buffer, 0, col->size());
    }

    // deserialize
    ReadBufferFromMemory read_buffer(mem->data(), mem->size());
    UInt64 col_cnt;
    readVarUInt(col_cnt, read_buffer);
    UInt64 row_cnt;
    readVarUInt(row_cnt, read_buffer);

    MutableColumns primary_key2;
    primary_key2.resize(col_cnt);

    for (UInt64 i = 0; i < col_cnt; i++)
    {
        UInt64 type;
        readVarUInt(type, read_buffer);
        auto data_type2 = createBaseDataTypeFromTypeIndex(static_cast<TypeIndex>(type));
        primary_key2[i] = data_type2->createColumn();
        primary_key2[i]->reserve(row_cnt);
        auto serializer = data_type->getDefaultSerialization();
        serializer->deserializeBinaryBulk(*primary_key2[i], read_buffer, row_cnt, 0, false, nullptr);
    }

    // check
    for (UInt32 i = 0; i < 10; i++)
    {
        UInt32 value = (*primary_key2[0])[i].get<UInt32>();
        ASSERT_EQ(i, value);
    }
}
}
