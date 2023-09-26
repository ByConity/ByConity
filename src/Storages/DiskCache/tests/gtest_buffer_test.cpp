#include <gtest/gtest.h>

#include <Storages/DiskCache/Buffer.h>

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
}
