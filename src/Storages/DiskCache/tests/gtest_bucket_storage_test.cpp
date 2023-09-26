#include <fmt/core.h>
#include <gtest/gtest.h>

#include <Storages/DiskCache/BucketStorage.h>

namespace DB::HybridCache
{

namespace
{
    size_t getEndOffset()
    {
        return sizeof(BucketStorage);
    }

    template <typename... T>
    size_t getEndOffset(size_t n, T... args)
    {
        return BucketStorage::slotSize(n) + getEndOffset(args...);
    }

    bool checkContent(MutableBufferView view, uint8_t c)
    {
        bool match = true;
        for (size_t i = 0; i < view.size(); ++i)
        {
            SCOPED_TRACE(fmt::format("index of view: {}", i));
            EXPECT_EQ(view.data()[i], c);
            if (view.data()[i] != c)
            {
                match = false;
            }
        }
        return match;
    }
}

TEST(BucketStorage, Allocate)
{
    const uint32_t capacity = 100;
    Buffer buf(capacity + sizeof(BucketStorage));
    auto * allocator = new (buf.data()) BucketStorage(capacity);

    auto v1 = allocator->allocate(10);
    EXPECT_FALSE(v1.done());
    EXPECT_EQ(10, v1.view().size());
    EXPECT_EQ(getEndOffset(10), v1.view().dataEnd() - buf.data());

    auto v2 = allocator->allocate(20);
    EXPECT_FALSE(v2.done());
    EXPECT_EQ(20, v2.view().size());
    EXPECT_EQ(getEndOffset(10, 20), v2.view().dataEnd() - buf.data());

    auto v3 = allocator->allocate(30);
    EXPECT_FALSE(v3.done());
    EXPECT_EQ(30, v3.view().size());
    EXPECT_EQ(getEndOffset(10, 20, 30), v3.view().dataEnd() - buf.data());

    // Allocate for something that we don't have space returns null
    auto null_v = allocator->allocate(30);
    EXPECT_TRUE(null_v.done());
}

TEST(BucketStorage, Iteration)
{
    const uint32_t capacity = 100;
    Buffer buf(capacity + sizeof(BucketStorage));
    auto * allocator = new (buf.data()) BucketStorage(capacity);

    auto v1 = allocator->allocate(10);
    auto v2 = allocator->allocate(15);
    auto v3 = allocator->allocate(20);

    std::fill(v1.view().data(), v1.view().dataEnd(), '1');
    std::fill(v2.view().data(), v2.view().dataEnd(), '2');
    std::fill(v3.view().data(), v3.view().dataEnd(), '3');

    // Make sure we can iterate through them and they're what we expect
    auto itr1 = allocator->getFirst();
    EXPECT_EQ(v1.view().data(), itr1.view().data());
    EXPECT_EQ(v1.view().size(), itr1.view().size());
    EXPECT_EQ(getEndOffset(10), v1.view().dataEnd() - buf.data());
    EXPECT_EQ(v1.view(), itr1.view());

    auto itr2 = allocator->getNext(itr1);
    EXPECT_EQ(v2.view().data(), itr2.view().data());
    EXPECT_EQ(v2.view().size(), itr2.view().size());
    EXPECT_EQ(getEndOffset(10, 15), v2.view().dataEnd() - buf.data());
    EXPECT_EQ(v2.view(), itr2.view());

    auto itr3 = allocator->getNext(itr2);
    EXPECT_EQ(v3.view().data(), itr3.view().data());
    EXPECT_EQ(v3.view().size(), itr3.view().size());
    EXPECT_EQ(getEndOffset(10, 15, 20), v3.view().dataEnd() - buf.data());
    EXPECT_EQ(v3.view(), itr3.view());

    // End is null
    auto itr4 = allocator->getNext(itr3);
    EXPECT_TRUE(itr4.done());

    // Next of null is still null
    auto itr5 = allocator->getNext(itr4);
    EXPECT_TRUE(itr5.done());
}

TEST(BucketStorage, RemoveFromMiddle)
{
    const uint32_t capacity = 100;
    Buffer buf(capacity + sizeof(BucketStorage));
    auto * allocator1 = new (buf.data()) BucketStorage(capacity);
    {
        auto v1 = allocator1->allocate(10);
        auto v2 = allocator1->allocate(15);
        auto v3 = allocator1->allocate(20);

        std::fill(v1.view().data(), v1.view().dataEnd(), '1');
        std::fill(v2.view().data(), v2.view().dataEnd(), '2');
        std::fill(v3.view().data(), v3.view().dataEnd(), '3');
    }

    // Look at the above two allocators, if I remove v2 from the first
    // allocator, they will be identical.
    allocator1->remove(allocator1->getNext(allocator1->getFirst()));

    auto itr1 = allocator1->getFirst();
    auto itr2 = allocator1->getNext(itr1);
    auto itr3 = allocator1->getNext(itr2);
    EXPECT_EQ(10, itr1.view().size());
    EXPECT_EQ(getEndOffset(10), itr1.view().dataEnd() - buf.data());
    EXPECT_TRUE(checkContent(itr1.view(), '1'));
    EXPECT_EQ(20, itr2.view().size());
    EXPECT_EQ(getEndOffset(10, 20), itr2.view().dataEnd() - buf.data());
    EXPECT_TRUE(checkContent(itr2.view(), '3'));
    EXPECT_TRUE(itr3.done());
}

TEST(BucketStorage, RemoveFromLast)
{
    const uint32_t capacity = 100;
    Buffer buf(capacity + sizeof(BucketStorage));
    auto * allocator1 = new (buf.data()) BucketStorage(capacity);
    {
        auto v1 = allocator1->allocate(10);
        auto v2 = allocator1->allocate(15);
        auto v3 = allocator1->allocate(20);

        std::fill(v1.view().data(), v1.view().dataEnd(), '1');
        std::fill(v2.view().data(), v2.view().dataEnd(), '2');
        std::fill(v3.view().data(), v3.view().dataEnd(), '3');
    }

    // Look at the above two allocators, if I remove v2 from the first
    // allocator, they will be identical.
    allocator1->remove(allocator1->getNext(allocator1->getNext(allocator1->getFirst())));

    auto itr1 = allocator1->getFirst();
    auto itr2 = allocator1->getNext(itr1);
    auto itr3 = allocator1->getNext(itr2);
    EXPECT_EQ(10, itr1.view().size());
    EXPECT_EQ(getEndOffset(10), itr1.view().dataEnd() - buf.data());
    EXPECT_TRUE(checkContent(itr1.view(), '1'));
    EXPECT_EQ(15, itr2.view().size());
    EXPECT_EQ(getEndOffset(10, 15), itr2.view().dataEnd() - buf.data());
    EXPECT_TRUE(checkContent(itr2.view(), '2'));
    EXPECT_TRUE(itr3.done());
}

TEST(BucketStorage, RemoveUntil)
{
    const uint32_t capacity = 100;
    Buffer buf(capacity + sizeof(BucketStorage));
    auto * allocator1 = new (buf.data()) BucketStorage(capacity);
    {
        auto v1 = allocator1->allocate(10);
        auto v2 = allocator1->allocate(15);
        auto v3 = allocator1->allocate(20);
        auto v4 = allocator1->allocate(18);

        std::fill(v1.view().data(), v1.view().dataEnd(), '1');
        std::fill(v2.view().data(), v2.view().dataEnd(), '2');
        std::fill(v3.view().data(), v3.view().dataEnd(), '3');
        std::fill(v4.view().data(), v4.view().dataEnd(), '4');
    }

    // After this, alloctor1 and allocator2 should be identical in memory content
    allocator1->removeUntil(allocator1->getNext(allocator1->getNext(allocator1->getFirst())));

    auto itr1 = allocator1->getFirst();
    auto itr2 = allocator1->getNext(itr1);
    EXPECT_EQ(18, itr1.view().size());
    EXPECT_EQ(getEndOffset(18), itr1.view().dataEnd() - buf.data());
    EXPECT_TRUE(checkContent(itr1.view(), '4'));
    EXPECT_TRUE(itr2.done());
}
}
