#include <Processors/Exchange/DataTrans/MultiPathBoundedQueue.h>
#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>

using namespace DB;
using namespace UnitTest;

TEST(ExchangeQueueTest, QueueDisable)
{
    auto memory_controller = std::make_shared<MemoryController>(0);
    auto queue = std::make_shared<MultiPathBoundedQueue>(20, memory_controller);

    String data = "Hello, world!";
    ASSERT_TRUE(queue->tryEmplace(10, MultiPathDataPacket(std::move(data))));
    EXPECT_EQ(memory_controller->size(), 0);

    MultiPathDataPacket packet;
    ASSERT_TRUE(queue->tryPop(packet));
    EXPECT_EQ(memory_controller->size(), 0);
    ASSERT_TRUE(std::holds_alternative<String>(packet));
    EXPECT_EQ(std::get<String>(packet).compare("Hello, world!"), 0);
}

TEST(ExchangeQueueTest, QueueEnableWithoutAlternative)
{
    auto memory_controller = std::make_shared<MemoryController>(100);
    auto queue = std::make_shared<MultiPathBoundedQueue>(20, memory_controller);

    size_t data_len = 0;
    for (size_t i = 1; i <= 9; ++i)
    {
        String data = "Hello, world!";
        data_len = data.length();
        bool ret = queue->tryEmplace(10, MultiPathDataPacket(std::move(data)));
        if (i <= 8)
        {
            ASSERT_TRUE(ret);
            EXPECT_EQ(memory_controller->size(), data_len * i);
        }
        else
        {
            ASSERT_FALSE(ret);
            EXPECT_EQ(memory_controller->size(), data_len * (i - 1));
        }
    }

    MultiPathDataPacket packet;
    ASSERT_TRUE(queue->tryPop(packet));
    EXPECT_EQ(memory_controller->size(), data_len * 7);
    ASSERT_TRUE(std::holds_alternative<String>(packet));
    EXPECT_EQ(std::get<String>(packet).compare("Hello, world!"), 0);

    queue->clear();
    EXPECT_EQ(memory_controller->size(), 0);
}

TEST(ExchangeQueueTest, QueueEnableWithAlternative)
{
    auto memory_controller = std::make_shared<MemoryController>(100);
    auto queue = std::make_shared<MultiPathBoundedQueue>(20, memory_controller);

    String data = "Hello, world!";
    size_t data_len = data.length();
    ASSERT_TRUE(queue->tryEmplace(10, std::move(data)));
    EXPECT_EQ(memory_controller->size(), data_len);

    MultiPathDataPacket packet;
    ASSERT_TRUE(queue->tryPop(packet));
    EXPECT_EQ(memory_controller->size(), 0);
    ASSERT_TRUE(std::holds_alternative<String>(packet));
    EXPECT_EQ(std::get<String>(packet).compare("Hello, world!"), 0);
}
