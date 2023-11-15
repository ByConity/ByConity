#include <chrono>
#include <initializer_list>
#include <thread>
#include <vector>
#include <future>
#include <Common/BlockingQueue.h>
#include <gtest/gtest.h>

using namespace DB;

class BlockingQueueTest: public ::testing::Test {
public:
    template <typename T>
    void verify(const BlockingQueue<T>& queue, const std::vector<T>& expected) {
        ASSERT_EQ(queue.size(), expected.size());

        for (size_t i = 0; i < expected.size(); ++i) {
            ASSERT_EQ(queue.container_[i], expected[i]);
        }
    }

    template <typename T>
    void verify(const std::vector<T>& vec, const std::initializer_list<T>& expected) {
        ASSERT_EQ(vec.size(), expected.size());

        auto iter = expected.begin();
        for (size_t i = 0; i < expected.size(); ++i) {
            ASSERT_EQ(vec[i], *iter);
            ++iter;
        }
    }
};

#define VERIFY(candidate, ...) \
    do { \
        ASSERT_NO_FATAL_FAILURE(verify(candidate, __VA_ARGS__)); \
    } while (0)

TEST_F(BlockingQueueTest, InsertBasic) {
    BlockingQueue<int> queue(4);

    VERIFY(queue, {});

    std::vector<int> vals {1, 2, 3};
    queue.insertBatch(vals, std::nullopt);
    VERIFY(queue, {1, 2, 3});

    vals = {4, 5, 6};
    queue.insertBatch(vals, 0);
    VERIFY(queue, {1, 2, 3, 4});
    VERIFY(vals, {5, 6});
}

TEST_F(BlockingQueueTest, ExtractBasic) {
    BlockingQueue<int> queue(4);

    VERIFY(queue, {});

    std::vector<int> vals {1, 2, 3, 4};
    queue.insertBatch(vals, std::nullopt);
    VERIFY(queue, {1, 2, 3, 4});
    VERIFY(vals, {});

    std::vector<int> res = queue.extractBatch(3, std::nullopt);
    VERIFY(res, {1, 2, 3});

    res = queue.extractBatch(3, 0);
    VERIFY(res, {4});
}

TEST_F(BlockingQueueTest, InsertWaiting) {
    BlockingQueue<int> queue(4);

    VERIFY(queue, {});

    std::vector<int> vals {1, 2, 3};
    queue.insertBatch(vals, std::nullopt);
    VERIFY(queue, {1, 2, 3});
    VERIFY(vals, {});

    vals = {4, 5, 6, 7, 8};
    queue.insertBatch(vals, 10 * 1000 * 1000);
    VERIFY(queue, {1, 2, 3, 4});
    VERIFY(vals, {5, 6, 7, 8});

    queue.extractBatch(1, std::nullopt);
    VERIFY(queue, {2, 3, 4});

    queue.insertBatch(vals, 0);
    VERIFY(queue, {2, 3, 4, 5});
    VERIFY(vals, {6, 7, 8});

    std::future<void> insert_res = std::async(std::launch::async, [&]() {
        queue.insertBatch(vals, std::nullopt);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::future<void> extract_res = std::async(std::launch::async, [&]() {
        std::vector<int> res = queue.extractBatch(6, std::nullopt);
        VERIFY(res, {2, 3, 4, 5, 6, 7});
        VERIFY(queue, {8});
    });

    insert_res.get();
    extract_res.get();
}

TEST_F(BlockingQueueTest, ExtractWaiting) {
    BlockingQueue<int> queue(4);
    VERIFY(queue, {});

    std::vector<int> vals = {1, 2, 3};
    queue.insertBatch(vals, std::nullopt);
    VERIFY(queue, {1, 2, 3});
    VERIFY(vals, {});

    std::vector<int> res = queue.extractBatch(4, 1 * 1000 * 1000);
    VERIFY(queue, {});
    VERIFY(res, {1, 2, 3});

    vals = {4, 5};
    queue.insertBatch(vals, std::nullopt);
    VERIFY(queue, {4, 5});
    VERIFY(vals, {});

    res = queue.extractBatch(3, 0);
    VERIFY(queue, {});
    VERIFY(res, {4, 5});

    vals = {6};
    queue.insertBatch(vals, std::nullopt);
    VERIFY(queue, {6});
    VERIFY(vals, {});

    std::future<void> extract_res = std::async(std::launch::async, [&]() {
        std::vector<int> result = queue.extractBatch(7, std::nullopt);
        VERIFY(queue, {});
        VERIFY(result, {6, 7, 8, 9, 10, 11, 12});
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::future<void> insert_res = std::async(std::launch::async, [&]() {
        std::vector<int> vec_vals {7, 8, 9, 10, 11, 12};
        queue.insertBatch(vec_vals, std::nullopt);
        VERIFY(vec_vals, {});
    });

    extract_res.get();
    insert_res.get();
}
