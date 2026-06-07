#include <Common/Logger.h>
#include <Common/Exception.h>
#include <gtest/gtest.h>
#include <bthread/bthread.h>
#include <thread>
#include <immintrin.h>

enum TaskFlags
{
    SLEEP = 0,
    THROW = 1,
    MALLOC = 2,
    COUNT = 3,
};

static void stress_memory()
{
    size_t sz = random() % 1024;
    volatile char *start = new char[sz];
    delete[] start;
}

struct TaskData
{
    bthread_t tid;
    std::bitset<TaskFlags::COUNT> flags;
    std::string msg;
};

void *taskFunc(void *arg)
{
    TaskData *task_data = reinterpret_cast<TaskData *>(arg);

    try
    {
        if (task_data->flags.test(TaskFlags::SLEEP))
            bthread_usleep(1);
        if (task_data->flags.test(TaskFlags::MALLOC))
            stress_memory();
        if (task_data->flags.test(TaskFlags::THROW))
            throw std::runtime_error(task_data->msg);
    }
    catch (const std::runtime_error &)
    {
        if (task_data->flags.test(TaskFlags::MALLOC))
            stress_memory();
        if (task_data->flags.test(TaskFlags::SLEEP))
            bthread_usleep(1);

        if (task_data->flags.test(TaskFlags::THROW))
        {
            try {
                if (task_data->flags.test(TaskFlags::MALLOC))
                    stress_memory();
                if (task_data->flags.test(TaskFlags::SLEEP))
                    bthread_usleep(1);
                throw;
            }
            catch (const std::runtime_error & e)
            {
                if (task_data->flags.test(TaskFlags::MALLOC))
                    stress_memory();
                if (task_data->flags.test(TaskFlags::SLEEP))
                    bthread_usleep(1);
                EXPECT_EQ(e.what(), task_data->msg);
            }
        }
    }
    //printf("%s global:%p exit\n", task_data->msg.c_str(), globals);
    return nullptr;
}

// Test case to check exception handling in bthread context
TEST(BthreadStressTest, HandleException) {
    // ... existing setup code ...
    bthread_setconcurrency(4);
    constexpr int numTasks = 2048; // Increased number of bthread tasks

    auto task_data = std::make_unique<TaskData[]>(numTasks);

    for (int i = 0; i < numTasks; ++i) {
        task_data[i].flags.set(TaskFlags::SLEEP, (i % 8 == 0));
        task_data[i].flags.set(TaskFlags::THROW, (i % 4 == 0));
        task_data[i].flags.set(TaskFlags::MALLOC, (i % 128 == 0));
        task_data[i].msg = "Task " + std::to_string(i);
    }

    // Let 3 task to sleep
    bthread_t waiting_tid;
    bthread_start_background(&waiting_tid, nullptr, [](void* arg) -> void* {
        // Start all tasks
        auto task_data_local = reinterpret_cast<TaskData*>(arg);
        for (size_t i = 0; i < numTasks; ++i)
            bthread_start_background(&task_data_local[i].tid, nullptr, taskFunc, &task_data_local[i]);

        // Wait for all tasks to finish
        for (size_t i = 0; i < numTasks; ++i)
            bthread_join(task_data_local[i].tid, nullptr);

        return nullptr;
    }, task_data.get());

    std::this_thread::sleep_for(std::chrono::seconds(5));
    bthread_join(waiting_tid, nullptr);
}
