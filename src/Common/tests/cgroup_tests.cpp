#include <iostream>

#include <gtest/gtest.h>

#include <Common/CGroup/CGroupManager.h>
#include <Common/CGroup/CGroupManagerFactory.h>
#include <Common/ThreadPool.h>

using namespace DB;

void test_cpu(std::vector<ThreadFromGlobalPool> & threads)
{
    CGroupManager & cgroup_manager = CGroupManagerFactory::instance();
    CpuControllerPtr cpu = cgroup_manager.createCpu("test", 1025);
    for (auto & thread : threads)
    {
        cpu->addTask(thread.gettid());
    }

    ASSERT_EQ(cpu->getTasks().size(), threads.size());
    ASSERT_EQ(cpu->getShare(), 1025);
}

TEST(CGroup, Flow)
{
    std::vector<ThreadFromGlobalPool> threads;
    threads.reserve(20);
    for (int i = 0; i < 20; ++i)
    {
        threads.emplace_back([](){
            std::this_thread::sleep_for(std::chrono::milliseconds(5000));
        });
    }
    test_cpu(threads);
    for (auto & thread : threads)
    {
        thread.join();
    }
}
