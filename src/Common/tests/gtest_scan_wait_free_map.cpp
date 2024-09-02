#include <Common/ScanWaitFreeMap.h>
#include <Common/Stopwatch.h>

#include <gtest/gtest.h>

#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>

static constexpr int requests = 512;
static constexpr int find_requests = 1024 * 64;
static constexpr int write_requests = 1024 * 2;
static constexpr int max_threads = 16;
static constexpr int records = 100000;

using TestMap = DB::ScanWaitFreeMap<std::string, std::shared_ptr<std::string>>;

inline void generateRecords(TestMap & map, int records_)
{
    for (int i = 0; i < records_; i++)
    {
        std::string k = "partition-" + std::to_string(i);
        std::shared_ptr<std::string> v = std::make_shared<std::string>(k);
        map.emplace(k, std::move(v));
    }
}

void ScanWaitFreeMapFindOnly()
{
    TestMap map;
    generateRecords(map, records);
    for (int thrs = 1; thrs <= max_threads; thrs *= 2)
    {
        std::vector<std::thread> threads;
        threads.reserve(thrs);
        auto reader = [&]
        {
            for (int request = find_requests / thrs; request; request--)
            {
                map.find("partition-123");
            }
        };

        Stopwatch watch;
        for (int i = 0; i < thrs; i++)
            threads.emplace_back(reader);
        
        for (auto & thread : threads)
            thread.join();
        
        double ns = watch.elapsedNanoseconds();
        std::cout << "thrs = " << thrs << ":\t" << ns / find_requests << " ns\t" << find_requests * 1e9 / ns << " rps" << std::endl;
    }
}


void ScanWaitFreeMapScanOnly()
{
    TestMap map;
    generateRecords(map, records);
    for (int thrs = 1; thrs <= max_threads; thrs *= 2)
    {
        std::vector<std::thread> threads;
        threads.reserve(thrs);
        auto reader = [&]
        {
            for (int request = requests / thrs; request; request--)
            {
                for (auto it = map.begin(); it != map.end(); ++it) {}
            }
        };

        Stopwatch watch;
        for (int i = 0; i < thrs; i++)
            threads.emplace_back(reader);
        
        for (auto & thread : threads)
            thread.join();
        
        double ns = watch.elapsedNanoseconds();
        std::cout << "thrs = " << thrs << ":\t" << ns / requests << " ns\t" << requests * 1e9 / ns << " rps" << std::endl;
    }
}

void ScanWaitFreeMapWriteOnly()
{
    TestMap map;
    generateRecords(map, records);
    std::vector<std::shared_ptr<std::string>> new_values;
    for (int i = 0; i < 100; i++)
        new_values.push_back(std::make_shared<std::string>("new-partition-" + std::to_string(i)));
    for (int thrs = 1; thrs <= max_threads; thrs *= 2)
    {
        std::vector<std::thread> threads;
        threads.reserve(thrs);
        auto writer = [&]()
        {
            for (int request = write_requests / thrs; request; request--)
                map.insert(new_values, [](const std::shared_ptr<std::string> & v) { return *v; });
        };

        Stopwatch watch;
        for (int i = 0; i < thrs; i++)
            threads.emplace_back(writer);
        
        for (auto & thread : threads)
            thread.join();
        double ns = watch.elapsedNanoseconds();
        ASSERT_EQ(map.trashSize(), 0);
        std::cout << "thrs = " << thrs << ":\t" << ns / write_requests << " ns\t" << write_requests * 1e9 / ns << " rps" << std::endl;
    }
}

void ScanWaitFreeMapScanWrite()
{
    DB::ScanWaitFreeMap<std::string, std::shared_ptr<std::string>> map;
    generateRecords(map, records);
    std::vector<std::shared_ptr<std::string>> new_values;
    for (int i = 0; i < 100; i++)
        new_values.push_back(std::make_shared<std::string>("new-partition-" + std::to_string(i)));
    for (int thrs = 1; thrs <= max_threads; thrs *= 2)
    {
        std::vector<std::thread> threads;
        std::vector<std::thread> write_threads;
        threads.reserve(thrs);
        write_threads.reserve(thrs);
        std::atomic<int> finished{0};

        auto reader = [&]()
        {
            for (int request = requests / thrs; request; request--)
            {
                for (auto it = map.begin(); it != map.end(); ++it) {}
            }
            finished++;
        };

        auto writer = [&]()
        {
            while (finished < thrs)
            {
                map.insert(new_values, [](const std::shared_ptr<std::string> & v) { return *v; });
            }
        };

        for (int i = 0; i < thrs; i++)
            write_threads.emplace_back(writer);

        Stopwatch watch;
        for (int i = 0; i < thrs; i++)
            threads.emplace_back(reader);
        
        for (auto & thread : threads)
            thread.join();
        
        double ns = watch.elapsedNanoseconds();

        for (auto & thread : write_threads)
            thread.join();
        ASSERT_EQ(map.trashSize(), 0);
        std::cout << "thrs = " << thrs << ":\t" << ns / requests << " ns\t" << requests * 1e9 / ns << " rps" << std::endl;
    }    
}

void ScanWaitFreeMapBasic()
{
    DB::ScanWaitFreeMap<int, int> map;

    /// Initial check
    std::vector<int> res, expected_res;
    for (auto it = map.begin(); it != map.end(); it++)
        res.push_back(*it);
    ASSERT_EQ(res, expected_res);
    ASSERT_EQ(res.size(), 0);
    ASSERT_EQ(map.trashSize(), 0);

    /// Insert
    std::vector<int> values{0,1,2,3,4};
    map.insert(values, [](const int& v) { return v; });
    for (auto it = map.begin(); it != map.end(); it++)
        res.push_back(*it);
    expected_res = std::vector<int>{4,3,2,1,0};
    ASSERT_EQ(res, expected_res);
    ASSERT_EQ(res.size(), 5);
    ASSERT_EQ(map.trashSize(), 0);

    /// insert new keys
    std::vector<int> new_values{5,6,7,8};
    map.insert(new_values, [](const int& v) { return v; });
    res.clear();
    for (auto it = map.begin(); it != map.end(); it++)
        res.push_back(*it);
    expected_res = std::vector<int>{8,7,6,5,4,3,2,1,0};
    ASSERT_EQ(res, expected_res);
    ASSERT_EQ(res.size(), 9);
    ASSERT_EQ(map.trashSize(), 0);

    /// insert exist keys
    std::vector<int> exist_values{13, 16};
    map.insert(exist_values, [](const int& v) { return v-10; });
    res.clear();
    for (auto it = map.begin(); it != map.end(); it++)
        res.push_back(*it);
    expected_res = std::vector<int>{8,7,16,5,4,13,2,1,0};
    ASSERT_EQ(res, expected_res); 
    ASSERT_EQ(res.size(), 9);
    ASSERT_EQ(map.trashSize(), 0);

    /// insert new and exist keys
    std::vector<int> mixed_values{11, 12, 3, 14};
    map.insert(mixed_values, [](const int& v) { return v; });

    res.clear();
    for (auto it = map.begin(); it != map.end(); it++)
        res.push_back(*it);
    expected_res = std::vector<int>{14,12,11,8,7,16,5,4,3,2,1,0};
    ASSERT_EQ(res, expected_res);           
    ASSERT_EQ(res.size(), 12);
    ASSERT_EQ(map.trashSize(), 0);

    /// Emplace and find
    {
        auto it = map.find(5);
        ASSERT_TRUE(it != map.end());
        if (it != map.end())
        {
            ASSERT_EQ(*it, 5);
        }

        it = map.find(21);
        ASSERT_TRUE(it == map.end());

        auto p = map.emplace(4, 14);
        ASSERT_FALSE(p.second);
        ASSERT_EQ(*p.first, 4);

        p = map.emplace(15, 25);
        ASSERT_TRUE(p.second);
        ASSERT_EQ(*p.first, 25);
    }
    res.clear();
    for (auto it = map.begin(); it != map.end(); it++)
        res.push_back(*it);
    expected_res = std::vector<int>{25,14,12,11,8,7,16,5,4,3,2,1,0};
    ASSERT_EQ(res, expected_res);           
    ASSERT_EQ(res.size(), 13);
    ASSERT_EQ(map.trashSize(), 0);

    /// Erase
    {
        auto items = map.erase(1);
        ASSERT_EQ(items, 1);
        res.clear();
        for (auto it = map.begin(); it != map.end(); it++)
            res.push_back(*it);
        expected_res = std::vector<int>{25,14,12,11,8,7,16,5,4,3,2,0};
        ASSERT_EQ(res, expected_res);           
        ASSERT_EQ(res.size(), 12);

        items = map.erase(1);
        ASSERT_EQ(items, 0);

        items = map.erase(std::vector<int>{1,2,3});
        ASSERT_EQ(items, 2);
        res.clear();
        for (auto it = map.begin(); it != map.end(); it++)
            res.push_back(*it);
        expected_res = std::vector<int>{25,14,12,11,8,7,16,5,4,0};
        ASSERT_EQ(res, expected_res);           
        ASSERT_EQ(res.size(), 10);
    }
    ASSERT_EQ(map.trashSize(), 0);

    /// Update
    map.update(7, 17);
    map.update(11, 21);
    map.update(30, 30);
    res.clear();
    for (auto it = map.begin(); it != map.end(); it++)
        res.push_back(*it);
    expected_res = std::vector<int>{30,25,14,12,21,8,17,16,5,4,0};
    ASSERT_EQ(res, expected_res);           
    ASSERT_EQ(res.size(), 11);
    ASSERT_EQ(map.trashSize(), 0);

    /// Clear
    map.clear();
    res.clear();
    for (auto it = map.begin(); it != map.end(); it++)
        res.push_back(*it);
    expected_res.clear();
    ASSERT_EQ(res, expected_res);           
    ASSERT_EQ(res.size(), 0);
    ASSERT_EQ(map.trashSize(), 0);
}

void ScanWaitFreeMapAtomic()
{
    TestMap map;
    std::vector<std::thread> threads;
    threads.reserve(2);
    size_t MAX_ROUND = 10000;
    size_t MAX_RECORDS = 100;
    std::mutex mutex;
    std::condition_variable write_ready;
    std::condition_variable read_ready;
    auto writer = [&]()
    {
        for (int round = 0; round < MAX_ROUND; ++round)
        {
            if (round % MAX_RECORDS == 0)
                map.clear();
            std::vector<std::shared_ptr<std::string>> new_values;
            new_values.push_back(std::make_shared<std::string>("partition-" + std::to_string(round)));
            map.insert(new_values, [](const std::shared_ptr<std::string> & v) { return *v; });
            std::unique_lock<std::mutex> lock(mutex);
            write_ready.notify_one();
            read_ready.wait(lock);
        }
    };

    auto reader = [&]()
    {
        {
            std::unique_lock<std::mutex> lock(mutex);
            write_ready.wait(lock);
        }
        for (int round = 0; round < MAX_ROUND; ++round)
        {
            size_t number = 0;
            for (auto it = map.begin(); it != map.end(); ++it) { number++; }
            ASSERT_EQ(round%MAX_RECORDS+1, number);
            std::unique_lock<std::mutex> lock(mutex);
            read_ready.notify_one();
            if (round < MAX_ROUND-1)
                write_ready.wait(lock);
        }
    };

    Stopwatch watch;
    threads.emplace_back(reader);
    threads.emplace_back(writer);
    
    for (auto & thread : threads)
        thread.join();
    double ms = watch.elapsedMilliseconds();
    std::cout << "Check rounds = " << MAX_ROUND << ":\t" << ms << " ms" << std::endl;
}

TEST(ScanWaitFreeMap, Basic) {  ScanWaitFreeMapBasic(); }
TEST(ScanWaitFreeMap, FindOnly) {  ScanWaitFreeMapFindOnly(); }
TEST(ScanWaitFreeMap, WriteOnly) {  ScanWaitFreeMapWriteOnly(); }
TEST(ScanWaitFreeMap, ScanOnly) {  ScanWaitFreeMapScanOnly(); }
TEST(ScanWaitFreeMap, ScanWrite) {  ScanWaitFreeMapScanWrite(); }
TEST(ScanWaitFreeMap, Atomic) { ScanWaitFreeMapAtomic(); }
