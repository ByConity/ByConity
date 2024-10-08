#include <Common/StorageElection/ElectionReader.h>
#include <Common/StorageElection/KvStorage.h>
#include <Common/StorageElection/StorageElector.h>

#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <common/logger_useful.h>

const static int test_nodes = 3;

static LoggerPtr getLogger()
{
    return getLogger("memstore_elector");
}

class MemStorage : public DB::IKvStorage
{
    mutable std::mutex lock;
    std::map<std::string, std::string> key_value;
    const static int max_sleep_ms = 256;
    const static int timeout_ms = 200;
    uint64_t version = 1;

public:
    virtual void put(const String & key, const String & value, bool if_not_exists) override
    {
        int sleep_time = random() % max_sleep_ms;
        usleep(sleep_time * 1000);
        if (sleep_time >= timeout_ms && sleep_time % 2 == 0)
        {
            throw sleep_time;
        }
        std::unique_lock<std::mutex> lk(lock);
        if (!if_not_exists || key_value.find(key) == key_value.end())
        {
            key_value[key] = value;
        }
        if (sleep_time >= timeout_ms && sleep_time % 2 == 1)
        {
            throw sleep_time;
        }
    }

    virtual std::pair<bool, String>
    putCAS(const String & key, const String & value, const String & expected, bool /*with_old_value*/) override
    {
        int sleep_time = random() % max_sleep_ms;
        usleep(sleep_time * 1000);
        if (sleep_time >= timeout_ms && sleep_time % 2 == 0)
        {
            throw sleep_time;
        }
        std::unique_lock<std::mutex> lk(lock);
        auto it = key_value.find(key);
        if (it == key_value.end())
        {
            return std::make_pair(false, std::string());
        }
        if (it->second != expected)
        {
            return std::make_pair(false, it->second);
        }
        it->second = value;
        if (sleep_time >= timeout_ms && sleep_time % 2 == 1)
        {
            throw sleep_time;
        }
        return std::make_pair(true, std::string());
    }

    virtual uint64_t get(const String & key, String & value) override
    {
        int sleep_time = random() % max_sleep_ms;
        usleep(sleep_time * 1000);
        std::unique_lock<std::mutex> lk(lock);
        auto it = key_value.find(key);
        if (it == key_value.end())
        {
            return 0;
        }
        value = it->second;
        return ++version;
    }
};


int main(int argc, char ** argv)
{
    int sleep_time = 30;
    if (argc > 1)
        sleep_time = atoi(argv[1]);

    Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter("%Y.%m.%d %H:%M:%S.%F %I <%p> %s: %t"));
    Poco::AutoPtr<Poco::ConsoleChannel> console_chanel(new Poco::ConsoleChannel);
    Poco::AutoPtr<Poco::FormattingChannel> channel(new Poco::FormattingChannel(formatter, console_chanel));
    Poco::Logger::root().setLevel("trace");
    Poco::Logger::root().setChannel(channel);

    auto store = std::make_shared<MemStorage>();
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    long ms_start = tv.tv_sec * 1000 + tv.tv_usec / 1000;
    std::thread test_threads[test_nodes];
    for (auto & th : test_threads)
    {
        th = std::thread([=] {
            auto tid = pthread_self();
            DB::HostWithPorts host_info(std::to_string(tid), 0, 0, 0, 0, 0, std::to_string(tid));
            auto elector = std::make_shared<DB::StorageElector>(store,
                10,
                40,
                host_info,
                "test",
                [=](const DB::HostWithPorts *) {
                    struct timeval tv1;
                    gettimeofday(&tv1, nullptr);
                    long now = tv1.tv_sec * 1000 + tv1.tv_usec / 1000 - ms_start;
                    LOG_INFO(getLogger(), "{}: {} BECOMES the leader", now, tid);
                    std::cout << now << " server: " << tid << " become leader" << std::endl;
                    return true;
                },
                [=](const DB::HostWithPorts *) {
                    struct timeval tv1;
                    gettimeofday(&tv1, nullptr);
                    long now = tv1.tv_sec * 1000 + tv1.tv_usec / 1000 - ms_start;
                    LOG_INFO(getLogger(), "{}: {} BECOMES the follower", now, tid);
                    std::cout << now << " server: " << tid << " become follwer" << std::endl;
                    return true;
                });
            std::cout << tid << " started at " << ms_start << std::endl;
            sleep(sleep_time);
            elector.reset();
            std::cout << tid << " stopped " << std::endl;
        });
    }
    std::thread watcher_thread([=]() mutable {
        DB::ElectionReader reader(store, "test");
        while (sleep_time--)
        {
            reader.refresh();
            auto result = reader.tryGetLeaderInfo();
            struct timeval tv1;
            gettimeofday(&tv1, nullptr);
            long now = tv1.tv_sec * 1000 + tv1.tv_usec / 1000 - ms_start;
            if (!result)
                std::cout << now << " client: nobody is the leader" << std::endl;
            else
                std::cout << now << " client: " << result->id << " is the leader" << std::endl;
            sleep(1);
        }
    });

    for (auto & th : test_threads)
    {
        th.join();
    }
    watcher_thread.join();
    return 0;
}
