#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <thread>
#include <utility>

#include <cstdlib>

#include <signal.h>
#include <sys/types.h>

#include <brpc/channel.h>
#include <bthread/bthread.h>
#include <bvar/latency_recorder.h>

#include <gflags/gflags.h>

#include <TSO/TSOClient.h>

using namespace bthread;

// Client concurrent config
DEFINE_uint32(begin_clients, 0, "the clients start, use clients if zero");
DEFINE_uint32(clients, 1, "clients");
DEFINE_uint32(clients_step, 10, "thread step");
DEFINE_uint64(clients_step_sleep_seconds, 3, "sleep seconds in each step group");
DEFINE_uint64(clients_per_step_sleep_seconds, 1, "sleep seconds in each step");

// Client operation count
DEFINE_uint64(operation, 0xFFFFFF, "operation execute count");

// Brpc config
DEFINE_string(address, "127.0.0.1:9490", "all kv server's address");
DEFINE_uint64(connection_timeout, 300, "read timeout");

// Reporter config
DEFINE_int64(report_intervals, 10, "report interval seconds");
DEFINE_bool(enable_output_group, true, "wrap output with Begin End");

#define ASSERT(v) \
    if (!(v)) \
    { \
        std::cerr << __PRETTY_FUNCTION__ << ":" << __LINE__ << " assert 0" << std::endl; \
        abort(); \
    }

namespace
{
inline uint64_t NowUs()
{
    auto duration = std::chrono::steady_clock::now().time_since_epoch();

    return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
}
}

std::atomic<bool> quit{false};
std::atomic<uint64_t> clients{0};

class Histogram
{
public:
    Histogram(std::string name)
        : name_(std::move(name)), report_intervals_(FLAGS_report_intervals), previous_count_(0), latency_recorder_(report_intervals_)
    {
    }
    void Measure(uint64_t latency_us) { latency_recorder_ << latency_us; }

    std::string Summary()
    {
        std::stringstream stream;
        auto count = latency_recorder_.count();

        stream << "[" << name_ << "] "
               << "Clients: " << clients << ", "
               << "Takes(s): " << FLAGS_report_intervals << ", "
               << "Count: " << count - previous_count_ << ", "
               << "OPS: " << latency_recorder_.qps(report_intervals_) << ", "
               << "Avg(us): " << latency_recorder_.latency_percentile(0.5) << ", "
               << "P95(us): " << latency_recorder_.latency_percentile(0.95) << ", "
               << "P99(us): " << latency_recorder_.latency_percentile(0.99) << ", "
               << "Max(us): " << latency_recorder_.max_latency();

        previous_count_ = count;

        return stream.str();
    }

private:
    std::string name_;
    int64_t report_intervals_; // seconds
    int64_t previous_count_;
    bvar::LatencyRecorder latency_recorder_;
};

class ReportUtil
{
public:
    ReportUtil(std::string name) : exited_(false), histogram_(std::move(name))
    {
        reporter_ = std::thread([this]() {
            while (!exited_)
            {
                bthread_usleep(FLAGS_report_intervals * 1000 * 1000);
                report();
            }
        });
    }

    ~ReportUtil()
    {
        exited_ = true;
        reporter_.join();

        report();
    }

    void Measure(uint64_t latency_us) { histogram_.Measure(latency_us); }

private:
    void report()
    {
        std::string summary = histogram_.Summary();
        if (summary.empty())
        {
            return;
        }

        if (FLAGS_enable_output_group)
        {
            std::cout << "------- Begin report -------" << std::endl;
        }
        std::cout << summary << std::endl;
        if (FLAGS_enable_output_group)
        {
            std::cout << "------- End report -------" << std::endl;
        }
    }

    std::atomic<bool> exited_;
    std::thread reporter_;
    Histogram histogram_;
};

class TSOClientWrapper
{
public:
    TSOClientWrapper() : client_(FLAGS_address) { }

    void Run() { client_.getTimestamp(); }

private:
    DB::TSO::TSOClient client_;
};


class Worker
{
public:
    Worker(const std::atomic<bool> & fence, uint64_t count, std::shared_ptr<ReportUtil> reporter)
        : fence_(fence), count_(count), reporter_(reporter), client_()
    {
    }

    void Run()
    {
        while (!fence_)
        {
            bthread_usleep(1000000);
        }

        for (uint64_t i = 0; i < count_; ++i)
        {
            if (quit.load(std::memory_order_relaxed))
            {
                break;
            }

            uint64_t start_us = NowUs();
            client_.Run();
            uint64_t end_us = NowUs();
            reporter_->Measure(end_us - start_us);
        }
    }

private:
    const std::atomic<bool> & fence_;
    uint64_t count_;
    std::shared_ptr<ReportUtil> reporter_;
    TSOClientWrapper client_;
};

void handle_signal(int signum)
{
    /* in case we registered this handler for multiple signals */
    if (signum == SIGINT)
    {
        quit = true;
    }
}

int main(int argc, char ** argv)
{
    signal(SIGINT, handle_signal);

    gflags::ParseCommandLineFlags(&argc, &argv, true);

    bthread_list_t thread_list;
    bthread_list_init(&thread_list, FLAGS_clients, FLAGS_clients);

    std::shared_ptr<ReportUtil> reporter = std::make_shared<ReportUtil>("GetTimestamp");

    if (FLAGS_begin_clients == 0)
    {
        FLAGS_begin_clients = FLAGS_clients;
    }

    std::atomic<bool> fence{false};
    std::cout << "\nbefore create " << FLAGS_begin_clients << "\n" << std::endl;
    auto creator = [reporter, &fence](bthread_list_t * thread_list) {
        auto worker = new Worker(fence, FLAGS_operation, reporter);
        bthread_t thread_id;
        auto executor = [](void * args) -> void * {
            auto worker = reinterpret_cast<Worker *>(args);
            worker->Run();
            delete worker;
            return nullptr;
        };

        int res = bthread_start_background(&thread_id, nullptr, executor, worker);
        ASSERT(res == 0);
        res = bthread_list_add(thread_list, thread_id);
        ASSERT(res == 0);

        ++clients;
    };

    for (size_t i = 0; i < FLAGS_begin_clients; ++i)
    {
        creator(&thread_list);
    }

    std::cout << "\nrun with begin clients " << FLAGS_begin_clients << "\n" << std::endl;

    fence = true;

    for (size_t i = FLAGS_begin_clients; i < FLAGS_clients; i += FLAGS_clients_step)
    {
        sleep(FLAGS_clients_step_sleep_seconds);

        if (quit.load(std::memory_order_relaxed))
        {
            break;
        }

        for (size_t j = 0; j < FLAGS_clients_step; ++j)
        {
            if (quit.load(std::memory_order_relaxed))
            {
                break;
            }
            creator(&thread_list);
            sleep(FLAGS_clients_per_step_sleep_seconds);
        }
    }

    bthread_list_join(&thread_list);
    bthread_list_destroy(&thread_list);

    return 0;
}
