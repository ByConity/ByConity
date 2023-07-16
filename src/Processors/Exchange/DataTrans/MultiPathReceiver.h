#pragma once

#include <Processors/Exchange/DataTrans/BoundedDataQueue.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/IBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/MultiPathBoundedQueue.h>
#include <boost/core/noncopyable.hpp>
#include <bthread/mutex.h>
#include <butil/iobuf.h>
#include <Poco/Logger.h>
#include <Core/Types.h>

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
namespace DB
{
struct MultiPathReceiverMetric
{
    bvar::Adder<UInt32> recv_rows;
    bvar::Adder<UInt64> recv_bytes;
    bvar::Adder<UInt64> recv_io_bytes;
    bvar::Adder<UInt32> recv_counts;
    bvar::Adder<UInt32> dser_time_ms;
    std::atomic_int finish_code{0};
};

class MultiPathReceiver final : public IBroadcastReceiver, private boost::noncopyable
{
public:
    explicit MultiPathReceiver(MultiPathQueuePtr collector_, BroadcastReceiverPtrs sub_receivers_, Block header_, String name_, bool enable_block_compress_);
    ~MultiPathReceiver() override;
    void registerToSenders(UInt32 timeout_ms) override;

    void registerToLocalSenders(UInt32 timeout_ms);
    void registerToSendersAsync(UInt32 timeout_ms);
    void registerToSendersJoin();

    RecvDataPacket recv(UInt32 timeout_ms) override;
    BroadcastStatus finish(BroadcastStatusCode status_code, String message) override;
    String getName() const override;

    static String generateName(
        size_t exchange_id, size_t write_segment_id, size_t read_segment_id, String& co_host_port)
    {
        return fmt::format(
            "MultiPathReceiver[{}_{}_{}_{}_{}]",
            write_segment_id,
            read_segment_id,
            0, // parallel_index
            exchange_id,
            co_host_port
        );
    }

private:
    std::atomic_bool registering{false};
    std::atomic_bool inited{false};

    BroadcastStatus init_fin_status{BroadcastStatusCode::RUNNING, false, "init"};
    std::atomic<BroadcastStatus *> fin_status {&init_fin_status};

    std::vector<AsyncRegisterResult> async_results;

    mutable bthread::Mutex running_receiver_mutex;
    mutable bthread::Mutex wait_register_mutex;
    bthread::ConditionVariable wait_register_cv;
    std::map<String, size_t> running_receiver_names;
    MultiPathQueuePtr collector;
    BroadcastReceiverPtrs sub_receivers;
    Block header;
    String name;
    bool enable_block_compress;
    Poco::Logger * logger;
    MultiPathReceiverMetric recv_metric;
};

}
