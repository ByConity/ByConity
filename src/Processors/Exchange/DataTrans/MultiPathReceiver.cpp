#include <Compression/CompressedReadBuffer.h>
#include <Core/Block.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <Processors/Exchange/DataTrans/NativeChunkInputStream.h>
#include <Processors/Exchange/DataTrans/Brpc/AsyncRegisterResult.h>
#include <Processors/Exchange/DataTrans/Brpc/BrpcRemoteBroadcastReceiver.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Processors/Exchange/DataTrans/Local/LocalBroadcastChannel.h>
#include <Processors/Exchange/DataTrans/MultiPathBoundedQueue.h>
#include <Processors/Exchange/DataTrans/MultiPathReceiver.h>
#include <Processors/Exchange/ExchangeUtils.h>
#include <Processors/Exchange/DataTrans/Brpc/ReadBufferFromBrpcBuf.h>
#include <boost/algorithm/string/predicate.hpp>
#include <Poco/Logger.h>
#include <Common/ClickHouseRevision.h>
#include <Common/Exception.h>
#include <common/types.h>
#include <common/logger_useful.h>

#include <atomic>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
}

MultiPathReceiver::MultiPathReceiver(
    MultiPathQueuePtr collector_, BroadcastReceiverPtrs sub_receivers_, Block header_, String name_, bool enable_block_compress_)
    : collector(std::move(collector_))
    , sub_receivers(std::move(sub_receivers_))
    , header(header_)
    , name(std::move(name_))
    , enable_block_compress(enable_block_compress_)
    , logger(&Poco::Logger::get("MultiPathReceiver"))
{
    for (auto & sub_receiver : sub_receivers)
    {
        auto sub_name = sub_receiver->getName();
        if (running_receiver_names.find(sub_name) == running_receiver_names.end())
        {
            running_receiver_names[sub_name] = 1;
        }
        else
        {
            running_receiver_names[sub_name] += 1;
        }
    }
}

MultiPathReceiver::~MultiPathReceiver()
{
    try
    {
        finish(BroadcastStatusCode::RECV_UNKNOWN_ERROR, "MultiPathReceiver destroyed");

        /// Wait all brpc register rpc done
        for (auto & res : async_results)
            brpc::Join(res.cntl->call_id());
    }
    catch (...)
    {
        tryLogCurrentException(logger);
    }

    auto * status = fin_status.load(std::memory_order_acquire);
    if (status && status != &init_fin_status)
        delete status;
}

void MultiPathReceiver::registerToSendersAsync(UInt32 timeout_ms)
{
    bool expected = false;
    if (registering.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire))
    {
        async_results.clear();
        async_results.reserve(sub_receivers.size());

        for (auto & receiver : sub_receivers)
        {
            auto * receiver_ptr = receiver.get();
            auto * brpc_receiver = dynamic_cast<BrpcRemoteBroadcastReceiver *>(receiver_ptr);
            if (brpc_receiver)
                async_results.emplace_back(brpc_receiver->registerToSendersAsync(timeout_ms));
        }

        LOG_DEBUG(logger, fmt::format("{} register to remote sender async", name));
    }
    else
    {
        std::unique_lock lock(wait_register_mutex);
        if (!wait_register_cv.wait_for(lock, std::chrono::milliseconds(timeout_ms + 100), [&] { return inited.load(std::memory_order_acquire); }))
            throw Exception("Wait register timeout for " + name, ErrorCodes::TIMEOUT_EXCEEDED);
    }
}

void MultiPathReceiver::registerToSendersJoin()
{
    /// Wait all brpc register rpc done
    for (auto & res : async_results)
        brpc::Join(res.cntl->call_id());

    /// get result
    for (auto & res : async_results)
    {
        // if exchange_enable_force_remote_mode = 1, sender and receiver in same process and sender stream may close before rpc end
        if (res.cntl->ErrorCode() == brpc::EREQUEST
            && boost::algorithm::ends_with(res.cntl->ErrorText(), "was closed before responded"))
        {
            LOG_INFO(
                logger,
                    "Receiver register sender async but sender already finished, host-{} , data_key: {}_{}_{}_{}",
                    butil::endpoint2str(res.cntl->remote_side()).c_str(),
                    res.request->query_id(),
                    res.request->exchange_id(),
                    res.request->parallel_id(),
                    res.request->coordinator_address());
            continue;
        }
        res.channel->assertController(*res.cntl);
        LOG_TRACE(
            logger,
                "Receiver register sender async successfully, host-{} , data_key: {}_{}_{}_{}",
                butil::endpoint2str(res.cntl->remote_side()).c_str(),
                res.request->query_id(),
                res.request->exchange_id(),
                res.request->parallel_id(),
                res.request->coordinator_address());
    }
    async_results.clear();
    inited.store(true, std::memory_order_release);
    wait_register_cv.notify_all();
    LOG_DEBUG(logger, fmt::format("{} register to sender async successfully", name));
}

void MultiPathReceiver::registerToLocalSenders(UInt32 timeout_ms)
{
    std::vector<LocalBroadcastChannel *> local_receivers;
    for (auto & receiver : sub_receivers)
    {
        auto * receiver_ptr = receiver.get();
        auto * local_receiver = dynamic_cast<LocalBroadcastChannel *>(receiver_ptr);
        if (local_receiver)
        {
            local_receivers.push_back(local_receiver);
        }
    }

    for (auto * local_receiver : local_receivers)
    {
        local_receiver->registerToSenders(timeout_ms);
    }

    LOG_DEBUG(logger, fmt::format("{} register to local sender successfully", name));
}

void MultiPathReceiver::registerToSenders(UInt32 timeout_ms)
{
    bool expected = false;
    if (registering.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire))
    {
        async_results.clear();
        async_results.reserve(sub_receivers.size());
        std::exception_ptr exception;
        try
        {
            std::vector<LocalBroadcastChannel *> local_receivers;
            for (auto & receiver : sub_receivers)
            {
                auto * receiver_ptr = receiver.get();
                auto * local_receiver = dynamic_cast<LocalBroadcastChannel *>(receiver_ptr);
                if (local_receiver)
                {
                    local_receivers.push_back(local_receiver);
                }
                else
                {
                    auto * brpc_receiver = dynamic_cast<BrpcRemoteBroadcastReceiver *>(receiver_ptr);
                    if (unlikely(!brpc_receiver))
                    {
                        throw Exception(
                            "Unexpected SubReceiver Type: " + std::string(typeid(receiver_ptr).name()), ErrorCodes::LOGICAL_ERROR);
                    }
                    async_results.emplace_back(brpc_receiver->registerToSendersAsync(timeout_ms));
                }
            }

            for (auto * local_receiver : local_receivers)
            {
                local_receiver->registerToSenders(timeout_ms);
            }
        }
        catch (...)
        {
            exception = std::current_exception();
        }

        /// Wait all brpc register rpc done
        for (auto & res : async_results)
            brpc::Join(res.cntl->call_id());

        if(exception)
            std::rethrow_exception(std::move(exception));

        /// get result
        for (auto & res : async_results)
        {
            // if exchange_enable_force_remote_mode = 1, sender and receiver in same process and sender stream may close before rpc end
            if (res.cntl->ErrorCode() == brpc::EREQUEST
                && boost::algorithm::ends_with(res.cntl->ErrorText(), "was closed before responded"))
            {
                LOG_INFO(
                    logger,
                        "Receiver register sender successfully but sender already finished, host-{} , data_key: {}_{}_{}_{}",
                        butil::endpoint2str(res.cntl->remote_side()).c_str(),
                        res.request->query_id(),
                        res.request->exchange_id(),
                        res.request->parallel_id(),
                        res.request->coordinator_address());
                continue;
            }
            res.channel->assertController(*res.cntl);
            LOG_TRACE(
                logger,
                    "Receiver register sender successfully, host-{} , data_key: {}_{}_{}_{}",
                    butil::endpoint2str(res.cntl->remote_side()).c_str(),
                    res.request->query_id(),
                    res.request->exchange_id(),
                    res.request->parallel_id(),
                    res.request->coordinator_address());
        }
        inited.store(true, std::memory_order_release);
        wait_register_cv.notify_all();
        LOG_DEBUG(logger, fmt::format("{} register to sender successfully", name));
    }
    else
    {
        std::unique_lock lock(wait_register_mutex);
        if (!wait_register_cv.wait_for(lock, std::chrono::milliseconds(timeout_ms + 100), [&] { return inited.load(std::memory_order_acquire); }))
            throw Exception("Wait register timeout for " + name, ErrorCodes::TIMEOUT_EXCEEDED);
    }
}

RecvDataPacket MultiPathReceiver::recv(UInt32 timeout_ms)
{
    MultiPathDataPacket data_packet;
    if (!collector->tryPop(data_packet, timeout_ms))
    {
        bool collector_closed = collector->closed();
        String error_msg = "Try pop receive collector for " + name;
        error_msg.append(collector_closed ? " interrupted" : " timeout for " + std::to_string(timeout_ms) + " ms.");

        BroadcastStatus current_status
            = finish(collector_closed ? BroadcastStatusCode::RECV_UNKNOWN_ERROR : BroadcastStatusCode::RECV_TIMEOUT, error_msg);
        return current_status;
    }
    if (std::holds_alternative<RawPacket>(data_packet))
    {
        RawPacket io_buf_ptr = std::move(std::get<RawPacket>(data_packet));
        auto read_buffer = std::make_unique<ReadBufferFromBrpcBuf>(*io_buf_ptr);
        std::unique_ptr<ReadBuffer> buf;
        if (enable_block_compress)
            buf = std::make_unique<CompressedReadBuffer>(*read_buffer);
        else
            buf = std::move(read_buffer);
        NativeChunkInputStream chunk_in(*buf, header);
        Chunk chunk = chunk_in.readImpl();
        return RecvDataPacket(std::move(chunk));
    }
    else if (std::holds_alternative<Chunk>(data_packet))
    {
        Chunk receive_chunk = std::move(std::get<Chunk>(data_packet));
        recv_metric.recv_rows << receive_chunk.getNumRows();
        recv_metric.recv_bytes << receive_chunk.bytes();
        recv_metric.recv_counts << 1;
        return RecvDataPacket(std::move(receive_chunk));
    } else {
        SendDoneMark receiver_name = std::get<SendDoneMark>(data_packet);
        bool all_receiver_done = false;
        {
            std::lock_guard lock(running_receiver_mutex);
            if (unlikely(running_receiver_names.empty()))
                throw Exception(name + " receive unexpected sendDoneMark from  " + receiver_name, ErrorCodes::LOGICAL_ERROR);
            else
            {
                if (running_receiver_names.find(receiver_name) != running_receiver_names.end())
                {
                    running_receiver_names[receiver_name] -= 1;
                    if (running_receiver_names[receiver_name] <= 0)
                    {
                        running_receiver_names.erase(receiver_name);
                    }
                }
                all_receiver_done = running_receiver_names.empty();
            }
        }
        if (all_receiver_done)
            return finish(BroadcastStatusCode::ALL_SENDERS_DONE, name + " received all data");
        else
            return recv(timeout_ms);
    }
}

BroadcastStatus MultiPathReceiver::finish(BroadcastStatusCode status_code, String message)
{
    if (!inited.load(std::memory_order_acquire))
    {
        return BroadcastStatus(BroadcastStatusCode::RECV_NOT_READY);
    }

    BroadcastStatus * current_status_ptr = &init_fin_status;
    BroadcastStatus * new_status_ptr = new BroadcastStatus(status_code, false, message);

    if (fin_status.compare_exchange_strong(current_status_ptr, new_status_ptr, std::memory_order_acq_rel, std::memory_order_acquire))
    {
        bool is_modifer = false;
        BroadcastStatus old_status(BroadcastStatusCode::RUNNING);
        for (auto & receiver : sub_receivers)
        {
            auto res = receiver->finish(status_code, message);
            if (res.is_modifer)
                is_modifer = true;
            else
                old_status = std::move(res);
        }
        /// Wakeup all pending receivers;
        collector->close();

        LOG_INFO(
            logger,
            fmt::format(
                "{} change finish status from {} to {} with message: {}, is_modifer: {}, subreceiver status: {}",
                name,
                current_status_ptr->code,
                status_code,
                message,
                is_modifer,
                old_status.code));

        if (is_modifer)
        {
            auto res = *new_status_ptr;
            recv_metric.finish_code.store(new_status_ptr->code, std::memory_order_relaxed);
            res.is_modifer = true;
            return res;
        }
        recv_metric.finish_code.store(old_status.code, std::memory_order_relaxed);
        return old_status;
    }
    else
    {
        delete new_status_ptr;
        LOG_TRACE(
            logger,
            fmt::format(
                "{} finished and can't change to status code {},  msg {} . Current status: {}, msg: {}",
                name,
                status_code,
                message,
                current_status_ptr->code,
                current_status_ptr->message));
        return *current_status_ptr;
    }
}

String MultiPathReceiver::getName() const
{
    return name;
}
}
