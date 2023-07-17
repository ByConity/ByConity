#pragma once

#include <memory>
#include <mutex>

#include <Core/Protocol.h>
#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>
#include <DataStreams/BlockIO.h>
#include <IO/Progress.h>
#include <IO/TimeoutSetter.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Storages/ColumnsDescription.h>
#include <Common/Stopwatch.h>

#include <IO/EmptyReadBuffer.h>
#include <IO/NullWriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context_fwd.h>
#include <Server/IServer.h>

namespace DB
{

/// State of query processing.
struct QueryState
{
    /// Identifier of the query.
    String query_id;

    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete;
    Protocol::Compression compression = Protocol::Compression::Disable;

    /// A queue with internal logs that will be passed to client. It must be
    /// destroyed after input/output blocks, because they may contain other
    /// threads that use this queue.
    InternalTextLogsQueuePtr logs_queue;
    BlockOutputStreamPtr logs_block_out;

    /// From where to read data for INSERT.
    std::shared_ptr<ReadBuffer> maybe_compressed_in;
    BlockInputStreamPtr block_in;

    /// Where to write result data.
    std::shared_ptr<WriteBuffer> maybe_compressed_out;
    BlockOutputStreamPtr block_out;

    /// Query text.
    String query;
    /// PlanSegment, unique pointer, life span is only held in query stage.
    PlanSegmentPtr plan_segment;
    /// Streams of blocks, that are processing the query.
    BlockIO io;

    /// Is request cancelled
    bool is_cancelled = false;
    bool is_connection_closed = false;
    /// empty or not
    bool is_empty = true;
    /// Data was sent.
    bool sent_all_data = false;
    /// Request requires data from the client (INSERT, but not INSERT SELECT).
    bool need_receive_data_for_insert = false;
    /// Temporary tables read
    bool temporary_tables_read = false;

    /// A state got uuids to exclude from a query
    bool part_uuids = false;

    /// Request requires data from client for function input()
    bool need_receive_data_for_input = false;
    /// temporary place for incoming data block for input()
    Block block_for_input;
    /// sample block from StorageInput
    Block input_header;

    /// To output progress, the difference after the previous sending of progress.
    Progress progress;

    /// Timeouts setter for current query
    std::unique_ptr<TimeoutSetter> timeout_setter;

    void reset()
    {
        *this = QueryState();
    }

    bool empty() const
    {
        return is_empty;
    }
};

struct LastBlockInputParameters
{
    Protocol::Compression compression = Protocol::Compression::Disable;
    Block header;
};

using QueryStatePtr = std::shared_ptr<QueryState>;

struct ClientVersionInfo
{
    String client_name;
    UInt64 client_version_major = 0;
    UInt64 client_version_minor = 0;
    UInt64 client_version_patch = 0;
    UInt64 client_tcp_protocol_version = 0;
    UInt64 client_plan_segment_version = 0;
};

struct InterServerInfo
{
    /// For inter-server secret (remote_server.*.secret)
    String salt;
    String cluster;
    String cluster_secret;
};

class TCPQuery
{
private:
    IServer & server;
    // Only for setting timeouts.
    Poco::Net::StreamSocket & socket;
    Poco::Logger * log;
    ClientVersionInfo client_info;
    InterServerInfo server_info;
    size_t unknown_packet_in_send_data = 0;

    /// Streams for reading/writing from/to client connection socket.
    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    ContextMutablePtr query_context;

    /// At the moment, only one ongoing query in the connection is supported at a time.
    QueryState state = QueryState();
    /// May initialized in different thread.
    std::optional<CurrentThread::QueryScope> query_scope;

    // std::atomic<bool> data{false};

    /// Last block input parameters are saved to be able to receive unexpected data packet sent after exception.
    LastBlockInputParameters last_block_in;

    /// Time after the last check to stop the request and send the progress.
    Stopwatch after_check_cancelled;
    Stopwatch after_send_progress;

    std::mutex task_callback_mutex;

    size_t poll_interval;
    int receive_timeout;

    std::tuple<size_t, int> getReadTimeouts(const Settings & connection_settings);

    String receiveReadTaskResponseAssumeLocked();

    bool receivePacket();
    void receiveQuery();
    void receiveCnchQuery();
    void receivePlanSegment();
    void receiveIgnoredPartUUIDs();
    bool receiveData(bool scalar);
    void readData(const Settings & connection_settings);
    bool readDataNext();

    [[noreturn]] void receiveUnexpectedData();
    [[noreturn]] void receiveUnexpectedQuery();
    [[noreturn]] void receiveUnexpectedHello();
    [[noreturn]] void receiveUnexpectedTablesStatusRequest();

    /// Process INSERT query
    void processInsertQuery(const Settings & connection_settings);

    /// Process a request that does not require the receiving of data blocks from the client
    void processOrdinaryQuery();

    void processOrdinaryQueryWithProcessors();

    void processTablesStatusRequest();

    void sendData(const Block & block); /// Write a block to the network.
    void sendPartUUIDs();
    void sendReadTaskRequestAssumeLocked();
    void sendTableColumns(const ColumnsDescription & columns);
    void sendProgress();

    void sendProfileInfo(const BlockStreamProfileInfo & info);
    void sendTotals(const Block & totals);
    void sendExtremes(const Block & extremes);

    /// Creates state.block_in/block_out for blocks read/write, depending on whether compression is enabled.
    void initBlockInput();
    void initBlockOutput(const Block & block);
    void initLogsBlockOutput(const Block & block);

public:
    TCPQuery(
        IServer & server_,
        Poco::Net::StreamSocket & socket_,
        ClientVersionInfo client_info_,
        InterServerInfo server_info_,
        std::shared_ptr<ReadBuffer> in_,
        std::shared_ptr<WriteBuffer> out_,
        ContextMutablePtr conn_context_);
    virtual ~TCPQuery();

    QueryState & getState()
    {
        return state;
    }
    ContextMutablePtr getContext()
    {
        return query_context;
    }
    void resetIOBuffer()
    {
        state.block_in.reset();
        state.maybe_compressed_in.reset();
        in = std::make_shared<EmptyReadBuffer>();

        state.block_out.reset();
        state.maybe_compressed_out.reset();
        state.logs_block_out.reset();
        out = std::make_shared<NullWriteBuffer>();
    }

    bool init();
    void prepare();
    bool run();

    void updateQueryScope()
    {
        query_scope.emplace(query_context);
    }

    void sendLogs();
    void sendLogData(const Block & block);
    void sendQueryWorkerMetrics();

    void sendAsyncQueryId(const String & id);
    time_t getReceiveTimeout() const
    {
        return receive_timeout;
    }

    /// This method is called right before the query execution.
    virtual void customizeContext(ContextMutablePtr /*context*/)
    {
    }

    bool isQueryCancelled();

    /// This function is called from different threads.
    void updateProgress(const Progress & value);
};

} // namespace DB
