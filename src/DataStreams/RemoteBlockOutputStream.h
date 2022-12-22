#pragma once

#include <Core/Block.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Common/Throttler.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>

namespace DB
{

class Connection;
class ReadBuffer;
struct Settings;


/** Allow to execute INSERT query on remote server and send data for it.
  */
class RemoteBlockOutputStream : public IBlockOutputStream
{
public:
    RemoteBlockOutputStream(Connection & connection_,
                            const ConnectionTimeouts & timeouts,
                            const String & query_,
                            const Settings & settings_,
                            const ClientInfo & client_info_,
                            ContextPtr context_);

    Block getHeader() const override { return header; }

    void write(const Block & block) override;
    void writeSuffix() override;

    /// Send pre-serialized and possibly pre-compressed block of data, that will be read from 'input'.
    void writePrepared(ReadBuffer & input, size_t size = 0);

    ~RemoteBlockOutputStream() override;

    void parseQueryWorkerMetrics(const QueryWorkerMetricElements & elements);

private:
    Connection & connection;
    String query;
    Block header;
    bool finished = false;
    ContextPtr context;
};

}
