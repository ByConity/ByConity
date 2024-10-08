#pragma once

#include <Common/Logger.h>
#include <cstddef>
#include <memory>
#include <DataStreams/RemoteBlockInputStream.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <Storages/DistributedDataClient.h>
#include <brpc/channel.h>
#include <Poco/Net/Socket.h>
#include "Core/Defines.h"

namespace DB
{

class ReadBufferFromRpcStreamFile : public ReadBufferFromFileBase
{
public:
    explicit ReadBufferFromRpcStreamFile(
        const std::shared_ptr<DistributedDataClient> & client,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory_ = nullptr,
        size_t alignment_ = 0);

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override { return current_file_offset - available(); }
    std::string getFileName() const override { return client->remote_file_path; }
    size_t getFileSize() override { return client->remote_file_size; }

private:
    std::shared_ptr<DistributedDataClient> client;
    size_t current_file_offset{0};
    LoggerPtr log = getLogger("ReadBufferFromRpcStreamFile");
};

}
