#pragma once

#include <Columns/ListIndex.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Processors/Exchange/DataTrans/NativeChunkInputStream.h>
#include <Processors/ISource.h>
#include <common/defines.h>

namespace DB
{

/// Read from given file.
class DiskExchangeDataSource : public ISource
{
public:
    DiskExchangeDataSource(Block header, std::vector<std::unique_ptr<ReadBufferFromFileBase>> bufs_)
        : ISource(std::move(header)), bufs(std::move(bufs_))
    {
        if (bufs.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "empty files to read");
        stream = std::make_unique<NativeChunkInputStream>(*bufs[idx], getOutputs().front().getHeader());
        LOG_DEBUG(&Poco::Logger::get("DiskExchangeDataSource"), "Start to read file {}", bufs[idx]->getFileName());
    }
    Chunk generate() override;
    String getName() const override
    {
        return "DiskExchangeDataSource";
    }

private:
    Chunk readNextFile();

    std::vector<std::unique_ptr<ReadBufferFromFileBase>> bufs;
    size_t idx = 0;
    NativeChunkInputStreamHolder stream;
};
}
