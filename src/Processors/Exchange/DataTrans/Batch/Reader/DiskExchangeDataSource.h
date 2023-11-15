#pragma once

#include <Columns/ListIndex.h>
#include <IO/ReadBufferFromFile.h>
#include <Processors/Exchange/DataTrans/NativeChunkInputStream.h>
#include <Processors/ISource.h>
#include <common/defines.h>
#include "IO/ReadBufferFromFileBase.h"

namespace DB
{

/// Read from given file.
class DiskExchangeDataSource : public ISource
{
public:
    DiskExchangeDataSource(Block header, std::unique_ptr<ReadBufferFromFileBase> buf_) : ISource(std::move(header)), buf(std::move(buf_))
    {
        stream = std::make_unique<NativeChunkInputStream>(*buf, getOutputs().front().getHeader());
    }
    String getFileName() const
    {
        chassert(buf);
        return buf->getFileName();
    }
    Chunk generate() override;
    String getName() const override
    {
        return "DiskExchangeDataSource";
    }

private:
    std::unique_ptr<ReadBufferFromFileBase> buf;
    NativeChunkInputStreamHolder stream;
};
}
