#pragma once

#include <Columns/ListIndex.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Processors/Exchange/DataTrans/NativeChunkInputStream.h>
#include <Processors/Exchange/ExchangeDataKey.h>
#include <Processors/ISource.h>
#include <common/defines.h>

namespace DB
{

/// Read from given file.
class DiskExchangeDataSource : public ISource
{
public:
    DiskExchangeDataSource(Block header, ExchangeDataKeyPtr key_, ContextPtr context_)
        : ISource(std::move(header)), key(key_), context(context_)
    {
    }
    Chunk generate() override;
    String getName() const override
    {
        return "DiskExchangeDataSource";
    }

private:
    Chunk readNextFile();
    void initStream();

    std::vector<std::unique_ptr<ReadBufferFromFileBase>> bufs;
    size_t idx = 0;
    NativeChunkInputStreamHolder stream;
    std::unique_ptr<CompressedReadBuffer> compressed_input;
    ExchangeDataKeyPtr key;
    ContextPtr context;
};
}
