#include <string>
#include <Processors/Exchange/DataTrans/Batch/DiskExchangeDataManager.h>
#include <Processors/Exchange/DataTrans/Batch/Reader/DiskExchangeDataSource.h>
#include <Processors/Exchange/DataTrans/NativeChunkInputStream.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
Chunk DiskExchangeDataSource::generate()
{
    if (!stream)
    {
        /// bufs initializatoin is delayed to generate method
        /// as to avoid any I/O ops within brpc call method
        bufs = context->getDiskExchangeDataManager()->readFiles(*key);
        if (bufs.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, fmt::format("empty files to read {}", *key));
        initStream();
        LOG_DEBUG(getLogger("DiskExchangeDataSource"), "Start to read file {}", bufs[0]->getFileName());
    }
    auto c = stream->readImpl();
    if (!c)
    {
        c = readNextFile();
    }
    return c;
}

void DiskExchangeDataSource::initStream()
{
    compressed_input = std::make_unique<CompressedReadBuffer>(*bufs[idx]);
    stream = std::make_unique<NativeChunkInputStream>(*compressed_input, getOutputs().front().getHeader());
}

Chunk DiskExchangeDataSource::readNextFile()
{
    Chunk res;
    while (idx != bufs.size() - 1 && !res)
    {
        idx++;
        initStream();
        LOG_DEBUG(getLogger("DiskExchangeDataSource"), "Start to read file {}", bufs[idx]->getFileName());
        res = stream->readImpl();
    }

    return res;
}
}
