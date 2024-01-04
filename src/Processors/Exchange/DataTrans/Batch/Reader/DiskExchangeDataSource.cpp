#include <string>
#include <Processors/Exchange/DataTrans/Batch/Reader/DiskExchangeDataSource.h>
#include <Processors/Exchange/DataTrans/NativeChunkInputStream.h>
#include "Common/Exception.h"

namespace DB
{
Chunk DiskExchangeDataSource::generate()
{
    auto c = stream->readImpl();
    if (!c)
    {
        c = readNextFile();
    }
    return c;
}

Chunk DiskExchangeDataSource::readNextFile()
{
    Chunk res;
    while (idx != bufs.size() - 1 && !res)
    {
        idx++;
        stream = std::make_unique<NativeChunkInputStream>(*bufs[idx], getOutputs().front().getHeader());
        LOG_DEBUG(&Poco::Logger::get("DiskExchangeDataSource"), "Start to read file {}", bufs[idx]->getFileName());
        res = stream->readImpl();
    }

    return res;
}
}
