#include <Processors/Exchange/DataTrans/Batch/Reader/DiskExchangeDataSource.h>
#include <Processors/Exchange/DataTrans/NativeChunkInputStream.h>
#include "Common/Exception.h"

namespace DB
{
Chunk DiskExchangeDataSource::generate()
{
    return stream->readImpl();
}
}
