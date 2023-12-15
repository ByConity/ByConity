#pragma once


#include <brpc/stream.h>
#include "common/types.h"
namespace DB
{
    bool brpcWriteWithRetry(brpc::StreamId id, const butil::IOBuf &buf, int retry_count, const String & message);
}
