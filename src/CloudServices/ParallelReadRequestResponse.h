#pragma once

#include "CloudServices/ParallelReadSplit.h"
#include "IO/ReadBuffer.h"
#include "IO/WriteBuffer.h"

namespace DB
{

struct ParallelReadRequest
{
    String worker_id;
    size_t min_weight {0};

    void serialize(WriteBuffer & out) const;
    void deserialize(ReadBuffer & in);
};

struct ParallelReadResponse
{
    bool finish{false};
    ParallelReadSplits split;

    void serialize(WriteBuffer & out) const;
    void deserialize(ReadBuffer & in);
};

}
