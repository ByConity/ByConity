#pragma once

#include "CloudServices/ParallelReadSplit.h"
#include "IO/ReadBuffer.h"
#include "IO/WriteBuffer.h"

namespace DB
{

enum class CoordinationMode
{
    Default
};

struct ParallelReadRequest
{
    CoordinationMode mode;
    size_t replica_num;
    size_t min_number_of_slice;

    void serialize(WriteBuffer & out) const;
    String describe() const;
    void deserialize(ReadBuffer & in);
    void merge(ParallelReadRequest & other);
};

struct ParallelReadResponse
{
    bool finish{false};
    IParallelReadSplits split;

    void serialize(WriteBuffer & out) const;
    String describe() const;
    void deserialize(ReadBuffer & in);
};

}
