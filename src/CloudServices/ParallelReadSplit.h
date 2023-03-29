#pragma once

#include <deque>

#include "IO/ReadBuffer.h"
#include "IO/WriteBuffer.h"
#include "Storages/Hive/HiveDataPart_fwd.h"

namespace DB
{

struct IParallelReadSplit
{
    virtual ~IParallelReadSplit() = default;
    IParallelReadSplit() = default;
    IParallelReadSplit(const IParallelReadSplit &) = default;

    virtual void serialize(WriteBuffer & out) const = 0;
    virtual String describe() const = 0;
    virtual void deserialize(ReadBuffer & in) const = 0;
};

struct IParallelReadSplits : public std::deque<IParallelReadSplit>
{
    using std::deque<IParallelReadSplit>::deque;

    void serialize(WriteBuffer & out) const;
    String describe() const;
    void deserialize(ReadBuffer & in);

    void merge(IParallelReadSplits & other);
};

struct HiveParallelReadSplit : public IParallelReadSplit
{
    HiveParallelReadSplit(HiveDataPartCNCHPtr part, size_t start, size_t end);

    HiveDataPartCNCHPtr part;
    size_t start;
    size_t end;

    void serialize(WriteBuffer & out) const override;
    String describe() const override;
    void deserialize(ReadBuffer & in) const override;
};

}
