#pragma once

#include <deque>
#include <type_traits>

#include "IO/ReadBuffer.h"
#include "IO/ReadBufferFromString.h"
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
    virtual void deserialize(ReadBuffer & in) = 0;
    virtual String describe() const = 0;
};

struct ParallelReadSplits : public IParallelReadSplit
{
    void serialize(WriteBuffer & out) const override;
    void deserialize(ReadBuffer & in) override;
    String describe() const override;

    void add(const IParallelReadSplit & split);
    size_t size() { return raw.size(); }

    const std::vector<String> & getData() { return raw; }

    template<typename T>
    std::vector<T> converTo() const
    {
        static_assert(std::is_base_of<IParallelReadSplit, T>::value);
        std::vector<T> res;
        res.reserve(raw.size());
        for (const auto & data : raw)
        {
            T t;
            ReadBufferFromString buf(data);
            t.deserialize(buf);
            res.emplace_back(std::move(t));
        }

        return res;
    }

private:
    std::vector<String> raw;
};

struct HiveParallelReadSplit : public IParallelReadSplit
{
    HiveParallelReadSplit() = default;
    HiveParallelReadSplit(HiveDataPartCNCHPtr part, size_t start, size_t end);

    HiveDataPartCNCHPtr part;
    size_t start;
    size_t end;

    void serialize(WriteBuffer & out) const override;
    void deserialize(ReadBuffer & in) override;
    String describe() const override;
};

}
