#pragma once

#include <vector>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/BoundedDataQueue.h>
#include <common/types.h>

namespace DB
{
class DataTransKey
{
public:
    virtual String getKey() const = 0;
    virtual String dump() const = 0;
    virtual ~DataTransKey() = default;
};

using DataTransKeyPtr = std::shared_ptr<DataTransKey>;
using DataTransKeyPtrs = std::vector<DataTransKeyPtr>;
}
