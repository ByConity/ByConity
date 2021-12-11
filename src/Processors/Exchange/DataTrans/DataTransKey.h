#pragma once

#include <common/types.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/BoundedDataQueue.h>

namespace DB
{
class DataTransKey
{
public:
    virtual String getKey() const = 0;
    virtual String dump() const = 0;
    
    // FIXME: to delete
    virtual String getCoordinatorAddress() const = 0;

    virtual ~DataTransKey() = default;
};

using DataTransKeyPtr = std::shared_ptr<DataTransKey>;
}

