#pragma once

#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/BoundedDataQueue.h>
#include <butil/iobuf.h>

namespace DB
{
class DataTransKey
{
public:
    virtual String getKey() const = 0;
    virtual String dump() const = 0;
    virtual String getCoordinatorAddress() const = 0;
    virtual ~DataTransKey() = default;
};

using DataTransKeyPtr = std::shared_ptr<DataTransKey>;

class ExchangeDataKey : public DataTransKey
{
public:
    ExchangeDataKey(
        const String & query_id_,
        size_t write_segment_id_,
        size_t read_segment_id_,
        size_t parallel_index_,
        const String & coordinator_address_)
        : query_id(query_id_)
        , write_segment_id(write_segment_id_)
        , read_segment_id(read_segment_id_)
        , parallel_index(parallel_index_)
        , coordinator_address(coordinator_address_)
    {
    }

    String getKey() const override
    {
        return query_id + "_" + std::to_string(write_segment_id) + "_" + std::to_string(read_segment_id) + "_"
            + std::to_string(parallel_index);
    }

    String getCoordinatorAddress() const override { return coordinator_address; }

    String dump() const override
    {
        return "query_id:" + query_id + ", write_segment_id:" + std::to_string(write_segment_id)
            + ", read_segment_id:" + std::to_string(read_segment_id) + ", parallel_index:" + std::to_string(parallel_index);
    }

private:
    String query_id;
    size_t write_segment_id;
    size_t read_segment_id;
    size_t parallel_index;
    String coordinator_address;
};

using ExchangeDataKeyPtr = std::shared_ptr<ExchangeDataKey>;
}
