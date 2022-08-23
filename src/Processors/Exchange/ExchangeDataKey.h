#pragma once

#include <string>
#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <fmt/core.h>

namespace DB
{
class ExchangeDataKey : public DataTransKey
{
public:
    ExchangeDataKey(
        String query_id_, UInt64 write_segment_id_, UInt64 read_segment_id_, UInt64 parallel_index_, String coordinator_address_)
        : query_id(std::move(query_id_))
        , write_segment_id(write_segment_id_)
        , read_segment_id(read_segment_id_)
        , parallel_index(parallel_index_)
        , coordinator_address(std::move(coordinator_address_))
    {
    }

    ~ExchangeDataKey() override = default;

    String getKey() const override
    {
        return query_id + "_" + std::to_string(write_segment_id) + "_" + std::to_string(read_segment_id) + "_"
            + std::to_string(parallel_index) + "_" + coordinator_address;
    }

    String dump() const override
    {
        return fmt::format(
            "ExchangeDataKey: [query_id: {}, write_segment_id: {}, read_segment_id: {}, parallel_index: {}, coordinator_address: {}]",
            query_id,
            write_segment_id,
            read_segment_id,
            parallel_index,
            coordinator_address);
    }

    inline const String & getQueryId() const { return query_id; }

    inline const String & getCoordinatorAddress() const { return coordinator_address; }

    inline UInt64 getWriteSegmentId() const {return write_segment_id;}

    inline UInt64 getReadSegmentId() const {return read_segment_id;}

    inline UInt64 getParallelIndex() const {return parallel_index;}

private:
    String query_id;
    UInt64 write_segment_id;
    UInt64 read_segment_id;
    UInt64 parallel_index;
    String coordinator_address;
};
}
