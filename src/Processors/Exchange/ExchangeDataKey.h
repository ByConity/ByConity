#pragma once

#include <Processors/Exchange/DataTrans/DataTransKey.h>
#include <fmt/core.h>

namespace DB
{
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
    ~ExchangeDataKey() override = default;

    String getKey() const override
    {
        return query_id + "_" + std::to_string(write_segment_id) + "_" + std::to_string(read_segment_id) + "_"
            + std::to_string(parallel_index) + "@" + coordinator_address;
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

private:
    String query_id;
    size_t write_segment_id;
    size_t read_segment_id;
    size_t parallel_index;
    String coordinator_address;
};
}
