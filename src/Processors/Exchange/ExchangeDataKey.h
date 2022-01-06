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
        String query_id_, String write_segment_id_, String read_segment_id_, String parallel_index_, String coordinator_address_)
        : query_id(std::move(query_id_))
        , write_segment_id(std::move(write_segment_id_))
        , read_segment_id(std::move(read_segment_id_))
        , parallel_index(std::move(parallel_index_))
        , coordinator_address(std::move(coordinator_address_))
    {
    }

    ExchangeDataKey(
        String query_id_, size_t write_segment_id_, size_t read_segment_id_, size_t parallel_index_, const String & coordinator_address_)
        : query_id(std::move(query_id_))
        , write_segment_id(std::to_string(write_segment_id_))
        , read_segment_id(std::to_string(read_segment_id_))
        , parallel_index(std::to_string(parallel_index_))
        , coordinator_address(coordinator_address_)
    {
    }
    ~ExchangeDataKey() override = default;

    String getKey() const override
    {
        return query_id + "_" + write_segment_id + "_" + read_segment_id + "_" + parallel_index + "_" + coordinator_address;
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
    String write_segment_id;
    String read_segment_id;
    String parallel_index;
    String coordinator_address;
};
}
