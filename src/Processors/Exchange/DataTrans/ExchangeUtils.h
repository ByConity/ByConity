#pragma once

namespace DB
{
class ExchangeUtils
{
public:
    static inline String generateDataKey(const String & query_id, size_t write_segment_id, size_t read_segment_id, size_t parallel_id)
    {
        return query_id + "_" + std::to_string(write_segment_id) + "_" + std::to_string(read_segment_id) + "_"
            + std::to_string(parallel_id);
    }
};
}
