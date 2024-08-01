#pragma once

#include <Core/Types.h>
#include <Common/SipHash.h>

namespace DB
{
/// indicate table level gran
const constexpr auto ALL_DEDUP_GRAN_PARTITION_ID = "ALL_PARTITION";
const constexpr auto FAKE_DEDUP_GRAN_PARTITION_ID = "FAKE_PARTITION";

/// Used to indicate dedup granularity
struct DedupGran
{
    String partition_id;
    Int64 bucket_number;

    DedupGran():
        partition_id(FAKE_DEDUP_GRAN_PARTITION_ID),
        bucket_number(-1) {}

    DedupGran(String partition_id_, Int64 bucket_number_):
        partition_id(partition_id_),
        bucket_number(bucket_number_) {}

    UInt64 hash() const
    {
        String hash_str = partition_id + "." + std::to_string(bucket_number);
        SipHash hash_state;
        hash_state.update(hash_str.data(), hash_str.size());
        return hash_state.get64();
    }

    bool operator==(const DedupGran & other) const
    {
        return partition_id == other.partition_id && bucket_number == other.bucket_number;
    }

    bool empty() const
    {
        return partition_id == FAKE_DEDUP_GRAN_PARTITION_ID;
    }

    String getDedupGranDebugInfo() const
    {
        return "dedup gran(partition_id " + partition_id + ", bucket_number " + std::to_string(bucket_number) + ")";
    }
};

using DedupGranTimeMap = std::unordered_map<DedupGran, UInt64>;
using DedupGranList = std::vector<DedupGran>;
}

namespace std
{
template <>
struct hash<DB::DedupGran>
{
    using argument_type = DB::DedupGran;
    using result_type = size_t;

    result_type operator()(const argument_type & dedup_gran) const
    {
        return dedup_gran.hash();
    }
};
}
