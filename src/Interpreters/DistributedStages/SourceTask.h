#pragma once

#include <map>
#include <set>
#include <string>
#include <Protos/plan_segment_manager.pb.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <fmt/core.h>
#include <common/types.h>

namespace DB
{

struct SourceTaskPayload
{
    std::set<Int64> buckets;
    size_t rows = 0;
    size_t part_num = 0;
    String toString() const
    {
        return fmt::format(
            "SourceTaskPayload(buckets:[{}],rows:{},part_num:{})",
            boost::join(buckets | boost::adaptors::transformed([](Int64 b) { return std::to_string(b); }), ","),
            rows,
            part_num);
    }
};

struct SourceTaskPayloadOnWorker
{
    String worker_id;
    size_t rows = 0;
    size_t part_num = 0;
    /// Bucket group is the minimum granularity to schedule source task, below is an example constructing bucket groups
    /// suppose we have two tables t1 and t2. t1's max bucket number is 4, t2's max bucket number is 8.
    /// t1 has buckets 0, 1, 2, 3 and t2 has buckets 0,1,2,3,4,5,6,7,8
    /// then we have bucket groups {0, 4}, {1, 5}, {2, 6}, {3, 7}
    std::map<Int64, std::set<Int64>> bucket_groups;
};

struct SourceTaskFilter
{
    std::optional<size_t> index;
    std::optional<size_t> count;
    std::optional<std::set<Int64>> buckets;
    bool isValid() const
    {
        return (index && count) || buckets;
    }
    Protos::SourceTaskFilter toProto() const;
    void fromProto(const Protos::SourceTaskFilter & proto);
    String toString() const
    {
        if (index && count)
            return fmt::format("SourceTaskFilter(idx:{},cnt:{})", *index, *count);
        else if (buckets)
            return fmt::format(
                "SourceTaskFilter(buckets:{})",
                boost::join(*buckets | boost::adaptors::transformed([](const auto & b) { return std::to_string(b); }), ","));
        return "SourceTaskFilter(invalid)";
    }
};

} // namespace DB
