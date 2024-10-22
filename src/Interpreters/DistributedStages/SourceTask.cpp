#include <Interpreters/DistributedStages/SourceTask.h>
#include <Protos/RPCHelpers.h>
#include <Protos/plan_segment_manager.pb.h>
#include <common/types.h>


namespace DB
{

Protos::SourceTaskFilter SourceTaskFilter::toProto() const
{
    Protos::SourceTaskFilter proto;
    if (index && count)
    {
        proto.set_index(index.value());
        proto.set_count(count.value());
    }
    else if (buckets)
    {
        for (const auto & b : buckets.value())
        {
            proto.add_buckets(b);
        }
    }
    return proto;
}

void SourceTaskFilter::fromProto(const Protos::SourceTaskFilter & proto)
{
    if (proto.has_count() && proto.has_index())
    {
        index = proto.index();
        count = proto.count();
    }
    else
    {
        buckets = std::set<Int64>();
        for (const auto b : proto.buckets())
        {
            buckets->insert(b);
        }
    }
}

Protos::SourceTaskStat SourceTaskStat::toProto() const
{
    Protos::SourceTaskStat proto;
    storage_id.toProto(*proto.mutable_storage_id());
    proto.set_rows(rows);

    return proto;
}

SourceTaskStat SourceTaskStat::fromProto(const Protos::SourceTaskStat & proto)
{
    return {RPCHelpers::createStorageID(proto.storage_id()), proto.rows()};
}

} // namespace DB
