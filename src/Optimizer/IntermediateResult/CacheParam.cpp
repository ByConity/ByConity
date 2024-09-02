#include <IO/Operators.h>
#include <Optimizer/IntermediateResult/CacheParam.h>
#include <Protos/RPCHelpers.h>
#include <Protos/plan_node_utils.pb.h>
#include <QueryPlan/PlanSerDerHelper.h>

namespace DB
{

void CacheParam::toProto(Protos::CacheParam & proto, [[maybe_unused]] bool for_hash_equals) const
{
    proto.set_digest(digest);
    serializeMapToProto(output_pos_to_cache_pos, *proto.mutable_output_pos_to_cache_pos());
    serializeMapToProto(cache_pos_to_output_pos, *proto.mutable_cache_pos_to_output_pos());
    RPCHelpers::fillStorageID(cached_table, *proto.mutable_cached_table());
    for (const auto & element : dependent_tables)
        element.toProto(*proto.add_dependent_tables());
}

void CacheParam::fillFromProto(const Protos::CacheParam & proto)
{
    digest = proto.digest();
    output_pos_to_cache_pos = deserializeMapFromProto<size_t, size_t>(proto.output_pos_to_cache_pos());
    cache_pos_to_output_pos = deserializeMapFromProto<size_t, size_t>(proto.cache_pos_to_output_pos());
    cached_table = RPCHelpers::createStorageID(proto.cached_table());
    for (const auto & proto_element : proto.dependent_tables())
    {
        auto storage_id = RPCHelpers::createStorageID(proto_element);
        dependent_tables.emplace_back(std::move(storage_id));
    }
}
}
