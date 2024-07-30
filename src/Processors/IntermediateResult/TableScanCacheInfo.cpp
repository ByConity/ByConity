#include <Processors/IntermediateResult/TableScanCacheInfo.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <string>

namespace DB
{
void CacheDependentScanInfo::toProto(Protos::CacheDependentScanInfo & proto, [[maybe_unused]] bool for_hash_equals) const
{
    proto.set_required_digest(required_digest);
    proto.set_table_index(table_index);
}

void CacheDependentScanInfo::fillFromProto(const Protos::CacheDependentScanInfo & proto)
{
    required_digest = proto.required_digest();
    table_index = proto.table_index();
}

void TableScanCacheInfo::toProto(Protos::TableScanCacheInfo & proto, [[maybe_unused]] bool for_hash_equals) const
{
    proto.set_status(StatusConverter::toProto(status));
    proto.set_cache_digest(cache_digest);
    proto.set_dependent_table_count(dependent_table_count);
    for (const auto & element : cache_dependent_info)
        element.toProto(*proto.add_cache_dependent_info());
}

void TableScanCacheInfo::fillFromProto(const Protos::TableScanCacheInfo & proto)
{
    status = StatusConverter::fromProto(proto.status());
    cache_digest = proto.cache_digest();
    dependent_table_count = proto.dependent_table_count();
    for (const auto & proto_element : proto.cache_dependent_info())
    {
        CacheDependentScanInfo element;
        element.fillFromProto(proto_element);
        cache_dependent_info.emplace_back(std::move(element));
    }
}

} // DB
