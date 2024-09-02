#pragma once

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Protos/EnumMacros.h>
#include <Protos/plan_node_utils.pb.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace DB
{
/**
 * \brief for caches that involve join / runtime filters, it is required that data from the right table is unchanged for cache to match
 * in such cases, the right TableScan will store the CacheDependentScanInfo so that the left table can include the version info
 * when computing CacheKey
 */
struct CacheDependentScanInfo
{
    // the digest of the consumer
    std::string required_digest;
    // the index of this table in the digest
    size_t table_index;

    void toProto(Protos::CacheDependentScanInfo & proto, bool for_hash_equals = false) const;
    void fillFromProto(const Protos::CacheDependentScanInfo & proto);
};

class TableScanCacheInfo
{
public:
    ENUM_WITH_PROTO_CONVERTER(
        Status, // enum name
        Protos::TableScanCacheInfo::Status, // proto enum message
        (CACHE_TABLE),
        (CACHE_DEPENDENT_TABLE),
        (NO_CACHE));

    TableScanCacheInfo(): TableScanCacheInfo(Status::NO_CACHE, "", 0, {}) {}

    static TableScanCacheInfo create(std::string digest, size_t dependents)
    {
        return TableScanCacheInfo(Status::CACHE_TABLE, std::move(digest), dependents, {});
    }

    void addDependent(std::string digest, size_t table_index)
    {
        status = Status::CACHE_DEPENDENT_TABLE;
        cache_dependent_info.emplace_back(CacheDependentScanInfo{std::move(digest), table_index});
    }

    Status getStatus() const { return status; }
    std::string getDigest() const { return cache_digest; }
    const std::vector<CacheDependentScanInfo> & getDependentInfo() const { return cache_dependent_info; }

    void toProto(Protos::TableScanCacheInfo & proto, bool for_hash_equals = false) const;
    void fillFromProto(const Protos::TableScanCacheInfo & proto);

private:
    TableScanCacheInfo(Status status_,
                       std::string cache_digest_,
                       size_t dependent_table_count_,
                       std::vector<CacheDependentScanInfo> cache_dependent_info_)
        : status(status_)
        , cache_digest(std::move(cache_digest_))
        , dependent_table_count(dependent_table_count_)
        , cache_dependent_info(std::move(cache_dependent_info_)) {}

    Status status;
    std::string cache_digest; // only for CACHED_TABLE
    size_t dependent_table_count; // only for CACHED_TABLE
    std::vector<CacheDependentScanInfo> cache_dependent_info; // only for CACHE_DEPENDENT_TABLE
};

} // DB
