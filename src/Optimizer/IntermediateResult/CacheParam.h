#pragma once

#include <Core/Types.h>
#include <Interpreters/StorageID.h>

#include <unordered_map>


namespace DB
{

namespace Protos
{
    class CacheParam;
}

struct CacheParam
{
    // plan digest equality is a necessary (but insufficient) condition for cache match
    std::string digest;
    // matches step output pos to cache pos
    std::unordered_map<size_t, size_t> output_pos_to_cache_pos;
    // reverse of the above map
    std::unordered_map<size_t, size_t> cache_pos_to_output_pos;
    // tables involved in the digest, except for the left-most table
    // the tables must produce equal results (parts) for cache match
    StorageID cached_table = StorageID::createEmpty();
    std::vector<StorageID> dependent_tables;
    void toProto(Protos::CacheParam & proto, bool for_hash_equals = false) const;
    void fillFromProto(const Protos::CacheParam & proto);
};

}
