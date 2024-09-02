#pragma once

#include <Core/Types.h>
#include <sstream>

namespace DB
{

/// Due to undetermin result of std::hash
/// Here we choose to tolerate both versions' result of clang and gcc
/// And choose SipHash64 to store as parts' determin table_definition_hash
class TableDefinitionHash
{
public:
    TableDefinitionHash():determin_hash(0),v1_hash(0),v2_hash(0),v1_quoted_hash(0) {}
    TableDefinitionHash(UInt64 determin_hash_, UInt64 v1_hash_, UInt64 v2_hash_, UInt64 v1_quoted_hash_)
        :determin_hash(determin_hash_), v1_hash(v1_hash_), v2_hash(v2_hash_), v1_quoted_hash(v1_quoted_hash_) {}

    bool operator==(const TableDefinitionHash & other) const { return this->determin_hash == other.determin_hash; }
    bool match(UInt64 hash_value) const
    {
        return hash_value == determin_hash || hash_value == v1_hash || hash_value == v2_hash || hash_value == v1_quoted_hash; 
    }

    UInt64 getDeterminHash() const { return determin_hash; }

    String toString() const
    {
        std::stringstream ss;
        ss << "determin_hash: " << determin_hash
           << ", v1_hash: " << v1_hash
           << ", v2_hash: " << v2_hash
           << ", v1_quoted_hash: " << v1_quoted_hash;
        return ss.str();
    }
private:
    UInt64 determin_hash;
    UInt64 v1_hash;
    UInt64 v2_hash;
    UInt64 v1_quoted_hash;
};
}
