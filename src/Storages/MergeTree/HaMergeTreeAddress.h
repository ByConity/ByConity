#pragma once
#include <common/types.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>


namespace DB
{

/// Lets you know where to send requests to get to the replica.

struct HaMergeTreeAddress
{
    String host;
    UInt16 replication_port;
    UInt16 queries_port;
    UInt16 ha_port;
    String database;
    String table;
    String scheme;

    HaMergeTreeAddress() = default;
    explicit HaMergeTreeAddress(const String & str)
    {
        fromString(str);
    }

    void writeText(WriteBuffer & out) const;

    void readText(ReadBuffer & in);

    String toString() const;

    void fromString(const String & str);
};

}
