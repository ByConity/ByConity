#pragma once
#include <Core/Types.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Lets you know where to send requests to get to the replica.

struct HaKafkaAddress
{
    String host;
    UInt16 ha_port;
    String database;
    String table;

    HaKafkaAddress() = default;
    explicit HaKafkaAddress(const String & str) { fromString(str); }

    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);

    String toString() const;
    void fromString(const String & str);
};

}
