#include <Storages/Kafka/HaKafkaAddress.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

namespace DB
{
void HaKafkaAddress::writeText(WriteBuffer & out) const
{
    out
        << "host: " << escape << host << '\n'
        << "ha_port: " << ha_port << '\n'
        << "database: " << escape << database << '\n'
        << "table: " << escape << table << '\n';
}

void HaKafkaAddress::readText(ReadBuffer & in)
{
    in
        >> "host: " >> escape >> host >> "\n"
        >> "ha_port: " >> ha_port >> "\n"
        >> "database: " >> escape >> database >> "\n"
        >> "table: " >> escape >> table >> "\n";
}

String HaKafkaAddress::toString() const
{
    WriteBufferFromOwnString out;
    writeText(out);
    return out.str();
}

void HaKafkaAddress::fromString(const String & str)
{
    ReadBufferFromString in(str);
    readText(in);
}
}
