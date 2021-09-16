#include <Storages/MergeTree/HaMergeTreeAddress.h>

#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

void HaMergeTreeAddress::writeText(WriteBuffer & out) const
{
    out
        << "host: " << escape << host << '\n'
        << "port: " << replication_port << '\n'
        << "tcp_port: " << queries_port << '\n'
        << "ha_port: " << ha_port << '\n'
        << "database: " << escape << database << '\n'
        << "table: " << escape << table << '\n'
        << "scheme: " << escape << scheme << '\n';

}

void HaMergeTreeAddress::readText(ReadBuffer & in)
{
    in
        >> "host: " >> escape >> host >> "\n"
        >> "port: " >> replication_port >> "\n"
        >> "tcp_port: " >> queries_port >> "\n"
        >> "ha_port: " >> ha_port >> "\n"
        >> "database: " >> escape >> database >> "\n"
        >> "table: " >> escape >> table >> "\n";

    if (!in.eof())
        in >> "scheme: " >> escape >> scheme >> "\n";
    else
        scheme = "http";
}

String HaMergeTreeAddress::toString() const
{
    WriteBufferFromOwnString out;
    writeText(out);
    return out.str();
}

void HaMergeTreeAddress::fromString(const String & str)
{
    ReadBufferFromString in(str);
    readText(in);
}
}
