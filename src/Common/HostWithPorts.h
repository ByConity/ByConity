#pragma once
#include <cstdint>
#include <functional>
#include <ostream>
#include <string>
#include <vector>

namespace DB
{
struct HostWithPorts;
using HostWithPortsVec = std::vector<HostWithPorts>;

std::string addBracketsIfIpv6(const std::string & host);
std::string createHostPortString(const std::string & host, uint16_t port);
bool isSameHost(const std::string & lhs_host, const std::string & rhs_host);
std::string_view removeBracketsIfIpv6(const std::string & host_name);

struct HostWithPorts
{
    std::string id;
    std::string host;
    uint16_t rpc_port{0};
    uint16_t tcp_port{0};
    uint16_t http_port{0};
    uint16_t exchange_port{0};
    uint16_t exchange_status_port{0};

    bool empty() const { return host.empty() || (rpc_port == 0 && tcp_port == 0); }

    std::string getRPCAddress() const { return addBracketsIfIpv6(host) + ':' + std::to_string(rpc_port); }
    std::string getTCPAddress() const { return addBracketsIfIpv6(host) + ':' + std::to_string(tcp_port); }
    std::string getHTTPAddress() const { return addBracketsIfIpv6(host) + ':' + std::to_string(http_port); }
    std::string getExchangeAddress() const { return addBracketsIfIpv6(host) + ':' + std::to_string(exchange_port); }
    std::string getExchangeStatusAddress() const { return addBracketsIfIpv6(host) + ':' + std::to_string(exchange_status_port); }

    std::string toDebugString() const;

    static HostWithPorts fromRPCAddress(const std::string & s);

    /// NOTE: PLEASE DO NOT implement any comparison operator which is a kind of bad code style

    struct IsSameEndpoint
    {
        bool operator()(const HostWithPorts & lhs, const HostWithPorts & rhs) const
        {
            return isSameHost(lhs.host, rhs.host) && lhs.rpc_port == rhs.rpc_port && lhs.tcp_port == rhs.tcp_port;
        }
    };

    struct IsExactlySame
    {
        bool operator()(const HostWithPorts & lhs, const HostWithPorts & rhs) const
        {
            return lhs.id == rhs.id && isSameHost(lhs.host, rhs.host) && lhs.rpc_port == rhs.rpc_port && lhs.tcp_port == rhs.tcp_port
                && lhs.http_port == rhs.http_port && lhs.exchange_port == rhs.exchange_port
                && lhs.exchange_status_port == rhs.exchange_status_port;
        }
    };

    bool
    isSameEndpoint(const HostWithPorts & rhs) const
    {
        return IsSameEndpoint{}(*this, rhs);
    }

    bool isExactlySame(const HostWithPorts & rhs) const { return IsExactlySame{}(*this, rhs); }

    static bool isExactlySameVec(const HostWithPortsVec & lhs, const HostWithPortsVec & rhs);
};

std::ostream & operator<<(std::ostream & os, const HostWithPorts & host_ports);

}

namespace std
{
template <>
struct hash<DB::HostWithPorts>
{
    std::size_t operator()(const DB::HostWithPorts & hp) const
    {
        using namespace std;
        return hash<string>()(hp.host) ^ hash<uint16_t>()(hp.rpc_port) ^ (hash<uint16_t>()(hp.tcp_port) << 16);
    }
};
}
