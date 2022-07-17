#include <Common/HostWithPorts.h>
#include <Common/parseAddress.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::string addBracketsIfIpv6(const std::string & host_name)
{
    std::string res;

    if (host_name.find_first_of(':') != std::string::npos && !host_name.empty() && host_name.back() != ']')
        res += '[' + host_name + ']';
    else
        res = host_name;
    return res;
}

std::string createHostPortString(const std::string & host, uint16_t port)
{
    return createHostPortString(host, toString(port));
}

std::string createHostPortString(const std::string & host, const std::string & port)
{
    return addBracketsIfIpv6(host) + ':' + port;
}

std::string_view removeBracketsIfIpv6(const std::string & host_name)
{
    if (host_name.find_first_of(':') != std::string::npos &&
        !host_name.empty() &&
        host_name.back() == ']' &&
        host_name.front() == '['
    )
        return std::string_view(host_name.data() + 1, host_name.size() - 2);
    return std::string_view(host_name.c_str());
}

bool isSameHost(const std::string & lhs, const std::string & rhs)
{
    if (lhs == rhs)
        return true;
    return removeBracketsIfIpv6(lhs) == removeBracketsIfIpv6(rhs);
}

std::string HostWithPorts::toDebugString() const
{
    WriteBufferFromOwnString wb;

    wb << '{';
    if (!id.empty())
        wb << id << " ";
    if (!host.empty())
        wb << host << " ";
    if (rpc_port != 0)
        wb << " rpc/" << rpc_port;
    if (tcp_port != 0)
        wb << " tcp/" << tcp_port;
    if (exchange_port != 0)
        wb << " exc/" << tcp_port;
    if (exchange_status_port != 0)
        wb << " exs/" << tcp_port;
    wb << '}';

    return wb.str();
}

HostWithPorts HostWithPorts::fromRPCAddress(const std::string & s)
{
    std::pair<std::string, UInt16> host_port = parseAddress(s, 0);
    return HostWithPorts{
        // cannot get hostname information from RPCAddress
        // will it be a problem if assign empty string here?
        "",
        std::string{removeBracketsIfIpv6(host_port.first)},
        host_port.second,
    };
}

bool HostWithPorts::isExactlySameVec(const HostWithPortsVec & lhs, const HostWithPortsVec & rhs)
{
    return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), HostWithPorts::IsExactlySame{});
}

std::ostream & operator<<(std::ostream & os, const HostWithPorts & host_ports)
{
    os << host_ports.toDebugString();
    return os;
}

}
