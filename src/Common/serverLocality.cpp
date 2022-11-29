#include <Common/serverLocality.h>
#include <Common/isLocalAddress.h>
#include <Common/DNSResolver.h>
#include <common/getFQDNOrHostName.h>
#include <common/logger_useful.h>

bool isLocalServer(const std::string & target, const std::string & port)
{
    try
    {
        const size_t pos = target.find_last_of(':');
        if (std::string::npos == pos)
        {
            LOG_ERROR(&Poco::Logger::get(__PRETTY_FUNCTION__),
                "Parse isLocalServer failed because cannot find colon in address {}", target);
            return false;
        }

        const std::string target_port = target.substr(pos+1, std::string::npos);

        if (target_port != port)
            return false;

        const std::string target_host = target.substr(0, pos);
        if (target_host.empty())
            return false;

        const std::string target_ip = DB::DNSResolver::instance().resolveHost(target_host).toString();

        if ((target_ip == "127.0.0.1") || (target_ip == "::1") || (target_ip == getIPOrFQDNOrHostName()))
        {
            return true;
        }
        else
        {
            Poco::Net::IPAddress ip_address{target_ip};
            return DB::isLocalAddress(ip_address);
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__, fmt::format("Parse isLocalServer failed for {}" , target));
    }
    return false;
}
