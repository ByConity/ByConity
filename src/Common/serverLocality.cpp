#include <Common/serverLocality.h>
#include <Common/isLocalAddress.h>
#include <Common/DNSResolver.h>
#include <common/getFQDNOrHostName.h>
#include <common/logger_useful.h>

bool isLocalServer(const std::string & target, const std::string & port)
{
    try
    {
        size_t pos = target.find_last_of(':');
        if (std::string::npos == pos)
        {
            LOG_ERROR(&Poco::Logger::get(__PRETTY_FUNCTION__),
                "Parse isLocalServer failed because cannot find colon in address {}", target);
            return false;
        }

        std::string target_ip = DB::DNSResolver::instance().resolveHost(target.substr(0, pos)).toString();
        std::string target_port = target.substr(pos+1, std::string::npos);

        if (target_port != port)
            return false;

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
        LOG_ERROR(&Poco::Logger::get(__PRETTY_FUNCTION__), "Parse isLocalServer failed for {}" , target);
    }
    return false;
}

