#include <Poco/Net/DNS.h>
#include <common/getFQDNOrHostName.h>


namespace
{
    std::string getFQDNOrHostNameImpl()
    {
        try
        {
            return Poco::Net::DNS::thisHost().name();
        }
        catch (...)
        {
            return Poco::Net::DNS::hostName();
        }
    }

    std::string getIPOrFQDNOrHostNameImpl()
    {
        try
        {
            auto this_host = Poco::Net::DNS::thisHost();
            if (this_host.addresses().size() > 0)
                return this_host.addresses().front().toString();
            else
                return this_host.name();
        }
        catch (...)
        {
            return Poco::Net::DNS::hostName();
        }
    }
}


const std::string & getFQDNOrHostName()
{
    static std::string result = getFQDNOrHostNameImpl();
    return result;
}

const std::string & getIPOrFQDNOrHostName()
{
    static std::string result = getIPOrFQDNOrHostNameImpl();
    return result;
}
