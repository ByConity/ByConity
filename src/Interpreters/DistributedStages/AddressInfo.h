#pragma once

#include <Core/Types.h>
#include <IO/WriteHelpers.h>


namespace DB
{
    class WriteBuffer;
    class ReadBuffer;

    class AddressInfo
    {
    public:
        AddressInfo() = default;
        AddressInfo(const String & host_name_, UInt16 port_, const String & user_, const String & password_);
        AddressInfo(const String & host_name_, UInt16 port_, const String & user_, const String & password_, UInt16 exchange_port_);
        AddressInfo(const String & host_name_, UInt16 port_, const String & user_, const String & password_, UInt16 exchange_port_, UInt16 exchange_status_port);

        void serialize(WriteBuffer &) const;
        void deserialize(ReadBuffer &);

        const String & getHostName() const { return host_name; }
        UInt16 getPort() const { return port; }
        UInt16 getExchangePort() const { return exchange_port;}
        UInt16 getExchangeStatusPort() const {return exchange_status_port;}
        const String & getUser() const { return user; }
        const String & getPassword() const { return password; }

        String toString() const
        {
            std::ostringstream ostr;

            ostr << "host_name: " << host_name << ", "
                 << "port: " << std::to_string(port) << ", "
                 << "user: " <<  user << ", "
                 << "password: " << password << ", "
                 << "exchange_port: " << exchange_port << ", "
                 << "exchange_status_port: " << exchange_status_port;
            return ostr.str();
        }

    private:
        String host_name;
        UInt16 port;
        String user;
        String password;
        UInt16 exchange_port;
        UInt16 exchange_status_port;
    };

    using AddressInfos = std::vector<AddressInfo>;

    inline String extractHostPort(const AddressInfo & address) { return address.getHostName() + ":" + toString(address.getPort()); }

    std::vector<String> extractHostPorts(const AddressInfos & addresses);

    inline String extractExchangeHostPort(const AddressInfo & address) {return address.getHostName() + ":" + toString(address.getExchangePort());}

    std::vector<String> extractExchangeHostPorts(const AddressInfos & addresses);

    inline String extractExchangeStatusHostPort(const AddressInfo & address) {return address.getHostName() + ":" + toString(address.getExchangeStatusPort());}

    std::vector<String> extractExchangeStatusHostPorts(const AddressInfos & addresses);

}
