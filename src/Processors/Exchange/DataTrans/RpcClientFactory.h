#pragma once

#include "RpcClient.h"

#include <mutex>
#include <unordered_map>
#include <Interpreters/Context.h>
#include <boost/noncopyable.hpp>
#include <brpc/channel.h>
#include <common/logger_useful.h>

namespace DB
{
// todo::aron need zhiyuan's refactor here
class RpcClientFactory : public boost::noncopyable
{
private:
    using RpcClients = std::unordered_map<String, std::shared_ptr<RpcClient>>;

public:
    static RpcClientFactory & getInstance()
    {
        static RpcClientFactory factory;
        return factory;
    }

    ~RpcClientFactory()
    {
        std::unique_lock lock(mutex);
        clients.clear();
    }

    std::shared_ptr<RpcClient> getClient(const String & host_port, bool connection_reuse = true)
    {
        if (!connection_reuse)
        {
            return std::make_shared<RpcClient>(host_port);
        }
        else
        {
            std::unique_lock lock(mutex);
            auto client_iter = clients.find(host_port);
            if (client_iter != clients.end())
            {
                if ((*client_iter).second->ok())
                    return clients[host_port];
                else
                {
                    clients.erase(host_port);
                    clients[host_port] = std::make_shared<RpcClient>(host_port);
                }
            }
            else
                clients[host_port] = std::make_shared<RpcClient>(host_port);
            return clients[host_port];
        }
    }

private:
    std::mutex mutex;
    RpcClients clients;
};

}
