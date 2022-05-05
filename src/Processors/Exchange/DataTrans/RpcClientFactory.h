#pragma once

#include "RpcClient.h"

#include <unordered_map>
#include <Interpreters/Context.h>
#include <boost/noncopyable.hpp>
#include <brpc/channel.h>
#include <bthread/mtx_cv_base.h>
#include "Common/Brpc/BrpcApplication.h"
#include "Common/Brpc/BrpcChannelConfigHolder.h"
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
            brpc::ChannelOptions channel_options = getChannelOptions();
            return std::make_shared<RpcClient>(host_port, &channel_options);
        }
        else
        {
            std::unique_lock lock(mutex);
            auto client_iter = clients.find(host_port);
            if (client_iter != clients.end() && (*client_iter).second->ok())
            {
                return clients[host_port];
            }
            brpc::ChannelOptions channel_options = getChannelOptions();
            clients[host_port] = std::make_shared<RpcClient>(host_port, &channel_options);
            return clients[host_port];
        }
    }

private:
    bthread::Mutex mutex;
    RpcClients clients;
    Poco::Logger * logger;
    std::shared_ptr<BrpcChannelConfigHolder> channel_config_holder;

    inline brpc::ChannelOptions getChannelOptions(){
        return channel_config_holder->queryConfig();
    }

    RpcClientFactory()
    {
        logger = & Poco::Logger::get("RpcClientFactory");
        channel_config_holder = BrpcApplication::getInstance().getConfigHolderByType<BrpcChannelConfigHolder>();
        auto reload_callback = [this](const brpc::ChannelOptions *, const brpc::ChannelOptions *)
        {
            LOG_INFO(this->logger, "Clear cached clients");
            std::unique_lock lock(this->mutex);
            this->clients.clear();
        };
        channel_config_holder->initReloadCallback(reload_callback);
    }
};

}
