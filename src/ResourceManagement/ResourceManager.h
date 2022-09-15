#pragma once
#include <Common/Config/ConfigProcessor.h>
#include <Server/IServer.h>
#include <daemon/BaseDaemon.h>

namespace DB
{

constexpr static auto ResourceManagerVersion = "1.0.0";

class ResourceManager: public BaseDaemon, public IServer
{
public:
    using ServerApplication::run;
    
    Poco::Util::LayeredConfiguration & config() const override
    {
        return BaseDaemon::config();
    }

    Poco::Logger & logger() const override
    {
        return BaseDaemon::logger();
    }

    ContextMutablePtr context() const override
    {
        return global_context;
    }

    bool isCancelled() const override
    {
        return BaseDaemon::isCancelled();
    }
    
    ~ResourceManager() override = default;

    void defineOptions(Poco::Util::OptionSet & _options) override;

    void initialize(Poco::Util::Application &) override;

protected:
    int run() override;

    int main(const std::vector<std::string> & args) override;

private:
    ContextMutablePtr global_context;
};

}
