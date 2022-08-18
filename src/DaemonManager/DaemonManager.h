#pragma once

#include <Server/IServer.h>
#include <daemon/BaseDaemon.h>

namespace DB::DaemonManager
{

class DaemonManager : public BaseDaemon, public IServer
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

    void defineOptions(Poco::Util::OptionSet & _options) override;

protected:
    int run() override;

    void initialize(Application & self) override;

    void uninitialize() override;

    int main(const std::vector<std::string> & args) override;

    std::string getDefaultCorePath() const override;

private:
    ContextMutablePtr global_context;
};

}
