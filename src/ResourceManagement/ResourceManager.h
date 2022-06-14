#pragma once
#include <Common/Config/ConfigProcessor.h>
#include <daemon/BaseDaemon.h>

namespace DB
{

constexpr static auto ResourceManagerVersion = "1.0.0";

class ResourceManager: public BaseDaemon
{
public:
    using ServerApplication::run;

    ~ResourceManager() override = default;

    void defineOptions(Poco::Util::OptionSet & _options) override;

    void initialize(Poco::Util::Application &) override;

protected:
    int run() override;

    int main(const std::vector<std::string> & args) override;
};

}
