#pragma once

#include <ResourceGroup/IResourceGroup.h>
#include <Parsers/IAST.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Timer.h>

#include <atomic>
#include <regex>
#include <unordered_set>
#include <vector>

namespace DB
{
class InternalResourceGroupManager : public IResourceGroupManager
{
public:
    InternalResourceGroupManager() {}
    ~InternalResourceGroupManager() override = default;

    IResourceGroup * selectGroup(const Context & query_context, const IAST * ast) override;
    void initialize(const Poco::Util::AbstractConfiguration & config) override;
    void shutdown() override {}
private:
    std::unique_ptr<ResourceTask> resource_task;
};

}
