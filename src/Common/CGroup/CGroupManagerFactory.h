#pragma once
#include <Common/CGroup/CGroupManager.h>

namespace DB
{

class CGroupManagerFactory
{
public:
    static CGroupManager & instance();
    static void loadFromConfig(const Poco::Util::AbstractConfiguration & config);
private:
    static CGroupManagerPtr cgroup_manager_instance;
};

}

