#include <Common/CGroup/CGroupManagerFactory.h>
#include <iostream>

namespace DB
{

CGroupManagerPtr CGroupManagerFactory::cgroup_manager_instance = std::make_shared<CGroupManager>(CGroupManager::PassKey());

CGroupManager & DB::CGroupManagerFactory::instance()
{
    return *cgroup_manager_instance;
}

void CGroupManagerFactory::loadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has("enable_cgroup") || !config.getBool("enable_cgroup"))
        return;

    if (!CGroupManagerFactory::instance().isInit() && config.has("enable_cpuset") && config.getBool("enable_cpuset"))
    {
        LOG_INFO(&Poco::Logger::get("CGroupManager"), "Init CGroupManager");
        CGroupManagerFactory::instance().init();
        if (config.has("root_cpuset_path") && !config.getString("root_cpuset_path").empty())
        {
            CGroupManagerFactory::instance().setCGroupCpuSetPath(config.getString("root_cpuset_path"));
        }

        if (cgroup_manager_instance->enable())
        {
            using Keys = Poco::Util::AbstractConfiguration::Keys;
            Keys keys;
            config.keys("cpuset", keys);
            for (const String & key : keys)
            {
                const String & cpu_set_name = key;
                String cpus = config.getString("cpuset."+key);
                if (nullptr != cgroup_manager_instance->getCpuSet(cpu_set_name))
                    continue;
                LOG_INFO(&Poco::Logger::get("CGroupManager"), "create cpuset: " + cpu_set_name + " cpus: " + cpus);
                cgroup_manager_instance->createCpuSet(cpu_set_name, cpus);
            }
        }
    }

    if (config.has("enable_cpu") && config.getBool("enable_cpu"))
    {
        if (config.has("root_cpu_path") && !config.getString("root_cpu_path").empty())
            CGroupManagerFactory::instance().setCGgroupCpuPath(config.getString("root_cpu_path"));
    }
}

}
