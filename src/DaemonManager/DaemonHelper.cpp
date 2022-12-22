#include <DaemonManager/DaemonHelper.h>
#include <DaemonManager/DaemonFactory.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace DaemonManager
{

std::map<std::string, unsigned int> updateConfig(
    std::map<std::string, unsigned int> && default_config,
    const Poco::Util::AbstractConfiguration & app_config)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    app_config.keys("daemon_manager.daemon_jobs", keys);

    std::for_each(keys.begin(), keys.end(),
        [& default_config, & app_config] (const std::string & key)
        {
            if (startsWith(key, "job"))
            {
                auto job_name = app_config.getString("daemon_manager.daemon_jobs." + key + ".name");

                if (!DaemonFactory::instance().validateJobName(job_name))
                    throw Exception("invalid config, there is no job named " + job_name,
                        ErrorCodes::INVALID_CONFIG_PARAMETER);

                auto it = default_config.find(job_name);
                if (it == default_config.end())
                    return;

                bool disable = app_config.getBool("daemon_manager.daemon_jobs." + key + ".disable", false);
                if (disable)
                {
                    default_config.erase(it);
                    return;
                }

                auto job_interval = app_config.getUInt("daemon_manager.daemon_jobs." + key + ".interval", 5000);
                it->second = job_interval;
            }
        }
    );

    return std::move(default_config);
}

void printConfig(std::map<std::string, unsigned int> & config, Poco::Logger * log)
{
    std::ostringstream oss;
    std::for_each(config.begin(), config.end(),
        [& oss] (const std::pair<std::string, unsigned int> & p)
        {
            oss << "Job name : " << p.first << ", job interval : " << p.second << '\n';
        }
    );

    LOG_INFO(log, oss.str());
}

} // end namespace DaemonManager

} // end namespace DB

