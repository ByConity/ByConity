#include <Common/ConfigurationCommon.h>

#include <Common/Exception.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
}

bool ConfigurationFieldBase::checkField(const PocoAbstractConfig & config, const String & current_prefix)
{
    full_key = current_prefix.empty() ? init_key : current_prefix + "." + init_key;

    if (config.has(full_key))
    {
        existed = true;

        if (deprecated())
        {
            LOG_WARNING(
                &Poco::Logger::get("Configuration"), "Config element {} is deprecated. Please remove corresponding tags!", full_key);
        }
    }
    else
    {
        if (recommended())
        {
            LOG_DEBUG(
                &Poco::Logger::get("Configuration"),
                "Config element {} is recommended to set in config.xml. You'd better customize it.", full_key);
        }
        else if (required())
        {
            throw Exception(
                "Config element " + full_key + " is required, but it is not in config.xml. Please add corresponding tags!",
                ErrorCodes::NO_ELEMENTS_IN_CONFIG);
        }
    }
    return existed;
}

void IConfiguration::loadFromPocoConfig(const PocoAbstractConfig & config, const String & current_prefix)
{
    for (auto * field : fields)
    {
        if (field->checkField(config, current_prefix))
            field->loadField(config);
    }

    loadFromPocoConfigImpl(config, current_prefix);
}

void IConfiguration::reloadFromPocoConfig(const PocoAbstractConfig & config)
{
    for (auto * field : fields)
        field->reloadField(config);

    for (auto * sub_config : sub_configs)
        sub_config->reloadFromPocoConfig(config);
}

}
