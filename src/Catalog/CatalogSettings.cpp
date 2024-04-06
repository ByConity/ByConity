#include <Catalog/CatalogSettings.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

template <typename T>
void valueLoadFromConfig(const Poco::Util::AbstractConfiguration & config, const String & key, AtomicSetting<T> & value)
{
    if (config.has(key))
        value = config.getUInt64(key);
}

template <>
void valueLoadFromConfig<bool>(const Poco::Util::AbstractConfiguration & config, const String & key, AtomicSettingBool & value)
{
    if (config.has(key))
        value = config.getBool(key);
}

void CatalogSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    for (const String & key : config_keys)
    {
        auto full_key = config_elem + "." + key;

#define SET(TYPE, NAME, DEFAULT) \
        else if (key == #NAME) valueLoadFromConfig(config, full_key, this->NAME);

        if (false) {}
        APPLY_FOR_CATALOG_SETTINGS(SET)
        else
            throw Exception("Unknown Catalog setting " + key + " in config", ErrorCodes::INVALID_CONFIG_PARAMETER);
#undef SET
    }
}
}
