#include <sstream>
#include <iostream>
#include <Common/Config/VWCustomizedSettings.h>
#include <Core/Types.h>

namespace DB
{
/**
<vw_customized_settings>
    <vw>
        <name>vw1</name>
        <enable_optimizer>1</enable_optimizer>
    </vw>
    
    <vw>
        <name>vw2</name>
        <enable_optimizer>0</enable_optimizer>
    </vw>
</vw_customized_settings>
*/
void VWCustomizedSettings::loadCustomizedSettings()
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config_holder->keys(config_keys);
    
    for (const String & key : config_keys)
    {
        Poco::Util::AbstractConfiguration::Keys nested_vw_config_keys;
        auto vw_name_config_key = key  + ".name";
        auto vw_name = config_holder->getString(vw_name_config_key);

        config_holder->keys(key, nested_vw_config_keys);
        std::unordered_map<String, String> vw_config;
        for (const String & nested_key : nested_vw_config_keys)
        {   
            if (nested_key != "name")
            {
                auto complete_key = key + "." + nested_key;
                auto raw_config_value = config_holder->getRawString(complete_key);

                vw_config.emplace(nested_key, raw_config_value);
            }
        }
        vw_config_keys.emplace(vw_name, vw_config);
    }
}

String VWCustomizedSettings::toString()
{
    std::stringstream to_string_stream;

    for (auto iter = vw_config_keys.begin(); iter != vw_config_keys.end(); iter++)
    {       
        to_string_stream << "[VW name:" << iter->first  << "]-";
        to_string_stream << "[";
        for (auto config_iter = iter->second.begin(); config_iter != iter->second.end(); config_iter++)
        {
            to_string_stream << "{" << config_iter->first << ":" << config_iter->second << "} ";
        }
        to_string_stream << "]" << ",";
    }

    to_string_stream << std::endl;
    return to_string_stream.str();
}

void VWCustomizedSettings::overwriteDefaultSettings(const String & vw_name, Settings & default_settings)
{
    auto vw_config_iter = vw_config_keys.find(vw_name);
    if (vw_config_iter == vw_config_keys.end())
        return;
    
    for (auto keys_iter = vw_config_iter->second.begin(); keys_iter != vw_config_iter->second.end(); keys_iter++)
    {
        default_settings.set(keys_iter->first, keys_iter->second);
    }
}

bool VWCustomizedSettings::isEmpty()
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config_holder->keys(config_keys);
    
    return config_keys.empty();
}

}
