#include "CnchFileSettings.h"

#include <filesystem>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <re2/stringpiece.h>
#include <Common/Exception.h>
#include <Common/SettingsChanges.h>
#include <common/logger_useful.h>
#include <Storages/IStorage.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int BAD_ARGUMENTS;
}

IMPLEMENT_SETTINGS_TRAITS(CnchFileSettingsTraits, APPLY_FOR_CNCHFILE_SETTINGS)

void CnchFileSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    try
    {
        for (const String & key : config_keys)
            set(key, config.getString(config_elem + "." + key));
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::INVALID_CONFIG_PARAMETER)
            e.addMessage("in CnchFile config");
        throw;
    }
}

void CnchFileSettings::loadFromQuery(ASTStorage & storage_def, bool /*attach*/)
{
    if (storage_def.settings)
    {
        try
        {
            applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::INVALID_CONFIG_PARAMETER)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }

    SettingsChanges & changes = storage_def.settings->changes;

#define ADD_IF_ABSENT(NAME) \
    if (std::find_if(changes.begin(), changes.end(), [](const SettingChange & c) { return c.name == #NAME; }) == changes.end()) \
        changes.push_back(SettingChange{#NAME, (NAME).value});

    APPLY_FOR_IMMUTABLE_CNCH_FILE_SETTINGS(ADD_IF_ABSENT)
#undef ADD_IF_ABSENT
}

void CnchFileSettings::applyCnchFileSettingChanges(const SettingsChanges & changes)
{
    for (const auto & setting : changes)
    {
#define SET(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
else if (setting.name == #NAME) set(#NAME, setting.value);
        if (false)
        {
        }
        APPLY_FOR_CNCHFILE_SETTINGS(SET)
        else throw Exception("Unknown CnchFile setting " + setting.name, ErrorCodes::BAD_ARGUMENTS);
#undef SET
    }
}

}
