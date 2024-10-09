#include <Backups/BackupSettings.h>
#include <Core/SettingsFields.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Interpreters/Context.h>
#include <Common/SettingsChanges.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_BACKUP_SETTINGS;
    extern const int WRONG_BACKUP_SETTINGS;
}

/// List of backup settings except base_backup_name and cluster_host_ids.
#define LIST_OF_BACKUP_SETTINGS(M) \
    M(String, compression_method) \
    M(Bool, async) \
    M(String, virtual_warehouse) \
    M(Bool, enable_auto_recover) \
    M(UInt64, max_backup_entries)

BackupSettings BackupSettings::fromBackupQuery(const ASTBackupQuery & query, ContextMutablePtr & context)
{
    BackupSettings res;

    if (query.settings)
    {
        const auto & settings = query.settings->as<const ASTSetQuery &>().changes;
        SettingsChanges context_changes;
        for (const auto & setting : settings)
        {
            if (setting.name == "compression_level")
                res.compression_level = static_cast<int>(SettingFieldInt64{setting.value}.value);
            else
#define GET_SETTINGS_FROM_BACKUP_QUERY_HELPER(TYPE, NAME) \
            if (setting.name == #NAME) \
                res.NAME = SettingField##TYPE{setting.value}.value; \
            else

            LIST_OF_BACKUP_SETTINGS(GET_SETTINGS_FROM_BACKUP_QUERY_HELPER)
            // If settings doesn't exist in BackupSettings, set it to context.
            context_changes.setSetting(setting.name, setting.value);
        }
        context->checkSettingsConstraints(context_changes);
        context->applySettingsChanges(context_changes);
    }

    return res;
}

void BackupSettings::copySettingsToQuery(ASTBackupQuery & query) const
{
    auto query_settings = std::make_shared<ASTSetQuery>();
    query_settings->is_standalone = false;

    static const BackupSettings default_settings;
    bool all_settings_are_default = true;

#define SET_SETTINGS_IN_BACKUP_QUERY_HELPER(TYPE, NAME) \
    if ((NAME) != default_settings.NAME) \
    { \
        query_settings->changes.emplace_back(#NAME, static_cast<Field>(SettingField##TYPE{NAME})); \
        all_settings_are_default = false; \
    }

    LIST_OF_BACKUP_SETTINGS(SET_SETTINGS_IN_BACKUP_QUERY_HELPER)

    if (all_settings_are_default)
        query_settings = nullptr;

    query.settings = query_settings;
}

}
