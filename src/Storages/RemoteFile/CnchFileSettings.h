#pragma once

#include <Core/Field.h>
#include <Core/BaseSettings.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/SettingsChanges.h>

namespace DB
{
class ASTStorage;
struct Settings;

#define APPLY_FOR_CNCHFILE_SETTINGS(M) \
    M(UInt64, index_granularity, 8192, "", 0) \
    M(String, cnch_vw_default, "vw_default", "", 0) \
    M(String, cnch_vw_read, "vw_read", "", 0) \
    M(String, cnch_vw_write, "vw_write", "", 0) \
    M(String, cnch_vw_task, "vw_task", "", 0) \
    M(String, resources_assign_type, "server_push", "", 0) \
    M(Bool, simple_hash_resources, true, "", 0) \
    M(Bool, cnch_temporary_table, false, "", 0) \
    M(Bool, prefer_cnch_catalog, true, "", 0)

/// Settings that should not change after the creation of a table.
#define APPLY_FOR_IMMUTABLE_CNCH_FILE_SETTINGS(M) M(index_granularity)

    DECLARE_SETTINGS_TRAITS(CnchFileSettingsTraits, APPLY_FOR_CNCHFILE_SETTINGS)



enum StorageResourcesAssignType
{
    WORKER_FETCH, // cnch server node will hold global data source and send it after receiving worker read-resource-request
    SERVER_PUSH, // cnch server node will send partial data source to workers by sharding rules
    SERVER_LOCAL
};

struct CnchFileSettings : public BaseSettings<CnchFileSettingsTraits>
    {
public:
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);

    /// NOTE: will rewrite the AST to add immutable settings.
    void loadFromQuery(DB::ASTStorage & storage_def, bool attach = false);
    void applyCnchFileSettingChanges(const SettingsChanges & changes);

    StorageResourcesAssignType resourcesAssignType() const
    {
        if (resources_assign_type.value == "server_push")
            return StorageResourcesAssignType::SERVER_PUSH;
        if (resources_assign_type.value == "server_local")
            return StorageResourcesAssignType::SERVER_LOCAL;
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unknown storage resources assign type: {}", resources_assign_type.value);
    }
};
}
