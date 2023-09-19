#pragma once


#include <Common/config.h>
#include "Interpreters/Context_fwd.h"

#if USE_LASFS
#    include <map>
#    include <string>
#    include <Core/Settings.h>
#    include <Interpreters/Context.h>


namespace DB
{
//the Settings map of lasfs
using LasfsSettings = std::map<std::string, std::string>;

//refresh lasfs settings from  current context
inline void refreshCurrentLasfsSettings(LasfsSettings & lasfsSettings, const ContextPtr & context)
{
    const Settings & settings = context->getSettingsRef();

    lasfsSettings.insert_or_assign("sessionToken", settings.lasfs_session_token.toString());

    lasfsSettings.insert_or_assign("identityId", settings.lasfs_identity_id.toString());
    lasfsSettings.insert_or_assign("identityType", settings.lasfs_identity_type.toString());


    lasfsSettings.insert_or_assign("accessKey", settings.lasfs_access_key.toString());
    lasfsSettings.insert_or_assign("secretKey", settings.lasfs_secret_key.toString());

    lasfsSettings.insert_or_assign("serviceName", settings.lasfs_service_name.toString());
    lasfsSettings.insert_or_assign("endpoint", settings.lasfs_endpoint.toString());
    lasfsSettings.insert_or_assign("region", settings.lasfs_region.toString());

    lasfsSettings.insert_or_assign("overwrite", settings.lasfs_overwrite.toString());
}
}

#endif

