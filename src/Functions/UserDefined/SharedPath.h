#pragma once
#include <sys/stat.h>
#include <sys/types.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Path.h>

static inline std::string getUDFPath(const Poco::Util::AbstractConfiguration *cfg)
{
    std::string dflt = cfg->getString("path", "/tmp") + "/user_defined";
    std::string path = cfg->getString("udf_path",
        Poco::Path(dflt).makeAbsolute().toString());

    mkdir(path.c_str(), 0755);

    return path;
}

static inline std::string getUDSPath(std::string udf_path)
{
    return udf_path + "/udf_manager.sock";
}
