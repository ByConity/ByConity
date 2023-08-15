#pragma once

#include <string>
#include <Poco/Logger.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

/**
 * @brief Get path with a trailing `/`.
 */
std::string getCanonicalPath(std::string && path);
/**
 * @brief Setup temporary path for later use.
 */
void setupTmpPath(Poco::Logger * log, const std::string & path);
}
