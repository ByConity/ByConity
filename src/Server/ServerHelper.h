#pragma once

#include <Common/Logger.h>
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
void setupTmpPath(LoggerPtr log, const std::string & path);
}
