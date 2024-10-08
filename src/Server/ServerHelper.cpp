#include "ServerHelper.h"
#include <Disks/IDisk.h>
#include <Poco/String.h>
#include "Common/StringUtils/StringUtils.h"
#include <Common/Exception.h>
#include <common/logger_useful.h>

namespace DB
{
std::string getCanonicalPath(std::string && path)
{
    Poco::trimInPlace(path);
    if (path.empty())
        throw Exception("path configuration parameter is empty", ErrorCodes::INVALID_CONFIG_PARAMETER);
    if (path.back() != '/')
        path += '/';
    return std::move(path);
}

std::string getUserName(uid_t user_id);
void setupTmpPath(LoggerPtr log, const std::string & path)
{
    LOG_DEBUG(log, "Setting up {} to store temporary data in it", path);

    fs::create_directories(path);

    /// Clearing old temporary files.
    fs::directory_iterator dir_end;
    for (fs::directory_iterator it(path); it != dir_end; ++it)
    {
        if (it->is_regular_file() && startsWith(it->path().filename(), "tmp"))
        {
            LOG_DEBUG(log, "Removing old temporary file {}", it->path().string());
            fs::remove(it->path());
        }
        else
            LOG_DEBUG(log, "Skipped file in temporary path {}", it->path().string());
    }
}
}
