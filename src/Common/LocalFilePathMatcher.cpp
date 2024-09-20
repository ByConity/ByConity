#include <filesystem>
#include <Common/LocalFilePathMatcher.h>

namespace DB
{

FileInfos LocalFilePathMatcher::getFileInfos(const String & prefix_path)
{
    FileInfos file_infos;
    for (const auto & entry : std::filesystem::directory_iterator(prefix_path))
    {
        file_infos.emplace_back(entry.path(), entry.is_directory());
    }
    return file_infos;
}

}
