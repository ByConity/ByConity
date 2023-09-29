#include <Optimizer/Dump/DumpUtils.h>

#include <Poco/JSON/Object.h>
#include <Poco/Path.h>
#include <Poco/Zip/Compress.h>
#include <Poco/Zip/ZipCommon.h>

#include <filesystem>
#include <fstream>
#include <string>

namespace DB::DumpUtils
{

const char * toString(QueryInfo type)
{
    switch (type)
    {
        case QueryInfo::query:
            return "query";
        case QueryInfo::current_database:
            return "current_database";
        case QueryInfo::settings:
            return "settings";
        case QueryInfo::memory_catalog_worker_size:
            return "memory_catalog_worker_size";
        case QueryInfo::explain:
            return "explain";
        case QueryInfo::frequency:
            return "frequency";
    }
}

std::string simplifyPath(const std::string & path)
{
    size_t pos = path.find_last_not_of('/');
    return (pos != std::string::npos) ? path.substr(0, pos + 1) : path;
}

void zipDirectory(const std::string & simplified_path_to_folder)
{
    Poco::Path src_dir_path{simplified_path_to_folder + '/'};
    src_dir_path.makeDirectory();
    std::ofstream out_stream(simplified_path_to_folder + ".zip", std::ios::binary);
    Poco::Zip::Compress compress(out_stream, true);
    compress.addRecursive(src_dir_path, Poco::Zip::ZipCommon::CL_NORMAL);
    compress.close();
    out_stream.close();
}

void createFolder(const std::string & simplified_path_to_folder)
{
    std::filesystem::path folder_path(simplified_path_to_folder + '/');
    if (!std::filesystem::exists(folder_path))
        std::filesystem::create_directories(folder_path);
}

void writeJsonToAbsolutePath(const Poco::JSON::Object & json, const std::string & absolute_path)
{
    std::ofstream out(absolute_path);
    json.stringify(out);
    out.close();
}
}
