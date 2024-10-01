#pragma once

#include <Poco/JSON/Object.h>
#include <common/types.h>

#include <string>
#include <unordered_map>

namespace DB::DumpUtils
{

/**
 * @enum QueryInfo defines the type of information that can be dumped with the query
 */
enum class QueryInfo
{
    query = 0,
    current_database = 1,
    settings = 2,
    memory_catalog_worker_size = 3,
    explain = 4,
    frequency = 5,
};

constexpr const char * DUMP_RESULT_FILE = "dump_result.json";

const char * toString(QueryInfo type);
std::string simplifyPath(const std::string & path);
void createFolder(const std::string & simplified_path_to_folder);
void zipDirectory(const std::string & simplified_path_to_folder);
void writeJsonToAbsolutePath(const Poco::JSON::Object & json, const std::string & absolute_path);

struct DumpSettings
{
    bool without_ddl = false;
    bool stats = true;
    bool shard_count = true;
    bool compress_directory = false;
    bool version = true;

    constexpr static char name[] = "Dump";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings = {
        {"without_ddl", without_ddl},
        {"stats", stats},
        {"shard_count", shard_count},
        {"compress_directory", compress_directory},
        {"version", version},
    };
    std::unordered_map<std::string, std::reference_wrapper<UInt64>> uint_settings = {};
};
}


