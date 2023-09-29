#pragma once

#include <Poco/JSON/Object.h>

#include <string>

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

constexpr const char * DDL_FILE = "ddl.json";
constexpr const char * VIEWS_FILE = "views.json";
constexpr const char * STATS_FILE = "stats.json";
constexpr const char * QUERIES_FILE = "queries.json";
constexpr const char * SHARD_COUNT_FILE = "shard_count.json";

const char * toString(QueryInfo type);
std::string simplifyPath(const std::string & path);
void createFolder(const std::string & simplified_path_to_folder);
void zipDirectory(const std::string & simplified_path_to_folder);
void writeJsonToAbsolutePath(const Poco::JSON::Object & json, const std::string & absolute_path);
}


