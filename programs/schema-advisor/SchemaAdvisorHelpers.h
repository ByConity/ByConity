#pragma once

#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>
#include <Advisor/Rules/WorkloadAdvisor.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/Dump/PlanReproducer.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <boost/program_options/variables_map.hpp>
#include <Poco/DirectoryIterator.h>
#include <Poco/FormattingChannel.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/PatternFormatter.h>
#include <Poco/StreamCopier.h>
#include <Poco/String.h>
#include <Common/Logger.h>
#include <Common/escapeForFileName.h>
#include <Common/formatIPv6.h>
#include "MockEnvironment.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int NO_FILE_IN_DATA_PART;
}

namespace po = boost::program_options;

[[maybe_unused]] static void setupLogging(const std::string & log_level)
{
    Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel);
    Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter);
    formatter->setProperty("pattern", "%L%Y-%m-%d %H:%M:%S.%i <%p> %s: %t");
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
    Poco::Logger::root().setChannel(formatting_channel);
    Poco::Logger::root().setLevel(log_level);
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int NETWORK_ERROR;
}

namespace po = boost::program_options;

static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;
static constexpr size_t DEFAULT_MAX_SAMPLE_CANDIDATE_NUM = 20;
static constexpr auto DEFAULT_TOS_PSM = "toutiao.tos.tosapi";

struct SamplingColumnFile
{
    SamplingColumnFile(std::string file_path_, std::string column_name_)
        : file_path(std::move(file_path_)), column_name(std::move(column_name_))
    {
    }

    std::string file_path;
    std::string column_name;
    size_t origin_file_size = 0;
    size_t optimized_file_size = 0;
};

using SamplingColumnFilePtr = std::shared_ptr<SamplingColumnFile>;
using SamplingColumnFiles = std::vector<SamplingColumnFilePtr>;

// a thread-safe implementation
class MessageCollector
{
public:
    void collect(std::string && msg)
    {
        std::lock_guard lock(mutex);
        messages.emplace_back(std::move(msg));
    }

    void logCollectedError()
    {
        for (const auto & msg : messages)
            LOG_ERROR(getLogger("MessageCollector"), "{}", msg);
        messages.clear();
    }

private:
    std::vector<std::string> messages;
    bthread::Mutex mutex;
};

static std::string readSqlFile(String source_uri, [[maybe_unused]]const po::variables_map & options)
{
    // std::string uri_prefix = source_uri.substr(0, source_uri.find_last_of('/'));
    // Poco::URI uri(uri_prefix);
    // const String& scheme = uri.getScheme();


    // if (scheme == "tos") // tos on cloud, url like "tos://bucket/key"
    // {
    //     if (!options.count("tos-ak"))
    //         throw Exception("Option tos-ak is missing for tos uri", ErrorCodes::BAD_ARGUMENTS);
    //     std::string tos_ak = options["tos-ak"].as<std::string>();

    //     Poco::URI tos_uri(source_uri);
    //     auto host = tos_uri.getHost();
    //     auto port = tos_uri.getPort();
    //     std::string tos_psm = DEFAULT_TOS_PSM;
    //     std::string tos_server;

    //     if (host.empty() || port == 0)
    //     {
    //         auto tos_servers = ServiceDiscovery::lookup(DEFAULT_TOS_PSM, std::pair<std::string, std::string>("cluster", "default"));
    //         if (tos_servers.empty())
    //             throw Exception("Can not find tos servers with PSM: " + tos_psm, ErrorCodes::NETWORK_ERROR);
    //         auto generator = std::mt19937(std::random_device{}()); // mt19937 engine
    //         std::uniform_int_distribution<int> distribution(0, tos_servers.size() - 1);
    //         tos_server = tos_servers.at(distribution(generator));
    //     }
    //     else
    //     {
    //         tos_server = normalizeHost(host) + ":" + toString(port);
    //     }

    //     ConnectionTimeouts timeouts(
    //         {DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT, 0},
    //         {DEFAULT_HTTP_READ_BUFFER_TIMEOUT, 0},
    //         {DEFAULT_HTTP_READ_BUFFER_TIMEOUT, 0});

    //     std::string tos_http_uri_str = fmt::format(
    //         "http://{}{}?timeout={}s", tos_server, tos_uri.getPath(), DBMS_DEFAULT_CONNECT_TIMEOUT_SEC);
    //     Poco::URI tos_http_uri = Poco::URI(tos_http_uri_str);
    //     HTTPSessionPtr session = makeHTTPSession(tos_http_uri, timeouts);

    //     Poco::Net::HTTPRequest request{Poco::Net::HTTPRequest::HTTP_GET, tos_http_uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1};
    //     request.set("X-Tos-Access", tos_ak);
    //     request.setHost(tos_http_uri.getHost());
    //     request.setChunkedTransferEncoding(false);

    //     session->sendRequest(request);
    //     Poco::Net::HTTPResponse response;
    //     std::istream * response_body = receiveResponse(*session, request, response, false);
    //     Poco::StreamCopier::copyToString(*response_body, res);
    // }
    // #if USE_VE_TOS
    // else if (scheme == "vetos") // tos on volcano engine, url like "vetos://bucket/key"
    // {
    //     Poco::URI vetos_uri(source_uri);
    //     vetos_uri.getPath();
    //     if(vetos_uri.getPath().empty() || vetos_uri.getHost().empty())
    //     {
    //         throw Exception("Invalid ve-tos path.", ErrorCodes::LOGICAL_ERROR);
    //     }
    //     const String& bucket = vetos_uri.getHost();
    //     size_t size = vetos_uri.getPath().size();
    //     String key = vetos_uri.getPath().substr(1, size - 1);
    //     if (!options.count("vetos-endpoint"))
    //             throw Exception("Option vetos-endpoint is missing for ve tos uri", ErrorCodes::BAD_ARGUMENTS);
    //     if (!options.count("vetos-region"))
    //             throw Exception("Option vetos-region is missing for ve tos uri", ErrorCodes::BAD_ARGUMENTS);
    //     if (!options.count("vetos-ak"))
    //             throw Exception("Option vetos-ak is missing for ve tos uri", ErrorCodes::BAD_ARGUMENTS);
    //     if (!options.count("vetos-sk"))
    //             throw Exception("Option vetos-sk is missing for ve tos uri", ErrorCodes::BAD_ARGUMENTS);
    //     std::string ve_tos_endpoint = options["vetos-endpoint"].as<std::string>();
    //     std::string ve_tos_region = options["vetos-region"].as<std::string>();
    //     std::string ve_tos_ak = options["vetos-ak"].as<std::string>();
    //     std::string ve_tos_sk = options["vetos-sk"].as<std::string>();

    //     std::unique_ptr<ReadBuffer> read_buf =
    //         std::make_unique<ReadBufferFromVETos>(ve_tos_endpoint, ve_tos_region, ve_tos_ak, ve_tos_sk, bucket, key);

    //     readStringUntilEOF(res, *read_buf);
    // }
    // #endif // USE_VE_TOS
    // else // absolute file path on local file system
    // {

    std::string res;

    std::ifstream fin(source_uri);
    std::stringstream buffer;
    buffer << fin.rdbuf();
    res = buffer.str();
    // }

    return res;
}


/// Select the target part according to specific rule if 'part' option is not specified
[[maybe_unused]] static std::string selectPartPath(const po::variables_map & options, const std::vector<std::string> & data_path_list, const std::string & db_name, const std::string & table_name, size_t sample_row_number)
{
    for (const auto & path : data_path_list)
    {
        if (options.count("part"))
        {
            std::string part = options["part"].as<std::string>();
            if (endsWith(part, "/"))
                part.pop_back();
            return path + "metadata/" + escapeForFileName(db_name) + "/" + escapeForFileName(table_name) + "/" + part;
        }

        std::string table_data_path = path + "data/" + escapeForFileName(db_name) + "/" + escapeForFileName(table_name) + "/";
        if (!std::filesystem::exists(table_data_path))
            continue;

        std::multimap<std::time_t, std::string> parts_by_timestamp;
        Poco::DirectoryIterator end;
        for (Poco::DirectoryIterator it(table_data_path); it != end; ++it)
        {
            if (it->isDirectory()
                && it.name() != "detached"
                && it.name() != "log"
                && it.name() != "catalog.db"
                && it.name() != "manifest"
                && !startsWith(it.name(), "tmp-fetch")
                && !startsWith(it.name(), "tmp_")
                && !startsWith(it.name(), "delete_tmp"))
            {
                size_t part_row_count;
                std::string part_count_path = it->path() + "/count.txt";
                {
                    ReadBufferFromFile in(part_count_path, METADATA_FILE_BUFFER_SIZE);
                    readIntText(part_row_count, in);
                    assertEOF(in);
                }
                if (part_row_count >= sample_row_number)
                {
                    parts_by_timestamp.emplace(it->getLastModified().epochTime(), it->path());
                    if (parts_by_timestamp.size() > DEFAULT_MAX_SAMPLE_CANDIDATE_NUM)
                        break;
                }
            }
        }
        if (!parts_by_timestamp.empty())
            return parts_by_timestamp.begin()->second;
    }

    throw Exception(db_name + "(" + table_name + "): failed to find qualified sample part.", ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART);
}

/// Generate output prefix in JSON format
[[maybe_unused]] static void serializeJsonPrefix(WriteBuffer & buf, std::string db_name, std::string table_name, std::string absolute_part_path, bool verbose)
{
    writeString(R"({"recommendation":{"db":")", buf);
    writeString(db_name, buf);
    writeString("\",", buf);
    writeString(R"("table":")", buf);
    writeString(table_name, buf);
    writeString("\",", buf);
    if (verbose)
    {
        writeString(R"("part selected":")", buf);
        writeString(absolute_part_path, buf);
        writeString("\",", buf);
    }
}

[[maybe_unused]] static void serializeJsonPrefixWithDB(WriteBuffer & buf, std::string db_name)
{
    writeString(R"({"recommendation":{"db":")", buf);
    writeString(db_name, buf);
    writeString("\",", buf);
}

/// Generate output suffix in JSON format
[[maybe_unused]] static void serializeJsonSuffix(WriteBuffer & buf)
{
    writeString("}}", buf);
}

/// Generate exception in JSON format
[[maybe_unused]] static void serializeException(WriteBuffer & buf, std::string error_msg)
{
    writeString(R"({"exception":")", buf);
    writeString(error_msg, buf);
    writeString("\"}", buf);
}

[[maybe_unused]] static std::vector<String> loadQueries(po::variables_map & options)
{
    std::string query_file = options["query-file"].as<std::string>();

    std::vector<std::string> splits;
    if (Poco::toLower(query_file).ends_with(".json"))
    {
        PlanReproducer reproducer{query_file, nullptr};
        for (const auto & name : reproducer.getQueries()->getNames())
            splits.emplace_back(reproducer.getQuery(name).query);
        return splits;
    }

    std::string query_content = readSqlFile(query_file, options);
    std::string delimiter = "\n";
    if (options.count("query-file-delimiter"))
        delimiter = options["query-file-delimiter"].as<std::string>();

    size_t last = 0;
    size_t next;
    while ((next = query_content.find(delimiter, last)) != std::string::npos)
    {
        auto query = query_content.substr(last, next - last);
        boost::replace_all(query, "\\r", "\r");
        boost::replace_all(query, "\\n", "\n");
        boost::replace_all(query, "\\t", "\t");
        boost::replace_all(query, "\\\"", "\"");
        boost::replace_all(query, "\\'", "'");
        splits.push_back(query);
        last = next + 1;
    }
    if (splits.empty())
        throw Poco::Exception("'" + query_file + "' is empty?");
    return splits;
}

[[maybe_unused]] static ContextMutablePtr createContext(po::variables_map & options, MockEnvironment & env)
{
    if (options["db"].empty())
        throw Exception("argument db is requried", ErrorCodes::BAD_ARGUMENTS);

    std::string db_name = options["db"].as<std::string>();
    std::vector<std::string> db_list;
    boost::algorithm::split(db_list, db_name, boost::is_any_of(","), boost::token_compress_on);

    if (db_list.empty())
        throw Exception("argument db is requried", ErrorCodes::BAD_ARGUMENTS);

    for (const auto & db : db_list)
    {
        env.createMockDatabase(db);
        // todo: currently we create all tables in the db
        for (const auto & table : env.listTables(db))
            env.createMockTable(db, table);
    }

    auto context = env.createQueryContext();
    context->setCurrentDatabase(db_list[0]);

    std::string settings = options["settings"].as<std::string>();
    if (!settings.empty())
    {
        ParserSetQuery parser{true};
        ASTPtr ast = parseQuery(parser, settings, 0, 0);
        context->applySettingsChanges(ast->as<ASTSetQuery>()->changes);
    }

    return context;
}

[[maybe_unused]] static void serializeJson(const std::string & advise_type, const String & advise_name, const String & db, const WorkloadAdvises & advises, WriteBuffer & buf, bool)
{
    Poco::JSON::Array advises_array;
    for (const auto & advise : advises)
    {
        Poco::JSON::Object advise_object;
        advise_object.set("db", advise->getTable().database);
        advise_object.set("table", advise->getTable().table);
        if (advise->getColumnName().has_value()) {
            advise_object.set("column", advise->getColumnName().value());
        }

        if (!advise->getCandidates().empty())
        {
            Poco::JSON::Array candidates;
            for (const auto & item : advise->getCandidates())
            {
                Poco::JSON::Object candidate_object;
                candidate_object.set(advise_name, item.first);
                candidate_object.set("benefit", item.second);
                candidates.add(candidate_object);
            }
            advise_object.set("candidates", candidates);
        }
        else
        {
            advise_object.set(advise_name, advise->getOptimizedValue());
            advise_object.set("benefit", advise->getBenefit());
        }

        if (!advise->getRelatedQueries().empty())
        {
            Poco::JSON::Array related_queries;
            for (const auto & query : advise->getRelatedQueries())
                related_queries.add(query);
            advise_object.set("relatedQueries", related_queries);
        }

        advises_array.add(advise_object);
    }

    Poco::JSON::Object advises_object;
    advises_object.set(advise_type, advises_array);

    Poco::JSON::Object recommendation_object;
    recommendation_object.set("db", db);
    recommendation_object.set(advise_type, advises_object);

    Poco::JSON::Object res;
    res.set("recommendation", recommendation_object);
    std::ostringstream oss;
    Poco::JSON::Stringifier::condense(res, oss);
    writeString(oss.str(), buf);
}
}
