#pragma once

#include <Common/Logger.h>
#include <Common/SettingsChanges.h>
#include <Core/QualifiedTableName.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/Dump/DumpUtils.h>
#include <Optimizer/Dump/ReproduceUtils.h>
#include <Poco/JSON/Object.h>
#include <Poco/Logger.h>

#include <string>
#include <optional>
#include <unordered_map>

namespace DB
{

class PlanReproducer
{
public:
    /**
    * \brief In correspondence with QueryDumper::Query, stores the query to be reproduced and the reproduce outcome
    */
    struct Query
    {
        const std::string query_id;
        const std::string query;
        const std::string current_database;
        const SettingsChanges settings_changes;
        const std::optional<std::string> memory_catalog_worker_size;
        const std::optional<std::string> original_explain;
    };

    explicit PlanReproducer(const std::string & file_path, ContextPtr from_context)
        : reproduce_path(ReproduceUtils::getFolder(file_path))
        , cluster(std::nullopt)
        , latest_context(Context::createCopy(from_context))
    {
        Poco::JSON::Object::Ptr client_file_json = readJsonFile();
        ddls =  client_file_json->getObject("ddls");
        views = client_file_json->getObject("views");
        stats = client_file_json->getObject("stats");
        queries = client_file_json->getObject("queries");
    }

    void setCluster(const std::string & cluster_name) {cluster = std::make_optional(cluster_name);}

    Query getQuery(const std::string & query_id);
    Poco::JSON::Object::Ptr getQueries() {return queries;}

    ContextMutablePtr makeQueryContext(
        const SettingsChanges & settings_changes = {},
        const std::optional<std::string> & database_name = std::nullopt,
        const std::optional<std::string> & memory_catalog_worker_size = std::nullopt);

    Poco::JSON::Object::Ptr readJsonFile()
    {
        return ReproduceUtils::readJsonFromAbsolutePath(reproduce_path);
    }

    void createTables(bool load_stats);

    // the context is used to determine whether the catalog adaptor will be created in memory or not
    void loadStats(ContextPtr catalog_adaptor_context, const std::unordered_set<QualifiedTableName> & tables_to_load);

    std::unordered_map<std::string, ReproduceUtils::DDLStatus> database_status;
    std::unordered_map<QualifiedTableName, ReproduceUtils::DDLStatus> table_status;

private:
    void createDatabase(const std::string & database_name);
    void createTable(const std::string & ddl);
    void dropDatabase(const std::string & database_name);
    void dropTable(const QualifiedTableName & table);
    // database/tables created are not visible in the current transaction
    void updateTransaction();

    const std::string reproduce_path;
    Poco::JSON::Object::Ptr ddls;
    Poco::JSON::Object::Ptr views;
    Poco::JSON::Object::Ptr queries;
    Poco::JSON::Object::Ptr stats;
    std::optional<String> cluster;
    ContextMutablePtr latest_context;
    const LoggerPtr log = getLogger("PlanReproducer");
};


}
