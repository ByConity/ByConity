#pragma once

#include <Core/QualifiedTableName.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/JSON/Object.h>
#include <Poco/Logger.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/StatisticsCollector.h>

#include <string>
#include <memory>
#include <unordered_set>

namespace DB
{

class StatsLoader : public WithContext
{
public:
    explicit StatsLoader(std::string _json_file_path, ContextPtr from_context)
        : WithContext(from_context)
        , json_file_path(std::move(_json_file_path))
        , stats_catalog(Statistics::createCatalogAdaptor(from_context)) {}
    std::unordered_set<QualifiedTableName> loadStats(bool load_all, const std::unordered_set<QualifiedTableName> & tables_to_load = {});
private:
    std::shared_ptr<Statistics::StatisticsCollector> readStatsFromJson(const std::string & database_name,
                                                                       const std::string & table_name,
                                                                       Poco::JSON::Object::Ptr stats_json);
    const std::string json_file_path;
    Statistics::CatalogAdaptorPtr stats_catalog;
    const Poco::Logger * log = &Poco::Logger::get("StatsLoader");
};
}
