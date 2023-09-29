#pragma once

#include <Functions/FunctionsHashing.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/Dump/DumpUtils.h>

#include <memory>
#include <string>
#include <unordered_map>

namespace DB
{

class QueryDumper
{
public:
    /**
     * \brief storing query information to be dumped
     */
    struct Query
    {
        const String query_id;
        const String query;
        const String current_database;
        const UInt64 recurring_hash;
        std::shared_ptr<Settings> settings;
        std::unordered_map<DumpUtils::QueryInfo, std::string> info;

        explicit Query(std::string _query_id,
                       std::string _query,
                       std::string _current_database)
            : query_id(std::move(_query_id))
            , query(std::move(_query))
            , current_database(std::move(_current_database))
            , recurring_hash(MurmurHash3Impl64::combineHashes(MurmurHash3Impl64::apply(query.c_str(), query.size()),
                                                              MurmurHash3Impl64::apply(current_database.c_str(), current_database.size()))) {}

        ContextMutablePtr buildQueryContextFrom(ContextPtr context) const;
    };

    explicit QueryDumper(const std::string & _dump_path): dump_path(DumpUtils::simplifyPath(_dump_path))
    {
        DumpUtils::createFolder(dump_path);
    }

    Query & add(const Query & query)
    {
        query_buffer.emplace_back(query);
        query_hash_frequencies[query.recurring_hash] = 1;
        return query_buffer.back();
    }

    size_t size() const { return query_buffer.size(); }

    void dump();

    std::unordered_map<UInt64, size_t> query_hash_frequencies;

private:
    const std::string dump_path;
    std::vector<Query> query_buffer;
};

}
