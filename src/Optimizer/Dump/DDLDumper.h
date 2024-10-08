#pragma once

#include <Common/Logger.h>
#include <Core/QualifiedTableName.h>
#include <Core/Names.h>
#include <Databases/IDatabase.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTSelectQuery.h>
#include <Interpreters/Context_fwd.h>
#include <Optimizer/Dump/DumpUtils.h>
#include <Poco/JSON/Object.h>
#include <Poco/Logger.h>
#include <QueryPlan/QueryPlan.h>

#include <string>
#include <optional>

namespace DB
{
/**
 * @class DDLDumper is capable of dumping ddls + stats, which can be obtained from table/database/plan
 */
class DDLDumper
{
public:
    explicit DDLDumper(const std::string & _dump_path)
        : dump_path(DumpUtils::simplifyPath(_dump_path))
        , stats(new Poco::JSON::Object)
    {
        DumpUtils::createFolder(dump_path);
    }

    void addTable(const QualifiedTableName & qualified_name, ContextPtr context);
    void addTable(const std::string & database_name, const std::string & table_name, ContextPtr context);

    Poco::JSON::Object::Ptr getTableStats(const std::string & database_name, const std::string & table_name, ContextPtr context);

    // returns the first shard_count to avoid planning
    std::optional<size_t> addTableFromSelectQuery(ASTPtr query_ptr, ContextPtr context, const NameSet & with_tables_context = {});

    void addTableFromDatabase(DatabasePtr database, ContextPtr context);
    void addTableFromDatabase(const std::string & database_name, ContextPtr context);

    void addTableAll(ContextPtr context);

    size_t tables() const { return table_ddls.size(); }
    size_t views() const { return view_ddls.size(); }

    Poco::JSON::Object::Ptr getJsonDumpResult();
    void dumpStats(const std::optional<std::string> & absolute_path = std::nullopt);

    void setDumpSettings(DumpUtils::DumpSettings & settings_) { settings = settings_; }
private:
    // returns the first shard_count to avoid planning
    std::optional<size_t> addTableFromAST(ASTPtr ast, ContextPtr context, const NameSet & with_tables_context);
    std::string getPath(const char * file_name) { return dump_path + '/' + file_name; }

    const std::string dump_path;
    Poco::JSON::Object::Ptr stats;
    std::unordered_map<QualifiedTableName, String> table_ddls;
    std::unordered_map<QualifiedTableName, String> view_ddls;
    std::unordered_map<QualifiedTableName, size_t> shard_counts;
    std::unordered_set<QualifiedTableName> visited_tables;
    DumpUtils::DumpSettings settings;
    const LoggerPtr log = getLogger("DDLDumper");
};

}
