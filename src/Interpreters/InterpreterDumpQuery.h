#pragma once

#include <Common/Logger.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTDumpQuery.h>
#include <Interpreters/QueryLog.h>
#include <Optimizer/Dump/QueryDumper.h>
#include <Optimizer/Dump/DDLDumper.h>


namespace DB
{

class InterpreterDumpQuery : public IInterpreter, WithConstContext
{
public:
    InterpreterDumpQuery(const ASTPtr & query_ptr_, ContextPtr context_)
        : WithConstContext(context_)
        , query_ptr(query_ptr_)
        , dump_path(getDumpPath(query_ptr, context_))
        , ddl_dumper(dump_path)
        , query_dumper(dump_path)
        {
        }

    BlockIO execute() override;

private:
    static std::string getDumpPath(ASTPtr & query_ptr_, ContextPtr context_);

    void executeDumpDDLImpl();
    void executeDumpQueryImpl();
    void executeDumpWorkloadImpl();

    std::string getZipFilePath() const
    {
        // todo allow input path
        return dump_path + ".zip";
    }

    String prepareQueryLogSQL() const;

    ASTPtr prepareQueryLogSelectList() const;
    ASTPtr prepareQueryLogTableExpression() const;
    ASTPtr prepareQueryLogConditions() const;

    void processQueryLogBlock(const Block & block);

    void process(const String & query_id,
                 const String & sql,
                 const std::shared_ptr<Settings> & settings,
                 const String & current_database);

    void dumpToFile(Poco::JSON::Object::Ptr & dump_res);

    ASTPtr query_ptr;
    const std::string dump_path;
    DDLDumper ddl_dumper;
    QueryDumper query_dumper;
    bool enable_ddl = true;
    bool enable_explain = false;
    const LoggerPtr log = getLogger("InterpreterDumpWorkloadQuery");
};

}
