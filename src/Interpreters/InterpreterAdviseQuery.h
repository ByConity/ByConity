#pragma once

#include <Advisor/WorkloadTable.h>
#include <Core/Types.h>
#include <DataStreams/BlockIO.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <string>
#include <vector>

namespace DB
{
/**
 * Advise optimized DDL from Workload and DDL queries.
 */
class InterpreterAdviseQuery : public IInterpreter, WithContext
{
public:
    InterpreterAdviseQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) { }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;

    std::vector<std::string> loadQueries();

    WorkloadTables loadTables();

    static void write(const String & output_file, std::vector<String> & queries);
};

}
