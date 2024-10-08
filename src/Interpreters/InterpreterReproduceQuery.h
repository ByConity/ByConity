#pragma once

#include <Common/Logger.h>
#include <DataStreams/BlockIO.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/Logger.h>
#include <Optimizer/Dump/PlanReproducer.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>

namespace DB
{
class InterpreterReproduceQuery : public IInterpreter, WithContext
{
public:
    InterpreterReproduceQuery(ASTPtr & query_ptr_, ContextMutablePtr & context_)
        : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    SelectQueryOptions options;
    const LoggerPtr log = getLogger("InterpreterReproduceQuery");

    /// database, table, status
    static BlockIO reproduceDDLImpl(PlanReproducer && reproducer);
    /// query_id, same_explain, exception, [query, original_explain, reproduced_explain]
    static BlockIO reproduceExplainImpl(PlanReproducer && reproducer,
                                        const std::vector<PlanReproducer::Query> & queries,
                                        const SettingsChanges & enforced_settings,
                                        bool verbose);
    /// query_id, has_exception, exception, [query]
    static BlockIO reproduceExecuteImpl(PlanReproducer && reproducer,
                                        const std::vector<PlanReproducer::Query> & queries,
                                        const SettingsChanges & enforced_settings,
                                        bool verbose);
};

}
