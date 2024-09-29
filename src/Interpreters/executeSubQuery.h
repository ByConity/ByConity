#pragma once

#include <Core/Settings.h>
#include <DataStreams/BlockIO.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/InterpreterSelectQueryUseOptimizer.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Common/SettingsChanges.h>

namespace DB
{

ContextMutablePtr createContextForSubQuery(ContextPtr context, String sub_query_tag = "");
ContextMutablePtr getContextWithNewTransaction(const ContextPtr & context, bool read_only, bool with_auth = false);

void executeSubQueryWithoutResult(const String & query, ContextMutablePtr query_context, bool internal = false);

Block executeSubQueryWithOneRow(
    const String & query, ContextMutablePtr query_context, bool internal = false, bool tolerate_multi_rows = true);

void executeSubQuery(const String & query, ContextMutablePtr query_context, std::function<void(Block &)> proc_block, bool internal = false);

Block executeSubPipelineWithOneRow(
    const ASTPtr & query,
    ContextMutablePtr query_context,
    std::function<void(InterpreterSelectQueryUseOptimizer &)> pre_execute,
    bool tolerate_multi_rows = true);

void executeSubPipeline(
    const ASTPtr & query,
    ContextMutablePtr query_context,
    std::function<void(InterpreterSelectQueryUseOptimizer &)> pre_execute,
    std::function<void(Block &)> proc_block);
}
