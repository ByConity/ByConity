#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Common/HostWithPorts.h>

namespace DB
{
HostWithPorts getTargetServer(ContextPtr context, ASTPtr & ast);
struct BlockIO;
void executeQueryByProxy(ContextMutablePtr context, const HostWithPorts & server, const ASTPtr & ast, BlockIO & res, bool in_interactive_txn, const String & query);
struct QueryLogElement;
void setExceptionStackTrace(QueryLogElement & elem);
}
