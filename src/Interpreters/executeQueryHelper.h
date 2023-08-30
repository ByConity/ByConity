#pragma once

#include <DataStreams/BlockIO.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Common/HostWithPorts.h>
namespace DB
{
HostWithPorts getTargetServer(ContextPtr context, ASTPtr & ast);

void executeQueryByProxy(ContextMutablePtr context, const HostWithPorts & server, const ASTPtr & ast, BlockIO & res);
}
