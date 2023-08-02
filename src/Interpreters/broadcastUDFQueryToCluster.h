#pragma once
#include "Parsers/IAST_fwd.h"
#include "Interpreters/Context.h"

namespace DB
{

void broadcastUDFQueryToCluster(const ASTPtr & query_ptr, ContextMutablePtr context);

bool isHostServer(bool is_internal, const ContextMutablePtr context);
}
