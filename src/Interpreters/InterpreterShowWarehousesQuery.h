#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTShowWarehousesQuery.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/** Shows logical Virtual Warehouses
  * Syntax is as follows:
  * SHOW WAREHOUSE vwname [LIKE <'pattern'>]
  */
class InterpreterShowWarehousesQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterShowWarehousesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    /// Show table or database.
    BlockIO execute() override;

private:
    ASTPtr query_ptr;

    String getRewrittenQuery();
};
}
