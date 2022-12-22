#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTDropWarehouseQuery.h>


namespace DB
{
class Context;

/** Drops a logical virtual warehouse.
  * Syntax is as follows:
  * DROP WAREHOUSE vwname
  */
class InterpreterDropWarehouseQuery : public IInterpreter, WithContext
{
public:
    InterpreterDropWarehouseQuery(const ASTPtr & query_ptr_, ContextPtr context_);

    /// Drop table or database.
    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};
}
