#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTAlterWarehouseQuery.h>


namespace DB
{

/** Alters a logical virtual warehouse's settings.
  * Altering of resource behaviour (e.g. Num of workers) will not be performed by CNCH,
  * and serves more as collected information for CNCH
  * Syntax is as follows:
  * ALTER WAREHOUSE  vwname
  * [RENAME TO some_name]
  * [SIZE {XS, S, M, L, XL, XXL}]
  * [SETTINGS]
  * [auto_suspend = <some number in seconds>]
  * [auto_resume = 0|1,]
  * [max_worker_groups = <num>,]
  * [min_worker_groups = <num>,]
  * [num_workers = <num>,]
  * [max_concurrent_queries = num]
  */
class InterpreterAlterWarehouseQuery : public IInterpreter, WithContext
{
public:
    InterpreterAlterWarehouseQuery(const ASTPtr & query_ptr_, ContextPtr context_);

    /// Alter table or database.
    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};
}
