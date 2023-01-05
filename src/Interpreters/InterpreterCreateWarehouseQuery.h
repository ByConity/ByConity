#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTCreateWarehouseQuery.h>


namespace DB
{

/** Creates a logical VW for query and task execution.
  * The physical VW resource will be separately started up, and the workers will register themselves to the Resource Manager
  * Syntax is as follows:
  * CREATE WAREHOUSE [IF NOT EXISTS] vwname
  * [SIZE {XS, S, M, L, XL, XXL}] // Either size or num_workers must be specified
  * [SETTINGS]
  * [auto_suspend = <some number in seconds>]
  * [auto_resume = 0|1,]
  * [max_worker_groups = <num>,]
  * [min_worker_groups = <num>,]
  * [num_workers = <num>,]
  * [max_concurrent_queries = num]
  */
class InterpreterCreateWarehouseQuery : public IInterpreter, WithContext
{
public:
    InterpreterCreateWarehouseQuery(const ASTPtr & query_ptr_, ContextPtr context_);

    /// Create table or database.
    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};
}
