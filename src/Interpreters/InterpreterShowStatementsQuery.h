#pragma once


#include <DataStreams/BlockIO.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Core/NamesAndTypes.h>
#include "Interpreters/Context_fwd.h"


namespace DB
{
/** Show all statements (dmls) in current interactive transaction session and its status
 *┌─────────────txn_id─┬─statement──────────────────────────────────────────┬─status───┐
 *│ 427688978462015491 │ INSERT INTO test.test11 SELECT * FROM test.test10  │ Finished │
 *│ 427688988056748043 │ INSERT INTO test.test11 SELECT * FROM test.test20  │ Aborted  │
 *└────────────────────┴────────────────────────────────────────────────────┴──────────┘
 */
class InterpreterShowStatementsQuery : public IInterpreter, WithContext
{
public:
    InterpreterShowStatementsQuery(const ASTPtr & query_ptr_, ContextPtr context_):
        WithContext(std::move(context_)), query_ptr(query_ptr_) {}
    static NamesAndTypes getBlockStructure();
    BlockIO execute() override;
private:
    ASTPtr query_ptr;
};

}
