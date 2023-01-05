#pragma once

#include <Databases/IDatabase.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class Context;
using DatabaseAndTable = std::pair<DatabasePtr, StoragePtr>;

class InterpreterUndropQuery : public IInterpreter, WithContext
{
public:
    InterpreterUndropQuery(const ASTPtr & query_ptr_, ContextPtr context_);

    /// Drop table or database.
    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
