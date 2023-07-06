#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class InterpreterUpdateQuery : public IInterpreter, WithContext
{
public:
    InterpreterUpdateQuery(const ASTPtr & query_ptr_, ContextPtr context_);

    BlockIO execute() override;

private:
    ASTPtr prepareInterpreterSelectQuery(const StoragePtr & storage);
    ASTPtr transformToInterpreterInsertQuery(const StoragePtr & storage);
    ASTPtr query_ptr;
    Poco::Logger * log;
};

}
