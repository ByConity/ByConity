#pragma once

#include <Common/Logger.h>
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

    BlockIO executePartialUpdate(const StoragePtr & storage);
    ASTPtr prepareInsertQueryForPartialUpdate(const StoragePtr & storage, const std::unordered_map<String, ASTPtr> & name_to_expression_map);
    ASTPtr query_ptr;
    LoggerPtr log;
};

}
