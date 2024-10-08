#pragma once

#include <memory>
#include <Backups/BackupsWorker.h>
#include <Core/Types.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Poco/Logger.h>

namespace DB
{
using BackupsWorkerPtr = std::shared_ptr<BackupsWorker>;

class InterpreterBackupQuery : public IInterpreter
{
public:
    InterpreterBackupQuery(ASTPtr & query_ptr_, ContextMutablePtr context_)
        : query_ptr(query_ptr_), context(context_), logger(getLogger("InterpreterBackupQuery"))
    {
    }

    // Before executing, do some check in case some useless work.
    void check();

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    ContextMutablePtr context;
    LoggerPtr logger;
};
}
