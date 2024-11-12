#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Common/SettingsChanges.h>


namespace DB
{

class ASTSetQuery;

/** Change one or several settings for the session or just for the current context.
  */
class InterpreterSetQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterSetQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    /** Usual SET query. Set setting for the session.
      */
    BlockIO execute() override;

    /** Set setting for current context (query context).
      * It is used for interpretation of SETTINGS clause in SELECT query.
      */
    void executeForCurrentContext();

    /// To apply SETTINGS clauses from query as early as possible
    static void applySettingsFromQuery(const ASTPtr & ast, ContextMutablePtr context);

    /// To extract SETTINGS clauses from query
    static SettingsChanges extractSettingsFromQuery(const ASTPtr & ast, ContextMutablePtr context);

    /// apply ab test profile from query
    static void applyABTestProfile(ContextMutablePtr query_context);

    static void applyPointLookupProfile(ContextMutablePtr query_context);

private:
    ASTPtr query_ptr;
};

}
