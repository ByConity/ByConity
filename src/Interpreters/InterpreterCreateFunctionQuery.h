#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTCreateFunctionQuery.h>

namespace DB
{

class Context;

class InterpreterCreateFunctionQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateFunctionQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_, bool is_internal_)
        : WithMutableContext(context_), query_ptr(query_ptr_), is_internal(is_internal_), log{&Poco::Logger::get("InterpreterCreateFunctionQuery")}
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    bool is_internal;

    size_t generateChecksum(const String &value);

    BlockIO executeForHostServer(const ASTCreateFunctionQuery &create_function_query);

    BlockIO executeForNonHostServer(const ASTCreateFunctionQuery &create_function_query);

    Poco::Logger * log;
};

}
