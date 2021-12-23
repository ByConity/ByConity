#pragma once

#include <Interpreters/Context.h>
#include <Databases/DatabaseMemory.h>

struct ContextHolder
{
    DB::SharedContextHolder shared_context;
    DB::ContextMutablePtr context;

    ContextHolder()
        : shared_context(DB::Context::createShared())
        , context(DB::Context::createGlobal(shared_context.get()))
    {
        context->makeGlobalContext();
        context->setPath("./");
        
        DB::DatabasePtr database = std::make_shared<DB::DatabaseMemory>("test_database", context);
        DB::DatabaseCatalog::instance().attachDatabase("test_database", database);
    }

    ContextHolder(ContextHolder &&) = default;
};

inline const ContextHolder & getContext()
{
    static ContextHolder holder;
    return holder;
}
