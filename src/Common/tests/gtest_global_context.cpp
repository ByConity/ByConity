#include "gtest_global_context.h"

DB::ContextMutablePtr
ContextHolder::createQueryContext(const String & query_id, const std::unordered_map<std::string, DB::Field> & settings) const
{
    auto query_context = DB::Context::createCopy(context);
    query_context->setSessionContext(context);
    query_context->setQueryContext(query_context);
    query_context->setCurrentQueryId(query_id);
    query_context->createPlanNodeIdAllocator();
    query_context->createSymbolAllocator();
    query_context->createOptimizerMetrics();
    for (const auto & item : settings)
        query_context->setSetting(item.first, item.second);
    return query_context;
}
