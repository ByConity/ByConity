#include <Catalog/CatalogUtils.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>
#include <algorithm>

namespace DB::Catalog
{

size_t getMaxThreads()
{
    constexpr size_t MAX_THREADS = 16;
    size_t max_threads = 1;
    if (auto thread_group = CurrentThread::getGroup())
    {
        auto context = thread_group->query_context.lock();
        if (context && context->getSettingsRef().catalog_enable_multiple_threads)
            max_threads = context->getSettingsRef().max_threads;
    }
    return std::min(max_threads, MAX_THREADS);
}

size_t getMinParts()
{
    constexpr size_t DEFAULT_MIN_PARTS = 10000;
    if (auto thread_group = CurrentThread::getGroup())
    {
        auto context = thread_group->query_context.lock();
        if (context)
            return context->getSettingsRef().catalog_multiple_threads_min_parts;
    }
    return DEFAULT_MIN_PARTS;
}

}
