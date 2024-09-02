#include <Catalog/CatalogUtils.h>
#include <Catalog/MetastoreProxy.h>
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

bool parseTxnIdFromUndoBufferKey(const String & key, UInt64 & txn_id)
{
    static const String ub_prefix{UNDO_BUFFER_PREFIX};

    auto pos = key.find(ub_prefix);
    if (pos == std::string::npos || pos + ub_prefix.size() > key.size())
    {
        return false;
    }
    auto under_score_pos = key.find('_', pos + ub_prefix.size());
    if (under_score_pos == std::string::npos)
    {
        return false;
    }
    String txn_id_str;
    if (key.at(under_score_pos - 1) == 'R') /// Reversed transaction id
    {
        txn_id_str = key.substr(pos + ub_prefix.size(), under_score_pos - 1 - pos - ub_prefix.size());
        std::reverse(txn_id_str.begin(), txn_id_str.end());
    }
    else
    {
        txn_id_str = key.substr(pos + ub_prefix.size(), under_score_pos - pos - ub_prefix.size());
    }
    txn_id = std::stoull(txn_id_str);
    return true;
}

}
