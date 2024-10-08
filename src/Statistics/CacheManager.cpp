/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <memory>
#include <Statistics/CacheManager.h>
#include <Statistics/CatalogAdaptor.h>
#include <Statistics/Parameters.h>

namespace DB::Statistics
{

std::unique_ptr<CacheManager::CacheType> CacheManager::cache;


void CacheManager::initialize(ContextPtr context)
{
    if (cache)
    {
        LOG_WARNING(getLogger("CacheManager"), "CacheManager already initialized");
        return;
    }
    auto max_size = context->getConfigRef().getUInt64("optimizer.statistics.max_cache_size", ConfigParameters::max_cache_size);

    auto expire_time = std::chrono::seconds(
        context->getConfigRef().getUInt64("optimizer.statistics.cache_expire_time", ConfigParameters::cache_expire_time));
    initialize(max_size, expire_time);
}

void CacheManager::initialize(UInt64 entry_size, std::chrono::seconds expire_time)
{
    (void)entry_size;
    cache = std::make_unique<CacheType>(expire_time);
}

void CacheManager::reset()
{
    cache->clear();
}

void CacheManager::invalidate(ContextPtr context, const StatsTableIdentifier & table)
{
    (void)context;
    // this operation is lightweight:
    //     local, in memory and exception free
    if (cache)
        cache->invalidate(table.getUniqueKey());
}
} // namespace DB::Statistics
