#include <common/logger_useful.h>
#include <Processors/IntermediateResult/CacheManager.h>

namespace ProfileEvents
{
    extern const Event IntermediateResultCacheHits;
    extern const Event IntermediateResultCacheMisses;
    extern const Event IntermediateResultCacheRefuses;
    extern const Event IntermediateResultCacheWait;
    extern const Event IntermediateResultCacheUncompleted;
}

namespace DB::IntermediateResult
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

CacheManager::CacheManager(size_t max_size_in_bytes)
    : cache(std::make_shared<Cache>(max_size_in_bytes)), log(getLogger("CacheManager"))
{
}

template <typename Type>
void CacheManager::validateIntermediateCache(
    CacheHolderPtr cache_holder,
    const ContextPtr & context,
    const String & digest,
    const String & full_table_name,
    bool has_unique_key,
    UInt64 last_modified_timestamp,
    time_t now,
    const String & name,
    const Type & element,
    std::vector<Type> & new_vectors)
{
    auto wait_time = context->getSettingsRef().wait_intermediate_result_cache.totalSeconds();
    UInt64 time = 0;
    // unique table's last_modified_timestamp is TxnTimestamp, see TxnTimestamp::toMillisecond
    if (has_unique_key)
        time = (last_modified_timestamp >> 18) / 1000;
    else
        time = last_modified_timestamp;
    if (wait_time && time + wait_time > static_cast<UInt64>(now))
    {
        new_vectors.emplace_back(std::move(element));
        ProfileEvents::increment(ProfileEvents::IntermediateResultCacheWait);
        return;
    }

    OwnerInfo owner_info{name, static_cast<time_t>(last_modified_timestamp)};
    CacheKey key{digest, full_table_name, owner_info};
    auto value = getCache(key);
    if (value != nullptr)
    {
        cache_holder->read_cache[key] = value->clone();
        ProfileEvents::increment(ProfileEvents::IntermediateResultCacheHits);
    }
    else
    {
        KeyState state = KeyState::Missed;
        auto exists = [&](Container::value_type & v) { state = v.second->state; };
        auto emplace = [&](const Container::constructor & ctor) {
            auto value_with_state = std::make_shared<ValueWithState>();
            value_with_state->value = std::make_shared<CacheValue>();
            ctor(key, value_with_state);
        };
        uncompleted_cache.lazy_emplace_l(key, exists, emplace);

        switch (state)
        {
            case KeyState::Missed: {
                new_vectors.emplace_back(std::move(element));
                cache_holder->write_cache.emplace(key);
                ProfileEvents::increment(ProfileEvents::IntermediateResultCacheMisses);
                break;
            }
            case KeyState::Uncompleted: {
                new_vectors.emplace_back(std::move(element));
                ProfileEvents::increment(ProfileEvents::IntermediateResultCacheUncompleted);
                break;
            }
            case KeyState::Refused: {
                new_vectors.emplace_back(std::move(element));
                ProfileEvents::increment(ProfileEvents::IntermediateResultCacheRefuses);
                break;
            }
            default:
                break;
        }
    }
}


CacheHolderPtr CacheManager::createCacheHolder(
    const ContextPtr & context,
    const String & digest,
    const StorageID & storage_id,
    const RangesInDataParts & parts_with_ranges,
    RangesInDataParts & new_parts_with_ranges)
{
    auto part_cache_holder = std::make_shared<CacheHolder>();
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    auto storage = DatabaseCatalog::instance().tryGetTable(storage_id, context);
    UInt64 last_modified_timestamp = 0;
    // tpcds1.item_449936491680890882 -> tpcds1.item
    auto table_name =storage_id.getFullTableName().substr(0,storage_id.getFullTableName().rfind("_"));

    for (auto & part_with_ranges : parts_with_ranges)
    {
        auto & data_part = part_with_ranges.data_part;

        if (storage->getInMemoryMetadataPtr()->hasUniqueKey())
            last_modified_timestamp = data_part->getDeleteBitmapVersion();
        else
            last_modified_timestamp = data_part->commit_time.toSecond();

        validateIntermediateCache<RangesInDataPart>(
            part_cache_holder,
            context,
            digest,
            table_name,
            storage->getInMemoryMetadataPtr()->hasUniqueKey(),
            last_modified_timestamp,
            now,
            data_part->name,
            part_with_ranges,
            new_parts_with_ranges);
    }

    part_cache_holder->all_part_in_cache = new_parts_with_ranges.empty();
    // It may be better to use proportional control here, add a configuration?
    // But now a merging agg will be added after the cache oper, so in the worst case, the performance will not be particularly bad
    part_cache_holder->all_part_in_storage = part_cache_holder->write_cache.empty() && new_parts_with_ranges.size() == parts_with_ranges.size();

    return part_cache_holder;
}

CacheHolderPtr CacheManager::createCacheHolder(
    const ContextPtr & context,
    const String & digest,
    const StorageID & storage_id,
    const HiveFiles & hive_files,
    HiveFiles & new_hive_files)
{
    auto hive_file_cache_holder = std::make_shared<CacheHolder>();
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    auto storage = DatabaseCatalog::instance().tryGetTable(storage_id, context);

    for (auto & file : hive_files)
    {
        validateIntermediateCache<HiveFilePtr>(
            hive_file_cache_holder,
            context,
            digest,
            storage_id.getFullTableName(),
            false,
            file->getLastModifiedTimestamp(),
            now,
            file->file_path,
            file,
            new_hive_files);
    }

    hive_file_cache_holder->all_part_in_cache = new_hive_files.empty();
    // It may be better to use proportional control here, add a configuration?
    // But now a merging agg will be added after the cache oper, so in the worst case, the performance will not be particularly bad
    hive_file_cache_holder->all_part_in_storage = hive_file_cache_holder->write_cache.empty() && new_hive_files.size() == hive_files.size();

    return hive_file_cache_holder;
}

void CacheManager::setComplete(const CacheKey & key)
{
    auto value = tryGetUncompletedCache(key);
    if (!value)
    {
        LOG_TRACE(log, "Skip Write cache {}", key.toString());
        return;
    }

    if (value->chunks.size() == 0)
    {
        auto empty_key = key.cloneWithoutOwnerInfo();
        value = tryGetUncompletedCache(empty_key);
        if (!value)
            modifyKeyStateToRefused(key);
    }
    if (value)
    {
        setCache(key, value);
        eraseUncompletedCache(key);
        LOG_TRACE(
            log,
            "Write cache {} finish, the memory usage is (chunk, rows, KB)->({}, {}, {})",
            key.toString(),
            value->chunks.size(),
            value->getRows(),
            value->getBytes() / static_cast<double>(1024));
    }
}

}
