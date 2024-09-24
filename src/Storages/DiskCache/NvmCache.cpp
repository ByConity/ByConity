#include <memory>
#include <semaphore>
#include <thread>
#include <utility>

#include <fmt/core.h>

#include <IO/UncompressedCache.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/CacheEngine.h>
#include <Storages/DiskCache/Contexts.h>
#include <Storages/DiskCache/Device.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/JobScheduler.h>
#include <Storages/DiskCache/NvmCache.h>
#include <Storages/DiskCache/RecordIO.h>
#include <Storages/DiskCache/Types.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/PrimaryIndexCache.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <common/StringRef.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <common/scope_guard.h>

namespace ProfileEvents
{
extern const Event NvmCacheLookupCount;
extern const Event NvmCacheLookupSuccCount;
extern const Event NvmCacheInsertCount;
extern const Event NvmCacheInsertSuccCount;
extern const Event NvmCacheRemoveCount;
extern const Event NvmCacheRemoveSuccCount;
extern const Event NvmCacheIoErrorCount;
extern const Event NvmAbortedPutOnInflightGet;
extern const Event NvmGetMissFast;
extern const Event NvmGetCoalesced;
extern const Event NvmGetMiss;
extern const Event NvmGetMissDueToInflightRemove;
extern const Event NvmAbortedPutOnTombstone;
extern const Event NvmPutErrors;
extern const Event NvmPuts;
extern const Event NvmGets;
extern const Event NvmDeletes;
extern const Event NvmSkippedDeletes;
}
namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

using namespace HybridCache;
using PutToken = NvmCache::PutToken;


template <typename T>
Handle NvmCache::find(HashedKey key, DecodeCallback cb, EngineTag tag)
{
    if (!isEnabled())
        return {};

    ProfileEvents::increment(ProfileEvents::NvmGets);
    auto shard = getShardForKey(key);
    inflight_puts[shard].invalidateToken(key.key());

    GetCtx * ctx{nullptr};
    Handle hdl{std::make_shared<WaitContext>()};
    {
        auto lock = getFillLockForShard(shard);
        auto & fill_map = getFillMapForShard(shard);
        auto it = fill_map.find(key.key());
        if (it == fill_map.end() && !put_contexts[shard].hasContexts() && !couldExist(key, tag))
        {
            ProfileEvents::increment(ProfileEvents::NvmGetMiss);
            ProfileEvents::increment(ProfileEvents::NvmGetMissFast);
            return {};
        }

        auto wait_context = hdl.wait_context;

        if (it != fill_map.end())
        {
            ctx = it->second.get();
            ctx->addWaiter(std::move(wait_context));
            ProfileEvents::increment(ProfileEvents::NvmGetCoalesced);
            return hdl;
        }

        auto new_ctx = std::make_unique<GetCtx>(key.key(), std::move(wait_context), std::make_shared<T>());
        auto res = fill_map.emplace(std::make_pair(new_ctx->getKey(), std::move(new_ctx)));
        chassert(res.second);
        ctx = res.first->second.get();
    }

    chassert(ctx);
    auto guard = make_scope_guard([key, this]() { removeFromFillMap(key); });

    lookupAsync(
        HybridCache::HashedKey::precomputed(ctx->getKey(), key.keyHash()),
        [this, ctx, callback = std::move(cb), tag](Status st, HashedKey k, Buffer v) {
            onGetComplete(*ctx, st, k, std::move(v), callback, tag);
        },
        tag);
    guard.dismiss();
    return hdl;
}

template Handle NvmCache::find<UncompressedCacheCell>(HashedKey key, DecodeCallback cb, EngineTag);

void NvmCache::put(HashedKey key, std::shared_ptr<void> item, PutToken token, EncodeCallback cb, EngineTag tag)
{
    if (!isEnabled())
        return;

    ProfileEvents::increment(ProfileEvents::NvmPuts);
    if (hasTombStone(key))
    {
        ProfileEvents::increment(ProfileEvents::NvmAbortedPutOnTombstone);
        return;
    }

    auto shard = getShardForKey(key);
    auto & put_ctxs = put_contexts[shard];
    auto & ctx = put_ctxs.createContext(key.key(), item);
    BufferView buf = cb(item.get());

    auto put_cleanup = [&put_ctxs, &ctx]() { put_ctxs.destroyContext(ctx); };
    auto guard = make_scope_guard([put_cleanup]() { put_cleanup(); });

    const bool executed = token.executeIfValid([&]() {
        auto status = insertAsync(
            HybridCache::HashedKey::precomputed(ctx.getKey(), key.keyHash()),
            buf,
            [this, put_cleanup](Status st, HashedKey) {
                if (st == Status::BadState)
                    disable("Insert Failure. BadState");

                put_cleanup();
            },
            tag);

        if (status == Status::Ok)
            guard.dismiss();
        else
            ProfileEvents::increment(ProfileEvents::NvmPutErrors);
    });

    if (!executed)
        ProfileEvents::increment(ProfileEvents::NvmAbortedPutOnInflightGet);
}

PutToken NvmCache::createPutToken(StringRef key)
{
    return inflight_puts[getShardForKey(key)].tryAcquireToken(key);
}

void NvmCache::onGetComplete(GetCtx & ctx, Status st, HashedKey key, Buffer val, DecodeCallback cb, EngineTag tag)
{
    auto guard = make_scope_guard([this, key]() { removeFromFillMap(key); });
    if (!isEnabled())
        return;

    if (st != Status::Ok)
    {
        if (st != Status::NotFound)
            remove(key, createDeleteTombStoneGuard(key), tag);
        ProfileEvents::increment(ProfileEvents::NvmGetMiss);
        return;
    }

    auto lock = getFillLock(key);
    if (hasTombStone(key) || !ctx.isValid())
    {
        ProfileEvents::increment(ProfileEvents::NvmGetMiss);
        ProfileEvents::increment(ProfileEvents::NvmGetMissDueToInflightRemove);
        return;
    }

    cb(ctx.obj, std::move(val));
}

bool NvmCache::hasTombStone(HashedKey key)
{
    const auto shard = key.keyHash() % kShards;
    return tombstones[shard].isPresent(key.key());
}

void NvmCache::remove(HashedKey key, DeleteTombStoneGuard guard, EngineTag tag)
{
    if (!isEnabled())
        return;
    chassert(guard);
    ProfileEvents::increment(ProfileEvents::NvmDeletes);

    const auto shard = getShardForKey(key);
    inflight_puts[shard].invalidateToken(key.key());

    if (!put_contexts[shard].hasContexts() && !couldExist(key, tag))
    {
        ProfileEvents::increment(ProfileEvents::NvmSkippedDeletes);
        return;
    }

    auto & del_ctxs = del_contexts[shard];
    auto & ctx = del_ctxs.createContext(key.key(), std::move(guard));

    auto del_cleanup = [&del_ctxs, &ctx, this](Status st, HashedKey) mutable {
        del_ctxs.destroyContext(ctx);
        if (st == Status::Ok || st == Status::NotFound)
            return;

        disable(fmt::format("Delete Failure. status = {}", static_cast<int>(st)));
    };

    removeAsync(HashedKey::precomputed(ctx.getKey(), key.keyHash()), del_cleanup, tag);
}

NvmCache::DeleteTombStoneGuard NvmCache::createDeleteTombStoneGuard(HashedKey key)
{
    const auto shard = key.keyHash() % kShards;
    auto guard = tombstones[shard].add(key.key());
    auto lock = getFillLockForShard(shard);

    return guard;
}

void NvmCache::reset()
{
    LOG_INFO(log, "reset nvmcache");
    scheduler->finish();
    for (auto & pair : pairs)
    {
        pair.small_item_cache->reset();
        pair.large_item_cache->reset();
    }
}

void NvmCache::flush()
{
    scheduler->finish();
    for (auto & pair : pairs)
    {
        pair.small_item_cache->flush();
        pair.large_item_cache->flush();
    }
}

void NvmCache::persist() const
{
    auto stream = createMetadataOutputStream(*device, metadata_size);
    if (stream)
    {
        for (const auto & pair : pairs)
        {
            pair.small_item_cache->persist(stream.get());
            pair.large_item_cache->persist(stream.get());
        }
    }
}

bool NvmCache::recover()
{
    auto stream = createMetadataInputStream(*device, metadata_size);
    if (!stream)
        return false;

    bool recovered = true;
    for (const auto & pair : pairs)
    {
        recovered &= pair.small_item_cache->recover(stream.get());
        if (!recovered)
            break;
        recovered &= pair.large_item_cache->recover(stream.get());
        if (!recovered)
            break;
    }

    if (!recovered)
        reset();

    if (recovered)
    {
        auto output_stream = createMetadataOutputStream(*device, metadata_size);
        if (output_stream)
            return output_stream->invalidate();
        else
            recovered = false;
    }
    return recovered;
}

void NvmCache::Pair::validate()
{
    if (small_item_cache != nullptr)
    {
        if (small_item_max_size == 0)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Small item cache is set without a max size");

        if (small_item_max_size > small_item_cache->getMaxItemSize())
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Small item max size should not exceed {} but is set to be {}.",
                small_item_cache->getMaxItemSize(),
                small_item_max_size);
    }

    if (!large_item_cache)
        large_item_cache = std::make_unique<NoopEngine>();
    if (!small_item_cache)
        small_item_cache = std::make_unique<NoopEngine>();
}


typename NvmCache::Config & NvmCache::Config::validate()
{
    if (pairs.empty())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "There should be at least one engine pair.");

    for (auto & p : pairs)
        p.validate();
    if (pairs.size() > 1 && (!selector))
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "More than one engine pairs with no selector.");
    return *this;
}


NvmCache::NvmCache(Config && config) : NvmCache{std::move(config.validate()), ValidConfigTag{}}
{
}


NvmCache::NvmCache(Config && config, ValidConfigTag)
    : max_concurrent_inserts{config.max_concurrent_inserts}
    , max_parcel_memory{config.max_parcel_memory}
    , metadata_size{config.metadata_size}
    , device{std::move(config.device)}
    , scheduler{std::move(config.scheduler)}
    , selector{std::move(config.selector)}
    , pairs{std::move(config.pairs)}
{
    LOG_INFO(log, "Max concurrent inserts: {}", max_concurrent_inserts);
    LOG_INFO(log, "Max parcel memory: {}", max_parcel_memory);
    LOG_INFO(log, "Metadata size: {}", metadata_size);
}


NvmCache::~NvmCache()
{
    try
    {
        LOG_INFO(log, "NvmCache: finish scheduler");
        scheduler->finish();
        LOG_INFO(log, "NvmCache: finish scheduler successful");
        scheduler.reset();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

bool NvmCache::shutDown()
{
    enabled = false;
    try
    {
        flush();
        persist();
    }
    catch (std::exception & ex)
    {
        LOG_ERROR(log, "Got error persist cache: ", ex.what());
        return false;
    }
    LOG_INFO(log, "Cache recovery saved to the Flash Device");
    return true;
}


bool NvmCache::isItemLarge(HashedKey key, BufferView value, EngineTag tag) const
{
    return key.key().size + value.size() > pairs[selectEnginePair(tag)].small_item_max_size;
}


size_t NvmCache::selectEnginePair(EngineTag tag) const
{
    if (selector)
        return selector(tag);
    else
        return 0;
}


bool NvmCache::couldExist(HashedKey key, EngineTag tag)
{
    auto & p = pairs[selectEnginePair(tag)];
    bool could_exist = p.small_item_cache->couldExist(key) || p.large_item_cache->couldExist(key);
    if (!could_exist)
        ProfileEvents::increment(ProfileEvents::NvmCacheLookupCount);
    return could_exist;
}


Status NvmCache::insert(HashedKey key, BufferView value, EngineTag tag)
{
    std::binary_semaphore done{0};
    Status cb_status{Status::Ok};
    auto status = insertAsync(
        key,
        value,
        [&done, &cb_status](Status s, HashedKey) {
            cb_status = s;
            done.release();
        },
        tag);

    if (status != Status::Ok)
        return status;
    done.acquire();
    return cb_status;
}


Status NvmCache::insertInternal(Pair & p, HashedKey key, BufferView value, bool & skip_insertion)
{
    Status status{Status::Ok};
    if (!skip_insertion)
    {
        if (p.isItemLarge(key, value))
            status = p.large_item_cache->insert(key, value);
        else
            status = p.small_item_cache->insert(key, value);
        if (status == Status::Retry)
            return status;
        skip_insertion = true;
    }

    switch (status)
    {
        case Status::Ok:
            ProfileEvents::increment(ProfileEvents::NvmCacheInsertSuccCount);
            break;
        case Status::BadState:
        case Status::DeviceError:
            ProfileEvents::increment(ProfileEvents::NvmCacheIoErrorCount);
            break;
        default:;
    }
    return status;
}


Status NvmCache::insertAsync(HashedKey key, BufferView value, InsertCallback cb, EngineTag tag)
{
    if (key.key().size > kMaxKeySize)
        return Status::Rejected;
    if (!admissionTest(key, value))
        return Status::Rejected;

    auto & p = pairs[selectEnginePair(tag)];
    ProfileEvents::increment(ProfileEvents::NvmCacheInsertCount);
    scheduler->enqueueWithKey(
        [this, cb = std::move(cb), key, value, &p, skip_insertion = false]() mutable {
            auto status = insertInternal(p, key, value, skip_insertion);
            if (status == Status::Retry)
                return JobExitCode::Reschedule;

            if (cb)
                cb(status, key);

            parcel_memory.fetch_sub(key.key().size + value.size());
            concurrent_inserts.fetch_sub(1);

            return JobExitCode::Done;
        },
        "insert",
        JobType::Write,
        key.keyHash());
    return Status::Ok;
}

bool NvmCache::admissionTest(HashedKey key, BufferView value) const
{
    size_t parcel_size = key.key().size + value.size();
    auto curr_parcel_memory = parcel_memory.fetch_add(parcel_size);
    auto curr_concurrent_inserts = concurrent_inserts.fetch_add(1);

    if (curr_concurrent_inserts <= max_concurrent_inserts && curr_parcel_memory <= max_parcel_memory)
        return true;

    concurrent_inserts.fetch_sub(1);
    parcel_memory.fetch_sub(parcel_size);

    return false;
}

void updateLookupStatus(Status status)
{
    switch (status)
    {
        case Status::Ok:
            ProfileEvents::increment(ProfileEvents::NvmCacheLookupSuccCount);
            break;
        case Status::DeviceError:
            ProfileEvents::increment(ProfileEvents::NvmCacheIoErrorCount);
            break;
        default:;
    }
}


Status NvmCache::lookup(HashedKey key, Buffer & value, EngineTag tag)
{
    auto & p = pairs[selectEnginePair(tag)];
    ProfileEvents::increment(ProfileEvents::NvmCacheLookupCount);
    Status status{Status::NotFound};

    while ((status = p.large_item_cache->lookup(key, value)) == Status::Retry)
        std::this_thread::yield();

    if (status == Status::NotFound)
    {
        while ((status = p.small_item_cache->lookup(key, value)) == Status::Retry)
            std::this_thread::yield();
    }

    updateLookupStatus(status);
    return status;
}


Status NvmCache::lookupInternal(Pair & p, HashedKey key, Buffer & value, bool & skip_large_item_cache)
{
    Status status{Status::NotFound};
    if (!skip_large_item_cache)
    {
        status = p.large_item_cache->lookup(key, value);
        if (status == Status::Retry)
            return status;
        skip_large_item_cache = true;
    }
    if (status == Status::NotFound)
    {
        status = p.small_item_cache->lookup(key, value);
        if (status == Status::Retry)
            return status;
    }

    updateLookupStatus(status);
    return status;
}


void NvmCache::lookupAsync(HashedKey key, LookupCallback cb, EngineTag tag)
{
    chassert(cb != nullptr);
    auto & p = pairs[selectEnginePair(tag)];
    scheduler->enqueueWithKey(
        [cb = std::move(cb), key, &p, skip_large_item_cache = false]() mutable {
            Buffer value;
            Status status = lookupInternal(p, key, value, skip_large_item_cache);
            if (status == Status::Retry)
                return JobExitCode::Reschedule;

            if (cb)
                cb(status, key, std::move(value));

            return JobExitCode::Done;
        },
        "lookup",
        JobType::Read,
        key.keyHash());
}


Status NvmCache::removeHashedKeyInternal(Pair & p, HashedKey key, bool & skip_small_item_cache)
{
    ProfileEvents::increment(ProfileEvents::NvmCacheRemoveCount);
    Status status{Status::NotFound};
    if (!skip_small_item_cache)
        status = p.small_item_cache->remove(key);

    if (status == Status::NotFound)
    {
        status = p.large_item_cache->remove(key);
        skip_small_item_cache = true;
    }
    switch (status)
    {
        case Status::Ok:
            ProfileEvents::increment(ProfileEvents::NvmCacheRemoveSuccCount);
            break;
        case Status::DeviceError:
            ProfileEvents::increment(ProfileEvents::NvmCacheIoErrorCount);
            break;
        default:;
    }
    return status;
}


Status NvmCache::remove(HashedKey key, EngineTag tag)
{
    auto & p = pairs[selectEnginePair(tag)];
    Status status{Status::Ok};
    bool skip_small_item_cache = false;
    while ((status = removeHashedKeyInternal(p, key, skip_small_item_cache)) == Status::Retry)
        std::this_thread::yield();

    return status;
}


void NvmCache::removeAsync(HashedKey key, RemoveCallback cb, EngineTag tag)
{
    auto & p = pairs[selectEnginePair(tag)];
    scheduler->enqueueWithKey(
        [cb = std::move(cb), key, &p, skip_small_item_cache = false]() mutable {
            auto status = removeHashedKeyInternal(p, key, skip_small_item_cache);
            if (status == Status::Retry)
                return JobExitCode::Reschedule;

            if (cb)
                cb(status, key);

            return JobExitCode::Done;
        },
        "remove",
        JobType::Write,
        key.keyHash());
}
}
