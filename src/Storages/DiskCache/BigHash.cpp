#include <Storages/DiskCache/BigHash.h>

#include <atomic>
#include <chrono>
#include <exception>
#include <istream>
#include <memory>
#include <ostream>
#include <random>
#include <shared_mutex>
#include <utility>
#include <sys/param.h>

#include <fmt/core.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>

#include <IO/ReadBuffer.h>
#include <Protos/disk_cache.pb.h>
#include <Storages/DiskCache/Bucket.h>
#include <Storages/DiskCache/Buffer.h>
#include <Storages/DiskCache/HashKey.h>
#include <Storages/DiskCache/Types.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/SharedMutex.h>
#include <Common/thread_local_rng.h>
#include <common/chrono_io.h>
#include <common/logger_useful.h>
#include <common/types.h>

namespace ProfileEvents
{
extern const Event BigHashEvictionCount;
extern const Event BigHashEvictionExpiredCount;
extern const Event BigHashLogicalWrittenCount;
extern const Event BigHashPhysicalWrittenCount;
extern const Event BigHashInsertCount;
extern const Event BigHashSuccInsertCount;
extern const Event BigHashRemoveCount;
extern const Event BigHashSuccRemoveCount;
extern const Event BigHashLookupCount;
extern const Event BigHashSuccLookupCount;
extern const Event BigHashIOErrorCount;
extern const Event BigHashBFFalsePositiveCount;
extern const Event BigHashBFProbCount;
extern const Event BigHashBFRejectCount;
}

namespace CurrentMetrics
{
extern const Metric BigHashItemCount;
extern const Metric BigHashUsedSizeBytes;
}

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INVALID_CONFIG_PARAMETER;
}

namespace DB::HybridCache
{
BigHash::Config & BigHash::Config::validate()
{
    if (cache_size < bucket_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cache_size: {} cannot be smaller than bucket_size: {}", cache_size, bucket_size);

    if (!powerof2(bucket_size))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "invalid bucket_size: {}", bucket_size);

    if (cache_size > UInt64{bucket_size} << 32)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Can't address big hash with 32 bits. cache_size: {}, bucket_size: {}", cache_size, bucket_size);

    if (cache_start_offset % bucket_size != 0 || cache_size % bucket_size != 0)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "cache_start_offset and cache size need to be multiple of bucket_size. cache_start_offset: {}, cache_size: {}, "
            "bucket_size: {}",
            cache_start_offset,
            cache_size,
            bucket_size);
    }

    if (bloom_filters && bloom_filters->numFilters() != numBuckets())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            fmt::format("bloom filter #filters mismatch #bucket: {} vs {}", bloom_filters->numFilters(), numBuckets()));

    return *this;
}

BigHash::BigHash(Config && config) : BigHash{std::move(config.validate()), ValidConfigTag{}}
{
}

BigHash::BigHash(Config && config, ValidConfigTag)
    : check_expired(std::move(config.check_expired))
    , destructor_callback{[callback = std::move(config.destructor_callback)](HashedKey key, BufferView value, DestructorEvent event) {
        if (callback)
            callback(key, value, event);
    }}
    , bucket_size(config.bucket_size)
    , cache_base_offset(config.cache_start_offset)
    , num_buckets(config.numBuckets())
    , bloom_filters{std::move(config.bloom_filters)}
    , device{*config.device}
{
    mutex = std::make_unique<SharedMutex[]>(kNumMutexes);
    LOG_INFO(
        log, fmt::format("BigHash created: buckets: {}, bucket size: {}, base offset: {}", num_buckets, bucket_size, cache_base_offset));
    reset();
}

void BigHash::reset()
{
    LOG_INFO(log, "Reset BigHash");
    generation_time = getSteadyClock();

    if (bloom_filters)
        bloom_filters->reset();

    CurrentMetrics::set(CurrentMetrics::BigHashItemCount, 0);
    CurrentMetrics::set(CurrentMetrics::BigHashUsedSizeBytes, 0);
}

UInt64 BigHash::getMaxItemSize() const
{
    auto item_overhead = BucketStorage::slotSize(sizeof(BucketEntry));
    return bucket_size - sizeof(Bucket) - item_overhead;
}

std::pair<Status, std::string> BigHash::getRandomAlloc(Buffer & value)
{
    auto dist = std::uniform_int_distribution<UInt64>(0, num_buckets - 1);
    BucketId bucket_id(dist(thread_local_rng));

    Bucket * bucket{nullptr};
    Buffer buffer;
    {
        std::unique_lock<SharedMutex> lock{getMutex(bucket_id)};
        buffer = readBucket(bucket_id);
        if (buffer.isNull())
        {
            ProfileEvents::increment(ProfileEvents::BigHashIOErrorCount);
            return std::make_pair(Status::NotFound, "");
        }

        bucket = reinterpret_cast<Bucket *>(buffer.data());
    }

    auto [key, value_view] = bucket->getRandomAlloc();
    if (key.empty() || value_view.isNull())
        return std::make_pair(Status::NotFound, "");

    value = Buffer{value_view};
    return std::make_pair(Status::Ok, key);
}

void BigHash::persist(google::protobuf::io::ZeroCopyOutputStream * stream)
{
    LOG_INFO(log, "Starting bighash persist");
    Protos::BigHashPersistentData pb;
    pb.set_format_version(kFormatVersion);
    pb.set_generation_time(generation_time.count());
    pb.set_item_count(CurrentMetrics::values[CurrentMetrics::BigHashItemCount].load(std::memory_order_relaxed));
    pb.set_bucket_size(bucket_size);
    pb.set_cache_base_offset(cache_base_offset);
    pb.set_num_buckets(num_buckets);
    pb.set_used_size_bytes(CurrentMetrics::values[CurrentMetrics::BigHashUsedSizeBytes].load(std::memory_order_relaxed));
    google::protobuf::io::CodedOutputStream ostream(stream);
    google::protobuf::util::SerializeDelimitedToCodedStream(pb, &ostream);

    if (bloom_filters)
    {
        bloom_filters->persist(&ostream);
        LOG_INFO(log, "Bloom filter persist done");
    }
    LOG_INFO(log, "Finished bighash persist");
}

bool BigHash::recover(google::protobuf::io::ZeroCopyInputStream * stream)
{
    LOG_INFO(log, "Starting bighash recovery");
    try
    {
        Protos::BigHashPersistentData pb;
        google::protobuf::io::CodedInputStream istream(stream);
        google::protobuf::util::ParseDelimitedFromCodedStream(&pb, &istream, nullptr);
        if (pb.format_version() != kFormatVersion)
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER, "Invalid format version {}, expected {}", pb.format_version(), kFormatVersion);

        auto config_validate
            = pb.bucket_size() == bucket_size && pb.cache_base_offset() == cache_base_offset && pb.num_buckets() == num_buckets;
        if (!config_validate)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Recovery config {}", pb.DebugString());

        generation_time = std::chrono::nanoseconds{pb.generation_time()};
        CurrentMetrics::set(CurrentMetrics::BigHashItemCount, pb.item_count());
        CurrentMetrics::set(CurrentMetrics::BigHashUsedSizeBytes, pb.used_size_bytes());
        if (bloom_filters)
        {
            bloom_filters->recover(&istream);
            LOG_INFO(log, "Recovered bloom filter");
        }
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "Failed to recover BigHash, reset it. Exception: {}", e.what());
        reset();
        return false;
    }

    LOG_INFO(log, "Finished bighash recovery");
    return true;
}

Status BigHash::insert(HashedKey key, BufferView value)
{
    const auto bucket_id = getBucketId(key);
    ProfileEvents::increment(ProfileEvents::BigHashInsertCount);

    UInt32 removed{0};
    UInt32 evicted{0};
    UInt32 evict_expired{0};

    UInt32 old_remaining_bytes = 0;
    UInt32 new_remaining_bytes = 0;

    std::vector<std::tuple<Buffer, Buffer, DestructorEvent>> removed_items;
    DestructorCallback callback = [&removed_items](HashedKey hash_key, BufferView val, DestructorEvent event) {
        removed_items.emplace_back(Buffer{makeView(hash_key.key())}, val, event);
    };

    {
        std::unique_lock<SharedMutex> lock{getMutex(bucket_id)};
        auto buffer = readBucket(bucket_id);
        if (buffer.isNull())
        {
            ProfileEvents::increment(ProfileEvents::BigHashIOErrorCount);
            return Status::DeviceError;
        }

        auto * bucket = reinterpret_cast<Bucket *>(buffer.data());
        old_remaining_bytes = bucket->remainingBytes();
        removed = bucket->remove(key, callback);
        std::tie(evicted, evict_expired) = bucket->insert(key, value, check_expired, callback);
        new_remaining_bytes = bucket->remainingBytes();

        if (bloom_filters)
        {
            if (removed + evicted == 0)
                bloom_filters->set(bucket_id.toUnderType(), key.keyHash());
            else
                bfRebuild(bucket_id, bucket);
        }

        const auto res = writeBucket(bucket_id, std::move(buffer));
        if (!res)
        {
            if (bloom_filters)
                bloom_filters->clear(bucket_id.toUnderType());
            ProfileEvents::increment(ProfileEvents::BigHashIOErrorCount);
            return Status::DeviceError;
        }
    }

    for (const auto & item : removed_items)
        destructor_callback(makeHashKey(std::get<0>(item)), std::get<1>(item).view(), std::get<2>(item));

    if (old_remaining_bytes < new_remaining_bytes)
        CurrentMetrics::sub(CurrentMetrics::BigHashUsedSizeBytes, new_remaining_bytes - old_remaining_bytes);
    else
        CurrentMetrics::add(CurrentMetrics::BigHashUsedSizeBytes, old_remaining_bytes - new_remaining_bytes);

    CurrentMetrics::add(CurrentMetrics::BigHashItemCount);
    CurrentMetrics::sub(CurrentMetrics::BigHashItemCount, evicted + removed);

    ProfileEvents::increment(ProfileEvents::BigHashEvictionCount, evicted);
    ProfileEvents::increment(ProfileEvents::BigHashEvictionExpiredCount, evict_expired);
    ProfileEvents::increment(ProfileEvents::BigHashLogicalWrittenCount, (key.key().size + value.size()));
    ProfileEvents::increment(ProfileEvents::BigHashPhysicalWrittenCount, bucket_size);
    ProfileEvents::increment(ProfileEvents::BigHashSuccInsertCount);

    return Status::Ok;
}

bool BigHash::couldExist(HashedKey key)
{
    const auto bucket_id = getBucketId(key);
    bool can_exist;
    {
        std::shared_lock<SharedMutex> lock{getMutex(bucket_id)};
        can_exist = !bfReject(bucket_id, key.keyHash());
    }

    if (!can_exist)
        ProfileEvents::increment(ProfileEvents::BigHashLookupCount);

    return can_exist;
}

UInt64 BigHash::estimateWriteSize(HashedKey, BufferView) const
{
    return bucket_size;
}

Status BigHash::lookup(HashedKey key, Buffer & value)
{
    const auto bucket_id = getBucketId(key);
    ProfileEvents::increment(ProfileEvents::BigHashLookupCount);

    Bucket * bucket{nullptr};
    Buffer buffer;

    {
        std::shared_lock<SharedMutex> lock{getMutex(bucket_id)};

        if (bfReject(bucket_id, key.keyHash()))
            return Status::NotFound;

        buffer = readBucket(bucket_id);
        if (buffer.isNull())
        {
            ProfileEvents::increment(ProfileEvents::BigHashIOErrorCount);
            return Status::DeviceError;
        }

        bucket = reinterpret_cast<Bucket *>(buffer.data());
    }

    auto value_view = bucket->find(key);
    if (value_view.isNull())
    {
        ProfileEvents::increment(ProfileEvents::BigHashBFFalsePositiveCount);
        return Status::NotFound;
    }
    value = Buffer{value_view};
    ProfileEvents::increment(ProfileEvents::BigHashSuccLookupCount);
    return Status::Ok;
}

Status BigHash::remove(HashedKey key)
{
    const auto bucket_id = getBucketId(key);
    ProfileEvents::increment(ProfileEvents::BigHashRemoveCount);

    UInt32 old_remaining_bytes = 0;
    UInt32 new_remaining_bytes = 0;

    Buffer value_copy;
    DestructorCallback callback = [&value_copy](HashedKey, BufferView value, DestructorEvent) { value_copy = Buffer{value}; };

    {
        std::unique_lock<SharedMutex> lock{getMutex(bucket_id)};
        if (bfReject(bucket_id, key.keyHash()))
        {
            return Status::NotFound;
        }

        auto buffer = readBucket(bucket_id);
        if (buffer.isNull())
        {
            ProfileEvents::increment(ProfileEvents::BigHashIOErrorCount);
            return Status::DeviceError;
        }

        auto * bucket = reinterpret_cast<Bucket *>(buffer.data());
        old_remaining_bytes = bucket->remainingBytes();
        if (!bucket->remove(key, callback))
        {
            ProfileEvents::increment(ProfileEvents::BigHashBFFalsePositiveCount);
            return Status::NotFound;
        }
        new_remaining_bytes = bucket->remainingBytes();

        if (bloom_filters)
            bfRebuild(bucket_id, bucket);

        const auto res = writeBucket(bucket_id, std::move(buffer));
        if (!res)
        {
            if (bloom_filters)
                bloom_filters->clear(bucket_id.toUnderType());
            ProfileEvents::increment(ProfileEvents::BigHashIOErrorCount);
            return Status::DeviceError;
        }
    }

    if (!value_copy.isNull())
        destructor_callback(key, value_copy.view(), DestructorEvent::Removed);

    chassert(old_remaining_bytes <= new_remaining_bytes);
    CurrentMetrics::sub(CurrentMetrics::BigHashUsedSizeBytes, new_remaining_bytes - old_remaining_bytes);
    CurrentMetrics::sub(CurrentMetrics::BigHashItemCount);

    ProfileEvents::increment(ProfileEvents::BigHashPhysicalWrittenCount, bucket_size);
    ProfileEvents::increment(ProfileEvents::BigHashSuccRemoveCount);

    return Status::Ok;
}

bool BigHash::bfReject(BucketId bucket_id, const UInt64 key_hash) const
{
    if (bloom_filters)
    {
        ProfileEvents::increment(ProfileEvents::BigHashBFProbCount);
        if (!bloom_filters->couldExist(bucket_id.toUnderType(), key_hash))
        {
            ProfileEvents::increment(ProfileEvents::BigHashBFRejectCount);
            return true;
        }
    }
    return false;
}

void BigHash::bfRebuild(BucketId bucket_id, const Bucket * bucket)
{
    chassert(bloom_filters);
    bloom_filters->clear(bucket_id.toUnderType());
    auto iter = bucket->getFirst();
    while (!iter.done())
    {
        bloom_filters->set(bucket_id.toUnderType(), iter.keyHash());
        iter = bucket->getNext(iter);
    }
}

void BigHash::flush()
{
    LOG_INFO(log, "Flush bighash");
    device.flush();
}

Buffer BigHash::readBucket(BucketId bucket_id)
{
    auto buffer = device.makeIOBuffer(bucket_size);
    chassert(!buffer.isNull());

    const bool res = device.read(getBucketOffset(bucket_id), buffer.size(), buffer.data());
    if (!res)
        return {};

    auto * bucket = reinterpret_cast<Bucket *>(buffer.data());

    const auto checksum_success = Bucket::computeChecksum(buffer.view()) == bucket->getChecksum();
    if (!checksum_success || static_cast<UInt64>(generation_time.count()) != bucket->generationTime())
        Bucket::initNew(buffer.mutableView(), generation_time.count());

    return buffer;
}

bool BigHash::writeBucket(BucketId bucket_id, Buffer buffer)
{
    auto * bucket = reinterpret_cast<Bucket *>(buffer.data());
    bucket->setChecksum(Bucket::computeChecksum(buffer.view()));
    return device.write(getBucketOffset(bucket_id), std::move(buffer));
}
}
