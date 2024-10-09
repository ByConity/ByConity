#include <Common/config.h>

#if USE_AWS_S3

#    include <IO/WriteBufferFromS3.h>
#    include <IO/WriteHelpers.h>

#    include <aws/s3/S3Client.h>
#    include <common/logger_useful.h>
#    include <common/scope_guard_safe.h>

#    include <utility>

#define RECORD_S3_OP_TIME(logger, event, extra_msg) \
    Stopwatch s3_op_watch; \
    SCOPE_EXIT_SAFE({ \
        if (std::uncaught_exception()) \
            ProfileEvents::increment(ProfileEvents::WriteBufferFromS3WriteErrors, 1); \
        UInt64 elapsed = s3_op_watch.elapsedMicroseconds(); \
        ProfileEvents::increment(event, elapsed); \
        LOG_TRACE(logger, "S3 operation " #event " takes {} us, Extra: {}", elapsed, extra_msg); \
    })

namespace ProfileEvents
{
    extern const Event WriteBufferFromS3WriteMicroseconds;
    extern const Event WriteBufferFromS3WriteBytes;
    extern const Event WriteBufferFromS3WriteErrors;

    extern const Event S3WriteBytes;
}


namespace DB
{
// S3 protocol does not allow to have multipart upload with more than 10000 parts.
// In case server does not return an error on exceeding that number, we print a warning
// because custom S3 implementation may allow relaxed requirements on that.
const int S3_WARN_MAX_PARTS = 10000;


namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int S3_OBJECT_ALREADY_EXISTS;
}


WriteBufferFromS3::WriteBufferFromS3(
    const std::shared_ptr<Aws::S3::S3Client> & client_ptr_,
    const String & bucket_,
    const String & key_,
    size_t upload_single_part_size_,
    size_t multipart_upload_threshold_,
    std::optional<std::map<String, String>> object_metadata_,
    size_t buffer_size_,
    bool allow_overwrite_,
    bool use_parallel_upload_,
    size_t parallel_upload_pool_size_,
    bool for_disk_s3_)
    : WriteBufferFromFileBase(buffer_size_, nullptr, 0)
    , s3_util(client_ptr_, bucket_, for_disk_s3_)
    , key(key_)
    , object_metadata(std::move(object_metadata_))
    , upload_single_part_size(upload_single_part_size_)
    , multipart_upload_threshold(multipart_upload_threshold_)
    , use_parallel_upload(use_parallel_upload_)
{
    if (!allow_overwrite_ && s3_util.exists(key_))
        throw Exception(ErrorCodes::S3_OBJECT_ALREADY_EXISTS, "Object {} already exists, abort", key_);

    allocateBuffer();

    if (use_parallel_upload)
        upload_pool = std::make_unique<ThreadPool>(parallel_upload_pool_size_, parallel_upload_pool_size_, parallel_upload_pool_size_ * 4);
}

WriteBufferFromS3::~WriteBufferFromS3()
{
    /// TODO: don't rely on dtor to invoke finalize because
    /// - if previous actions on buffer works as normal, it may swallow exception is finalize throws
    /// - if previous actions on buffer throws, we don't need to upload file to s3
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void WriteBufferFromS3::nextImpl()
{
    if (!offset())
        return;

    try
    {
        temporary_buffer->write(working_buffer.begin(), offset());

        ProfileEvents::increment(ProfileEvents::S3WriteBytes, offset());

        last_part_size += offset();

        /// Data size exceeds singlepart upload threshold, need to use multipart upload.
        if (multipart_upload_id.empty() && last_part_size > multipart_upload_threshold)
            createMultipartUpload();

        if (!multipart_upload_id.empty() && last_part_size > upload_single_part_size)
        {
            writePart();
            allocateBuffer();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to write to s3");

        if (!multipart_upload_id.empty())
        {
            abortMultipartUpload();
        }

        throw;
    }
}

void WriteBufferFromS3::allocateBuffer()
{
    if (!temporary_buffer || use_parallel_upload)
    {
        temporary_buffer = Aws::MakeShared<Aws::StringStream>("temporary buffer");
        temporary_buffer->exceptions(std::ios::badbit);
        std::lock_guard lock(mutex);
        part_buffers[part_id.load()] = temporary_buffer;
    }
    // If we don't need upload buffer parallel, just clear it
    else
    {
        temporary_buffer->str("");
        temporary_buffer->clear();
    }

    total_write_size += last_part_size;
    last_part_size = 0;
}

void WriteBufferFromS3::finalizeImpl()
{
    next();

    try
    {
        if (multipart_upload_id.empty())
        {
            makeSinglepartUpload();
        }
        else
        {
            /// Write rest of the data as last part.
            writePart();
            completeMultipartUpload();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to finalize write");

        if (!multipart_upload_id.empty())
        {
            abortMultipartUpload();
        }

        throw;
    }
}

void WriteBufferFromS3::createMultipartUpload()
{
    RECORD_S3_OP_TIME(log, ProfileEvents::WriteBufferFromS3WriteMicroseconds, "Create multipart upload with id " + multipart_upload_id);

    multipart_upload_id = s3_util.createMultipartUpload(key, object_metadata, std::nullopt);
}

void WriteBufferFromS3::writePart()
{
    auto size = temporary_buffer->tellp();

    if (size < 0)
        throw Exception("Failed to write part. Buffer in invalid state.", ErrorCodes::S3_ERROR);

    if (size == 0)
    {
        LOG_DEBUG(log, "Skipping writing part. Buffer is empty.");
        return;
    }

    if (part_tags.size() == S3_WARN_MAX_PARTS)
    {
        // Don't throw exception here by ourselves but leave the decision to take by S3 server.
        LOG_WARNING(log, "Maximum part number in S3 protocol has reached (too many parts). Server may not accept this whole upload.");
    }

    auto upload_task = [&, size = size, cur_part_id = part_id.load()]() {
        String etag;
        RECORD_S3_OP_TIME(
            log,
            ProfileEvents::WriteBufferFromS3WriteMicroseconds,
            "Write part for " + multipart_upload_id + " with size " + std::to_string(size) + ", etag: " + etag);

        std::shared_ptr<Aws::StringStream> cur_buffer;
        if (use_parallel_upload)
        {
            std::lock_guard lock(mutex);
            cur_buffer = part_buffers[cur_part_id];
        }
        else
            cur_buffer = temporary_buffer;

        etag = s3_util.uploadPart(key, multipart_upload_id, cur_part_id, size, cur_buffer);

        ProfileEvents::increment(ProfileEvents::WriteBufferFromS3WriteBytes, size);

        std::lock_guard lock(mutex);
        part_tags[cur_part_id] = etag;
        part_buffers.erase(cur_part_id);
    };

    if (use_parallel_upload)
        upload_pool->scheduleOrThrowOnError(upload_task);
    else
        upload_task();

    part_id++;
}

void WriteBufferFromS3::completeMultipartUpload()
{
    if (use_parallel_upload)
        upload_pool->wait();

    if (part_tags.empty() || multipart_upload_id.empty())
    {
        throw Exception("Failed to complete multipart upload. No parts have uploaded", ErrorCodes::S3_ERROR);
    }

    RECORD_S3_OP_TIME(
        log,
        ProfileEvents::WriteBufferFromS3WriteMicroseconds,
        "Complete multipart upload " + multipart_upload_id);

    std::vector<String> part_etag_list;
    part_etag_list.reserve(part_tags.size());
    for (const auto & [_, etag] : part_tags)
        part_etag_list.emplace_back(etag);

    s3_util.completeMultipartUpload(key, multipart_upload_id, part_etag_list);
}

void WriteBufferFromS3::abortMultipartUpload()
{
    if (multipart_upload_id.empty())
    {
        throw Exception("Trying to abort multi part upload but no multi part has been created", ErrorCodes::LOGICAL_ERROR);
    }

    RECORD_S3_OP_TIME(log, ProfileEvents::WriteBufferFromS3WriteMicroseconds, "Abort multipart upload " + multipart_upload_id);

    s3_util.abortMultipartUpload(key, multipart_upload_id);
}

void WriteBufferFromS3::makeSinglepartUpload()
{
    auto size = temporary_buffer->tellp();

    if (size < 0)
        throw Exception("Failed to make single part upload. Buffer in invalid state", ErrorCodes::S3_ERROR);

    RECORD_S3_OP_TIME(log, ProfileEvents::WriteBufferFromS3WriteMicroseconds, "Put single part of size " + std::to_string(size));

    s3_util.upload(key, size, temporary_buffer, object_metadata, std::nullopt);
    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3WriteBytes, size);
}

}

#endif
