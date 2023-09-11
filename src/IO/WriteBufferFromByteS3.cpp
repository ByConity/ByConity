#include <IO/WriteBufferFromByteS3.h>
#include <IO/WriteHelpers.h>
#include <IO/S3Common.h>
#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <common/logger_useful.h>
#include <common/scope_guard.h>

#include <utility>

#define RECORD_S3_OP_TIME(logger, event, extra_msg) \
    Stopwatch s3_op_watch; \
    SCOPE_EXIT({ \
        UInt64 elapsed = s3_op_watch.elapsedMicroseconds(); \
        ProfileEvents::increment(event, elapsed); \
        LOG_TRACE(logger, fmt::format("S3 operation "#event" takes {} micro seconds, Extra: {}", \
            elapsed, extra_msg)); \
    })

namespace ProfileEvents
{
    extern const Event WriteBufferFromS3Write;
    extern const Event WriteBufferFromS3WriteFailed;
    extern const Event WriteBufferFromS3WriteMicro;
    extern const Event WriteBufferFromS3WriteBytes;
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
    extern const int NOT_IMPLEMENTED;
    extern const int S3_OBJECT_ALREADY_EXISTS;
    extern const int BAD_ARGUMENTS;
}

WriteBufferFromByteS3::WriteBufferFromByteS3(
    const std::shared_ptr<Aws::S3::S3Client>& client_,
    const String& bucket_,
    const String& key_,
    UInt64 max_single_put_threshold_,
    UInt64 min_segment_size_,
    std::optional<std::map<String, String>> object_metadata_,
    size_t buf_size_,
    bool allow_overwrite_,
    char* mem_,
    size_t alignment_)
    : WriteBufferFromFileBase(buf_size_, mem_, alignment_)
    , key(key_)
    , object_metadata(object_metadata_)
    , s3_util(client_, bucket_)
    , max_single_put_threshold(max_single_put_threshold_)
    , min_segment_size(min_segment_size_)
    , temporary_buffer(nullptr)
    , last_part_size(0)
    , total_write_size(0)
    , log(&Poco::Logger::get("WriteBufferFromByteS3"))
{
    if (max_single_put_threshold > min_segment_size)
    {
        throw Exception(fmt::format("Single put size {} must larger than segment {}",
            max_single_put_threshold, min_segment_size), ErrorCodes::BAD_ARGUMENTS);
    }

    if (!allow_overwrite_ && s3_util.exists(key_))
    {
        throw Exception(fmt::format("Object {} already exists, abort", key_),
            ErrorCodes::S3_OBJECT_ALREADY_EXISTS);
    }
    allocateBuffer();
}

WriteBufferFromByteS3::~WriteBufferFromByteS3()
{
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void WriteBufferFromByteS3::nextImpl()
{
    // Skip empty buffer
    if (!offset())
        return;

    try
    {
        temporary_buffer->write(working_buffer.begin(), offset());

        last_part_size += offset();

        /// Data size exceeds singlepart upload threshold, need to use multipart upload.
        if (multipart_upload_id.empty() && last_part_size >= max_single_put_threshold)
        {
            createMultipartUpload();
        }

        if (!multipart_upload_id.empty() && last_part_size > min_segment_size)
        {
            writePart();
            clearBuffer();
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

void WriteBufferFromByteS3::sync()
{
}

void WriteBufferFromByteS3::allocateBuffer()
{
    temporary_buffer = Aws::MakeShared<Aws::StringStream>("temporary buffer");
    temporary_buffer->exceptions(std::ios::badbit);
}

void WriteBufferFromByteS3::clearBuffer()
{
    temporary_buffer->str("");
    temporary_buffer->clear();
    total_write_size += last_part_size;
    last_part_size = 0;
}

void WriteBufferFromByteS3::finalize()
{
    /// FIXME(wsy) move final flush into the caller
    /// otherwise we may throw in dtor
    MemoryTracker::LockExceptionInThread lock(VariableContext::Global);
    if (finalized)
        return;

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

        finalized = true;
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

void WriteBufferFromByteS3::createMultipartUpload()
{
    RECORD_S3_OP_TIME(log, ProfileEvents::WriteBufferFromS3WriteMicro,
        "Create multipart upload with id " + multipart_upload_id);
    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3Write, 1);

    try
    {
        multipart_upload_id = s3_util.createMultipartUpload(key, object_metadata);
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::WriteBufferFromS3WriteFailed, 1);
        throw;
    }
}

void WriteBufferFromByteS3::writePart()
{
    auto size = temporary_buffer->tellp();
    if (size < 0)
    {
        throw Exception("Failed to write part. Buffer in invalid state.", ErrorCodes::S3_ERROR);
    }
    if (size == 0)
    {
        LOG_TRACE(log, "Skipping writing part. Buffer is empty.");
        return;
    }

    if (part_tags.size() == S3_WARN_MAX_PARTS)
    {
        // Don't throw exception here by ourselves but leave the decision to take by S3 server.
        LOG_WARNING(log, "Maximum part number in S3 protocol has reached (too many parts). Server may not accept this whole upload.");
    }

    String tag;
    RECORD_S3_OP_TIME(log, ProfileEvents::WriteBufferFromS3WriteMicro,
        "Write part for " + multipart_upload_id + " with size " + std::to_string(size) + ", tag: " + tag);
    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3Write, 1);
    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3WriteBytes, size);
    try
    {
        tag = s3_util.uploadPart(key, multipart_upload_id, part_tags.size() + 1,
            size, temporary_buffer);
        part_tags.push_back(tag);
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::WriteBufferFromS3WriteFailed, 1);
        throw;
    }
}

void WriteBufferFromByteS3::completeMultipartUpload()
{
    if (part_tags.empty() || multipart_upload_id.empty())
    {
        throw Exception("Failed to complete multipart upload. No parts have uploaded",
            ErrorCodes::S3_ERROR);
    }

    RECORD_S3_OP_TIME(log, ProfileEvents::WriteBufferFromS3WriteMicro,
        "Complete multipart upload " + multipart_upload_id);
    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3Write, 1);
    try
    {
        s3_util.completeMultipartUpload(key, multipart_upload_id,
            part_tags);
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::WriteBufferFromS3WriteFailed, 1);
        throw;
    }
}

void WriteBufferFromByteS3::abortMultipartUpload()
{
    if (multipart_upload_id.empty())
    {
        throw Exception("Trying to abort multi part upload but no multi part has been created",
            ErrorCodes::LOGICAL_ERROR);
    }

    RECORD_S3_OP_TIME(log, ProfileEvents::WriteBufferFromS3WriteMicro,
        "Abort multi part upload " + multipart_upload_id);
    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3Write, 1);

    try
    {
        s3_util.abortMultipartUpload(key, multipart_upload_id);
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::WriteBufferFromS3WriteFailed, 1);
        throw;
    }
}

void WriteBufferFromByteS3::makeSinglepartUpload()
{
    auto size = temporary_buffer->tellp();

    if (size < 0)
    {
        throw Exception("Failed to make single part upload. Buffer in invalid state", ErrorCodes::S3_ERROR);
    }

    RECORD_S3_OP_TIME(log, ProfileEvents::WriteBufferFromS3WriteMicro,
        "Write single part of size " + std::to_string(size));
    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3Write, 1);
    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3WriteBytes, size);

    try
    {
        s3_util.upload(key, size, temporary_buffer, object_metadata);
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::WriteBufferFromS3WriteFailed, 1);
        throw;
    }
}

}
