#pragma once

#include <atomic>
#include <mutex>
#include <unordered_map>
#include <IO/S3Common.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Poco/URI.h>
#include <Common/Logger.h>
#include <Common/ThreadPool.h>

#if USE_AWS_S3

#include <memory>
#    include <common/logger_useful.h>
#    include <common/types.h>

#    include <IO/BufferWithOwnMemory.h>
#    include <IO/WriteBuffer.h>

#    include <aws/core/utils/memory/stl/AWSStringStream.h> // Y_IGNORE

namespace Aws::S3
{
class S3Client;
}

namespace DB
{

/**
 * Buffer to write a data to a S3 object with specified bucket and key.
 * If data size written to the buffer is less than 'max_single_part_upload_size' write is performed using singlepart upload.
 * In another case multipart upload is used:
 * Data is divided on chunks with size greater than 'minimum_upload_part_size'. Last chunk can be less than this threshold.
 * Each chunk is written as a part to S3.
 */
class WriteBufferFromS3 : public WriteBufferFromFileBase
{
private:
    S3::S3Util s3_util;
    String key;
    std::optional<std::map<String, String>> object_metadata;

    size_t upload_single_part_size;
    size_t multipart_upload_threshold;

    /// Buffer to accumulate data.
    std::shared_ptr<Aws::StringStream> temporary_buffer;
    size_t last_part_size;
    size_t total_write_size;

    /// Upload in S3 is made in parts.
    /// We initiate upload, then upload each part and get ETag as a response, and then finish upload with listing all our parts.
    String multipart_upload_id;
    std::atomic_size_t part_id = 1;
    // part id -> part tag
    std::mutex mutex;
    std::map<size_t, String> part_tags;
    std::unordered_map<size_t, std::shared_ptr<Aws::StringStream>> part_buffers;
    bool use_parallel_upload;
    std::unique_ptr<ThreadPool> upload_pool;

    LoggerPtr log = getLogger("WriteBufferFromS3");

public:
    explicit WriteBufferFromS3(
        const std::shared_ptr<Aws::S3::S3Client> & client_ptr_,
        const String & bucket_,
        const String & key_,
        size_t upload_single_part_size_ = 128 * 1024 * 1024,
        size_t multipart_upload_threshold_ = 64 * 1024 * 1024,
        std::optional<std::map<String, String>> object_metadata_ = std::nullopt,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        bool allow_overwrite_ = false,
        bool use_parallel_upload_ = false,
        size_t parallel_upload_pool_size_ = 8,
        bool for_disk_s3_ = false);

    ~WriteBufferFromS3() override;

    void nextImpl() override;

    void sync() override {}

    String getFileName() const override
    {
        return s3_util.getBucket() + "/" + key;
    }


protected:
    void finalizeImpl() override;

    WriteBuffer * inplaceReconstruct(const String & out_path, [[maybe_unused]] std::unique_ptr<WriteBuffer> nested) override
    {
        std::shared_ptr<Aws::S3::S3Client> client_ptr_tmp = std::move(this->s3_util.getClient());
        const Poco::URI out_uri(out_path);
        const String & bucket_tmp = out_uri.getHost();
        const String & key_tmp = out_uri.getPath().substr(1);
        size_t minimum_upload_part_size_tmp = this->upload_single_part_size;
        size_t max_single_part_upload_size_tmp = this->multipart_upload_threshold;
        bool use_parallel_upload_tmp = this->use_parallel_upload;
        size_t parallel_upload_pool_size_tmp = 8;
        if (use_parallel_upload)
            parallel_upload_pool_size_tmp = this->upload_pool->getMaxThreads();

        // Call the destructor explicitly but does not free memory
        this->~WriteBufferFromS3();
        new (this) WriteBufferFromS3(
            client_ptr_tmp,
            bucket_tmp,
            key_tmp,
            minimum_upload_part_size_tmp,
            max_single_part_upload_size_tmp,
            std::nullopt,
            DBMS_DEFAULT_BUFFER_SIZE,
            use_parallel_upload_tmp,
            parallel_upload_pool_size_tmp);
        return this;
    }

private:
    void allocateBuffer();

    void createMultipartUpload();

    void writePart();

    void completeMultipartUpload();

    void abortMultipartUpload();

    void makeSinglepartUpload();
};

}

#endif
