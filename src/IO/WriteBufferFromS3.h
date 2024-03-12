#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AWS_S3

#    include <memory>
#    include <vector>
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
class WriteBufferFromS3 : public BufferWithOwnMemory<WriteBuffer>
{
private:
    String bucket;
    String key;
    std::optional<std::map<String, String>> object_metadata;
    std::shared_ptr<Aws::S3::S3Client> client_ptr;
    size_t minimum_upload_part_size;
    size_t max_single_part_upload_size;
    /// Buffer to accumulate data.
    std::shared_ptr<Aws::StringStream> temporary_buffer;
    size_t last_part_size;

    /// Upload in S3 is made in parts.
    /// We initiate upload, then upload each part and get ETag as a response, and then finish upload with listing all our parts.
    String multipart_upload_id;
    std::vector<String> part_tags;

    Poco::Logger * log = &Poco::Logger::get("WriteBufferFromS3");

public:
    explicit WriteBufferFromS3(
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        size_t minimum_upload_part_size_,
        size_t max_single_part_upload_size_,
        std::optional<std::map<String, String>> object_metadata_ = std::nullopt,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    void nextImpl() override;

protected:
    void finalizeImpl() override;

    WriteBuffer * inplaceReconstruct(const String & out_path, [[maybe_unused]] std::unique_ptr<WriteBuffer> nested) override
    {
        std::shared_ptr<Aws::S3::S3Client> client_ptr_tmp = std::move(this->client_ptr);
        const Poco::URI out_uri(out_path);
        const String & bucket_tmp = out_uri.getHost();
        const String & key_tmp = out_uri.getPath().substr(1);
        size_t minimum_upload_part_size_tmp = this->minimum_upload_part_size;
        size_t max_single_part_upload_size_tmp = this->max_single_part_upload_size;

        // Call the destructor explicitly but does not free memory
        this->~WriteBufferFromS3();
        new (this) WriteBufferFromS3(client_ptr_tmp, bucket_tmp, key_tmp, minimum_upload_part_size_tmp, max_single_part_upload_size_tmp);
        return this;
    }

private:
    void allocateBuffer();

    void createMultipartUpload();
    void writePart();
    void completeMultipartUpload();

    void makeSinglepartUpload();
};

}

#endif
