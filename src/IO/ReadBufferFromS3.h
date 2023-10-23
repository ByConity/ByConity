#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AWS_S3

#    include <memory>
#    include <IO/HTTPCommon.h>
#    include <IO/ReadBuffer.h>
#    include <aws/s3/model/GetObjectResult.h>
#    include "IO/ReadBufferFromFileBase.h"
#    include "IO/ReadSettings.h"

namespace Aws::S3
{
class S3Client;
}

namespace DB
{
/**
 * Perform S3 HTTP GET request and provide response to read.
 */
class ReadBufferFromS3 : public ReadBufferFromFileBase
{
private:
    std::shared_ptr<Aws::S3::S3Client> client_ptr;
    String bucket;
    String key;
    UInt64 max_single_read_retries;
    off_t offset = 0;
    std::optional<Aws::S3::Model::GetObjectResult> read_result;
    std::unique_ptr<ReadBuffer> impl;

    Poco::Logger * log = &Poco::Logger::get("ReadBufferFromS3");

public:
    explicit ReadBufferFromS3(
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        const ReadSettings & read_settings,
        UInt64 max_single_read_retries_ = 3,
        bool restricted_seek_ = true);

    ~ReadBufferFromS3() override;

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;

    std::string getFileName() const override { return bucket + "/" + key; }
    size_t getFileSize() override;

    bool supportsReadAt() override { return true; }
    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) override;

private:
    std::unique_ptr<ReadBuffer> initialize();

    Aws::S3::Model::GetObjectResult sendRequest(size_t range_begin, std::optional<size_t> range_end_incl) const;

    ReadSettings read_settings;
    std::optional<size_t> file_size;
    bool restricted_seek;
    bool read_all_range_successfully = false;
};

}

#endif
