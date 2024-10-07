#pragma once

#include <Common/Logger.h>
#include <cstdint>
#include <memory>
#include <aws/s3/S3Client.h>
#include <Common/Throttler.h>
#include <IO/RemoteFSReader.h>
#include <IO/S3Common.h>
#include <aws/s3/model/GetObjectResult.h>

namespace Aws::S3 {
class S3Client;
}

namespace DB {

// Makesure all implementation of this class is idempontent, if some read operation
// encounter any exception, it should maintain it's old state
class S3Reader {
public:
    virtual ~S3Reader() = default;

    // Didn't guarantee read as much as size, it may read fewer than expected
    // if eof or failure happened
    virtual uint64_t read(char* buffer, uint64_t size) = 0;
    // Didn't guarantee return actuall offset if seek over file size
    virtual uint64_t seek(uint64_t offset) = 0;

    // Offset may exceeded object's size
    virtual uint64_t offset() const = 0;

    virtual uint64_t remain() const = 0;

    virtual const String& bucket() const = 0;
    virtual const String& key() const = 0;
    virtual std::shared_ptr<Aws::S3::S3Client> client() const = 0;
    virtual LoggerPtr logger() const = 0;
};

class S3TrivialReader: public S3Reader {
public:
    S3TrivialReader(const std::shared_ptr<Aws::S3::S3Client>& client,
        const String& bucket, const String& key);

    virtual uint64_t read(char* buffer, uint64_t size) override;
    virtual uint64_t seek(uint64_t offset) override;

    virtual uint64_t offset() const override { return current_offset_; }

    virtual uint64_t remain() const override { return 0; }

    virtual const String& bucket() const override { return bucket_; }
    virtual const String& key() const override { return key_; }
    virtual std::shared_ptr<Aws::S3::S3Client> client() const override { return client_; }
    virtual LoggerPtr logger() const override { return nullptr; }

private:
    uint64_t readFragment(char* buffer, uint64_t offset, uint64_t size);

    std::shared_ptr<Aws::S3::S3Client> client_;
    const String bucket_;
    const String key_;

    uint64_t current_offset_;
    bool finished_;
};

class S3ReadAheadReader: public S3Reader {
public:
    S3ReadAheadReader(const std::shared_ptr<Aws::S3::S3Client>& client,
        const String& bucket, const String& key, size_t min_read_size,
        size_t max_read_expand_times, size_t read_expand_pct,
        size_t seq_read_thres, LoggerPtr logger);

    virtual ~S3ReadAheadReader() override;

    virtual uint64_t read(char* buffer, uint64_t size) override;
    virtual uint64_t seek(uint64_t offset) override;

    virtual uint64_t offset() const override { return current_offset_; }

    virtual uint64_t remain() const override { return reader_end_offset_ - current_offset_; }

    virtual const String& bucket() const override { return bucket_; }
    virtual const String& key() const override { return key_; }
    virtual std::shared_ptr<Aws::S3::S3Client> client() const override { return client_; }
    virtual LoggerPtr logger() const override { return logger_; }

private:
    void updateBufferSize(uint64_t size);
    bool refillBuffer();
    uint64_t readFromBuffer(char* buffer, size_t size);
    void resetReader();
    void resetReaderAndSize(uint64_t offset);

    const size_t min_read_size_;
    const size_t max_read_expand_times_;
    const size_t read_expand_pct_;
    const size_t seq_read_threshold_;

    LoggerPtr logger_;

    std::shared_ptr<Aws::S3::S3Client> client_;
    const String bucket_;
    const String key_;

    // Expanded times of current reader, update on each reader_ replacement
    uint32_t read_expanded_times_;

    uint64_t readed_bytes_on_current_reader_;

    // Offset of first byte in reader
    uint64_t current_offset_;
    uint64_t reader_size_;
    // End offset of reader, may past file end
    uint64_t reader_end_offset_;
    std::optional<Aws::S3::Model::GetObjectResult> read_result_;
    bool reader_is_drained_;
};

class S3RemoteFSReader: public RemoteFSReader {
public:
    S3RemoteFSReader(std::unique_ptr<S3Reader> reader, size_t read_backoff_ms,
        size_t read_retry, const ThrottlerPtr& throttler);

    virtual String objectName() const override;

    virtual uint64_t read(char* buffer, uint64_t size) override;
    virtual uint64_t seek(uint64_t offset) override;

    virtual uint64_t offset() const override;

    virtual uint64_t remain() const override;

private:
    size_t read_backoff_ms_;

    size_t read_retry_;

    ThrottlerPtr throttler_;

    std::unique_ptr<S3Reader> reader_;
};

struct S3RemoteFSReaderOpts: public RemoteFSReaderOpts {
    S3RemoteFSReaderOpts(const std::shared_ptr<Aws::S3::S3Client>& client,
        const String& bucket, bool read_ahead = true, size_t read_retry = 3, size_t read_backoff_ms = 100,
        const ThrottlerPtr& throttler = nullptr, size_t ra_min_read_size = 64 * 1024, size_t ra_max_read_expand_times = 8,
        size_t ra_expand_pct = 150, size_t ra_threshold_pct = 70);

    virtual std::unique_ptr<RemoteFSReader> create(const String& path) override;

    const bool read_ahead_;

    // S3RemoteFSReader options
    const size_t read_retry_;
    const size_t read_backoff_ms_;
    const ThrottlerPtr throttler_;

    // Service info
    const std::shared_ptr<Aws::S3::S3Client> client_;
    const String bucket_;

    LoggerPtr logger_;

    // Readahead options
    size_t ra_min_read_size_;
    size_t ra_max_read_expand_times_;
    size_t ra_expand_pct_;
    size_t ra_threshold_pct_;
};

}
