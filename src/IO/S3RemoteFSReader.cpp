#include <chrono>
#include <cstdint>
#include <thread>
#include <IO/S3RemoteFSReader.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <Poco/Logger.h>
#include "Common/ProfileEvents.h"

namespace ProfileEvents {
    extern const Event S3TrivialReaderReadCount;
    extern const Event S3TrivialReaderReadMicro;
    extern const Event S3TrivialReaderReadBytes;
    extern const Event S3ReadAheadReaderReadCount;
    extern const Event S3ReadAheadReaderRemoteReadCount;
    extern const Event S3ReadAheadReaderReadMicro;
    extern const Event S3ReadAheadReaderExpectReadBytes;
    extern const Event S3ReadAheadReaderReadBytes;
}

namespace DB {

namespace ErrorCodes {
    extern const int S3_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
}

S3TrivialReader::S3TrivialReader(const std::shared_ptr<Aws::S3::S3Client>& client,
    const String& bucket, const String& key):
        client_(client), bucket_(bucket), key_(key), current_offset_(0) {}

uint64_t S3TrivialReader::read(char* buffer, uint64_t size) {
    if (size == 0) {
        return 0;
    }

    Stopwatch watch;
    ProfileEvents::increment(ProfileEvents::S3TrivialReaderReadCount);
    SCOPE_EXIT({ProfileEvents::increment(ProfileEvents::S3TrivialReaderReadMicro, watch.elapsedMicroseconds());});

    uint64_t readed = readFragment(buffer, current_offset_, size);

    ProfileEvents::increment(ProfileEvents::S3TrivialReaderReadBytes, readed);

    current_offset_ += readed;

    return readed;
}

uint64_t S3TrivialReader::readFragment(char* buffer, uint64_t offset, uint64_t size) {
    String range = fmt::format("bytes={}-{}", offset, offset + size - 1);

    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(bucket_);
    req.SetKey(key_);
    req.SetRange(range);

    Aws::S3::Model::GetObjectOutcome outcome = client_->GetObject(req);

    if (outcome.IsSuccess()) {
        Aws::IOStream& stream = outcome.GetResult().GetBody();
        stream.read(buffer, size);
        size_t last_read_count = stream.gcount();
        if (!last_read_count) {
            if (stream.eof()) {
                return 0;
            }

            if (stream.fail()) {
                throw Exception("Cannot read from istream", ErrorCodes::S3_ERROR);
            }

            throw Exception("Unexpected state of istream", ErrorCodes::S3_ERROR);
        }

        return last_read_count;
    } else {
        if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::REQUESTED_RANGE_NOT_SATISFIABLE) {
            return 0;
        }
        throw S3::S3Exception(outcome.GetError());
    }
}

uint64_t S3TrivialReader::seek(uint64_t offset) {
    current_offset_ = offset;
    return offset;
}

S3ReadAheadReader::S3ReadAheadReader(const std::shared_ptr<Aws::S3::S3Client>& client,
    const String& bucket, const String& key, size_t min_read_size,
    size_t max_read_expand_times, size_t read_expand_pct,
    size_t seq_read_thres, Poco::Logger* logger):
        min_read_size_(min_read_size), max_read_expand_times_(max_read_expand_times),
        read_expand_pct_(read_expand_pct), seq_read_threshold_(seq_read_thres),
        logger_(logger), client_(client), bucket_(bucket), key_(key),
        read_expanded_times_(0), readed_bytes_on_current_reader_(0), current_offset_(0),
        reader_size_(min_read_size), reader_end_offset_(0),
        reader_(std::make_unique<Aws::S3::Model::GetObjectResult>()),
        hint_file_size_(std::nullopt) {
    if (read_expand_pct < 100) {
        throw Exception(fmt::format("Read expand ratio {} is less than 100", read_expand_pct),
            ErrorCodes::BAD_ARGUMENTS);
    }
}

uint64_t S3ReadAheadReader::read(char* buffer, uint64_t size) {
    if (hint_file_size_.has_value() && current_offset_ >= hint_file_size_.value()) {
        return 0;
    }

    Stopwatch watch;
    ProfileEvents::increment(ProfileEvents::S3ReadAheadReaderReadCount);
    ProfileEvents::increment(ProfileEvents::S3ReadAheadReaderExpectReadBytes, size);
    SCOPE_EXIT({ProfileEvents::increment(ProfileEvents::S3ReadAheadReaderReadMicro, watch.elapsedMicroseconds());});

    if (current_offset_ == reader_end_offset_) {
        updateBufferSize(size);

        if (!refillBuffer()) {
            return 0;
        }
    }

    return readFromBuffer(buffer, size);
}

void S3ReadAheadReader::updateBufferSize(uint64_t size) {
    uint64_t used_pct = reader_size_ == 0 ? 0 :
        100 * readed_bytes_on_current_reader_ / reader_size_;

    readed_bytes_on_current_reader_ = 0;

    if (used_pct >= seq_read_threshold_) {
        if (read_expanded_times_ < max_read_expand_times_) {
            ++read_expanded_times_;
            reader_size_ = reader_size_ * read_expand_pct_ / 100;
        }
    } else {
        read_expanded_times_ = 0;
        reader_size_ = size;
    }

    reader_size_ = std::max(reader_size_, std::max(size, min_read_size_));
}

bool S3ReadAheadReader::refillBuffer() {
    ProfileEvents::increment(ProfileEvents::S3ReadAheadReaderRemoteReadCount);

    assert(current_offset_ == reader_end_offset_
        && "Refill buffer when cursor didn't reach end of current reader");

    String range = "bytes=" + std::to_string(current_offset_) + "-" \
        + std::to_string(current_offset_ + reader_size_ - 1);

    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(bucket_);
    req.SetKey(key_);
    req.SetRange(range);

    Aws::S3::Model::GetObjectOutcome outcome = client_->GetObject(req);

    if (outcome.IsSuccess()) {
        *reader_ = outcome.GetResultWithOwnership();
        uint64_t readed = reader_->GetContentLength();
        reader_end_offset_ = current_offset_ + readed;

        if (!hint_file_size_.has_value() && readed < reader_size_) {
            hint_file_size_ = reader_end_offset_;
        }

        ProfileEvents::increment(ProfileEvents::S3ReadAheadReaderReadBytes, readed);

        return true;
    } else {
        if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::REQUESTED_RANGE_NOT_SATISFIABLE) {
            return false;
        }
        throw S3::S3Exception(outcome.GetError(), fmt::format("read bucket = {}, key = {} failed!", bucket_, key_));
    }
}

uint64_t S3ReadAheadReader::readFromBuffer(char* buffer, uint64_t size) {
    try {
        Aws::IOStream& stream = reader_->GetBody();
        stream.read(buffer, size);
        size_t last_read_count = stream.gcount();

        if (!last_read_count) {
            if (stream.eof()) {
                return 0;
            }

            if (stream.fail()) {
                throw Exception(fmt::format("Cannot read from input stream while reading {}",
                    key_), ErrorCodes::S3_ERROR);
            }

            throw Exception(fmt::format("Unexpected state of input stream while reading {}",
                key_), ErrorCodes::S3_ERROR);
        }

        readed_bytes_on_current_reader_ += last_read_count;
        current_offset_ += last_read_count;
        return last_read_count;
    } catch (...) {
        // Read from source failed, reset reader
        resetReader(current_offset_);

        throw;
    }
}

uint64_t S3ReadAheadReader::seek(uint64_t offset) {
    try {
        if (offset >= current_offset_ && offset < reader_end_offset_) {
            if (offset == current_offset_) {
                return offset;
            }

            Aws::IOStream& stream = reader_->GetBody();
            stream.ignore(offset - current_offset_);

            size_t last_read_count = stream.gcount();

            if (stream.eof()) {
                throw Exception(fmt::format("Unexpected eof when seek to {} from {} "
                    "with ignore, ignored count {}", offset, current_offset_,
                    last_read_count), ErrorCodes::S3_ERROR);
            }

            if (stream.fail()) {
                throw Exception(fmt::format("Failed to ignore data when seek to {} from {}",
                    offset, current_offset_), ErrorCodes::S3_ERROR);
            }

            current_offset_ = offset;
            return offset;
        }

        resetReader(offset);
    } catch (...) {
        resetReader(offset);
    }
    return offset;
}

void S3ReadAheadReader::resetReader(uint64_t offset) {
    read_expanded_times_ = 0;
    readed_bytes_on_current_reader_ = 0;
    current_offset_ = offset;
    reader_size_ = min_read_size_;
    reader_end_offset_ = offset;
}

S3RemoteFSReader::S3RemoteFSReader(std::unique_ptr<S3Reader> reader, size_t read_backoff_ms,
    size_t read_retry, const ThrottlerPtr& throttler):
        read_backoff_ms_(read_backoff_ms), read_retry_(read_retry),
        throttler_(throttler), reader_(std::move(reader)) {}

String S3RemoteFSReader::objectName() const {
    return reader_->key();
}

uint64_t S3RemoteFSReader::read(char* buffer, uint64_t size) {
    uint64_t total_readed = 0;
    size_t read_tried = 0;
    auto sleep_time_with_backoff_milliseconds = std::chrono::milliseconds(read_backoff_ms_);

    while (size > total_readed) {
        try {
            uint64_t readed = reader_->read(buffer + total_readed, size - total_readed);

            if (readed == 0) {
                break;
            }

            if (throttler_ != nullptr) {
                throttler_->add(readed);
            }

            total_readed += readed;
        } catch (const Exception& e) {
            if (read_tried++ >= read_retry_) {
                throw;
            }

            std::this_thread::sleep_for(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;
        }
    }

    return total_readed;
}

uint64_t S3RemoteFSReader::seek(uint64_t offset) {
    return reader_->seek(offset);
}

uint64_t S3RemoteFSReader::offset() const {
    return reader_->offset();
}

uint64_t S3RemoteFSReader::remain() const {
    return reader_->remain();
}

S3RemoteFSReaderOpts::S3RemoteFSReaderOpts(const std::shared_ptr<Aws::S3::S3Client>& client,
    const String& bucket, bool read_ahead, size_t read_retry, size_t read_backoff_ms,
    const ThrottlerPtr& throttler, size_t ra_min_read_size, size_t ra_max_read_expand_times,
    size_t ra_expand_pct, size_t ra_threshold_pct):
        read_ahead_(read_ahead), read_retry_(read_retry), read_backoff_ms_(read_backoff_ms),
        throttler_(throttler), client_(client), bucket_(bucket),
        logger_(&Poco::Logger::get("S3RemoteFSReader")), ra_min_read_size_(ra_min_read_size),
        ra_max_read_expand_times_(ra_max_read_expand_times),
        ra_expand_pct_(ra_expand_pct), ra_threshold_pct_(ra_threshold_pct) {}

std::unique_ptr<RemoteFSReader> S3RemoteFSReaderOpts::create(const String& path) {
    std::unique_ptr<S3Reader> reader = nullptr;
    // if (read_ahead_) {
    //     reader = std::make_unique<S3ReadAheadReader>(client_, bucket_, path,
    //         ra_min_read_size_, ra_max_read_expand_times_, ra_expand_pct_,
    //         ra_threshold_pct_, logger_);
    // } else {
        reader = std::make_unique<S3TrivialReader>(client_, bucket_, path);
    // }

    return std::make_unique<S3RemoteFSReader>(std::move(reader), read_backoff_ms_,
        read_retry_, throttler_);
}

}
