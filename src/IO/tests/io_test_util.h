#pragma once

#include <memory>
#include <thread>
#include <filesystem>
#include <random>
#include <gtest/gtest.h>
#include <IO/ReadBufferFromFile.h>
#include <Core/Types.h>
#include <Common/ThreadPool.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/RemoteFSReader.h>
#include <IO/WriteBufferFromFile.h>

namespace DB {

namespace ErrorCodes {
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

void replaceStr(String& str, const String& old_str, const String& new_str);

String randomString(size_t length);

class MockedRemoteFSReader: public RemoteFSReader {
public:
    MockedRemoteFSReader(const String& path, size_t max_delay_ns):
            re_(rand()), dist_(std::nullopt), in_(path) {
        if (max_delay_ns > 0) {
            dist_ = std::uniform_int_distribution<size_t>(0, max_delay_ns - 1);
        }
    }
 
    virtual String objectName() const override {
        return in_.getFileName();
    }

    virtual uint64_t read(char* buffer, uint64_t size) override {
        if (dist_.has_value()) {
            if (size_t wait_ns = dist_.value()(re_); wait_ns != 0) {
                std::this_thread::sleep_for(std::chrono::nanoseconds(wait_ns));
            }
        }

        return in_.readBig(buffer, size);
    }

    virtual uint64_t seek(uint64_t offset) override {
        return in_.seek(offset, SEEK_SET);
    }

    virtual uint64_t offset() const override {
        return in_.offset();
    }
    
    virtual uint64_t remain() const override {
        return 0;
    }

private:
    std::default_random_engine re_;
    std::optional<std::uniform_int_distribution<size_t>> dist_;
    ReadBufferFromFile in_;
};

struct MockedRemoteFSReaderOpts: public RemoteFSReaderOpts {
    MockedRemoteFSReaderOpts(size_t delay): max_request_delay_(delay) {}

    virtual std::unique_ptr<RemoteFSReader> create(const String& path) {
        return std::make_unique<MockedRemoteFSReader>(path, max_request_delay_);
    }

    size_t max_request_delay_;
};

class BufferVerifyReader {
public:
    BufferVerifyReader(std::unique_ptr<ReadBufferFromFileBase> source,
        std::unique_ptr<ReadBufferFromFileBase> candidate): source_(std::move(source)),
            candidate_(std::move(candidate)) {}

    void read(size_t size) {
        String buffer(size + 1, '\0');
        size_t src_readed = source_->read(buffer.data(), size);
        String candidate_buffer(size + 1, '\0');
        size_t candidate_readed = candidate_->read(candidate_buffer.data(), size);

        ASSERT_EQ(src_readed, candidate_readed);
        ASSERT_EQ(buffer, candidate_buffer);
    }

    void seek(size_t offset) {
        source_->seek(offset);
        candidate_->seek(offset);
    }
    
private:
    std::unique_ptr<ReadBufferFromFileBase> source_;
    std::unique_ptr<ReadBufferFromFileBase> candidate_;
};

}