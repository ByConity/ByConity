#pragma once
#include "config_formats.h"
#if USE_ARROW || USE_ORC || USE_PARQUET

#include <arrow/io/interfaces.h>

namespace DB
{

class ReadBuffer;
class SeekableReadBuffer;
class WriteBuffer;

class ArrowBufferedOutputStream : public arrow::io::OutputStream
{
public:
    explicit ArrowBufferedOutputStream(WriteBuffer & out_);

    // FileInterface
    arrow::Status Close() override;

    arrow::Result<int64_t> Tell() const override;

    bool closed() const override { return !is_open; }

    // Writable
    arrow::Status Write(const void * data, int64_t length) override;

private:
    WriteBuffer & out;
    int64_t total_length = 0;
    bool is_open = false;

    ARROW_DISALLOW_COPY_AND_ASSIGN(ArrowBufferedOutputStream);
};

class RandomAccessFileFromSeekableReadBuffer : public arrow::io::RandomAccessFile
{
public:
    RandomAccessFileFromSeekableReadBuffer(SeekableReadBuffer & in_, off_t file_size_);

    arrow::Result<int64_t> GetSize() override;

    arrow::Status Close() override;

    arrow::Result<int64_t> Tell() const override;

    bool closed() const override { return !is_open; }

    arrow::Result<int64_t> Read(int64_t nbytes, void * out) override;

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

    arrow::Status Seek(int64_t position) override;

private:
    SeekableReadBuffer & in;
    off_t file_size;
    bool is_open = false;

    ARROW_DISALLOW_COPY_AND_ASSIGN(RandomAccessFileFromSeekableReadBuffer);
};

class RandomAccessFileFromRandomAccessReadBuffer : public arrow::io::RandomAccessFile
{
public:
    explicit RandomAccessFileFromRandomAccessReadBuffer(SeekableReadBuffer & in_, size_t file_size_);

    // These are thread safe.
    arrow::Result<int64_t> GetSize() override;
    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;
    arrow::Future<std::shared_ptr<arrow::Buffer>> ReadAsync(
        const arrow::io::IOContext&, int64_t position, int64_t nbytes) override;

    // These are not thread safe, and arrow shouldn't call them. Return NotImplemented error.
    arrow::Status Seek(int64_t) override;
    arrow::Result<int64_t> Tell() const override;
    arrow::Result<int64_t> Read(int64_t, void*) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t) override;

    arrow::Status Close() override;
    bool closed() const override { return !is_open; }

private:
    SeekableReadBuffer & in;
    size_t file_size;
    bool is_open = true;

    ARROW_DISALLOW_COPY_AND_ASSIGN(RandomAccessFileFromRandomAccessReadBuffer);
};

class ArrowInputStreamFromReadBuffer : public arrow::io::InputStream
{
public:
    explicit ArrowInputStreamFromReadBuffer(ReadBuffer & in);
    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;
    arrow::Status Abort() override;
    arrow::Result<int64_t> Tell() const override;
    arrow::Status Close() override;
    bool closed() const override { return !is_open; }

private:
    ReadBuffer & in;
    bool is_open = false;

    ARROW_DISALLOW_COPY_AND_ASSIGN(ArrowInputStreamFromReadBuffer);
};

std::shared_ptr<arrow::io::RandomAccessFile> asArrowFile(ReadBuffer & in);
std::shared_ptr<arrow::io::RandomAccessFile> asArrowFile(SeekableReadBuffer & in, size_t file_size);

}

#endif
