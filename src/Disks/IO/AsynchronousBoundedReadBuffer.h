#pragma once

#include <Common/Logger.h>
#include <chrono>
#include <utility>
#include <IO/AsynchronousReader.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <Common/Priority.h>

namespace Poco { class Logger; }

namespace DB
{

struct AsyncReadCounters;
using AsyncReadCountersPtr = std::shared_ptr<AsyncReadCounters>;

class AsynchronousBoundedReadBuffer : public ReadBufferFromFileBase
{
public:
    using Impl = ReadBufferFromFileBase;
    using ImplPtr = std::unique_ptr<Impl>;

    explicit AsynchronousBoundedReadBuffer(
        ImplPtr impl_,
        IAsynchronousReader & reader_,
        const ReadSettings & settings_,
        AsyncReadCountersPtr async_read_counters_ = nullptr);

    ~AsynchronousBoundedReadBuffer() override;

    String getFileName() const override { return impl->getFileName(); }

    size_t getFileSize() override { return impl->getFileSize(); }

    off_t seek(off_t offset_, int whence) override;

    void prefetch(Priority priority) override;

    void setReadUntilPosition(size_t position) override; /// [..., position).

    void setReadUntilEnd() override { return setReadUntilPosition(getFileSize()); }

    off_t getPosition() override { return file_offset_of_buffer_end - available() + bytes_to_ignore; }

    bool supportsReadAt() override { return impl->supportsReadAt(); }

    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) override
    {
        return impl->readBigAt(to, n, range_begin, progress_callback);
    }

private:
    const ImplPtr impl;
    const ReadSettings read_settings;
    IAsynchronousReader & reader;

    size_t file_offset_of_buffer_end = 0; /// What offset in file corresponds to working_buffer.end().
    std::optional<size_t> read_until_position;
    /// If nonzero then working_buffer is empty.
    /// If a prefetch is in flight, the prefetch task has been instructed to ignore this many bytes.
    size_t bytes_to_ignore = 0;

    Memory<> prefetch_buffer;
    std::future<IAsynchronousReader::Result> prefetch_future;

    LoggerPtr log;

    AsyncReadCountersPtr async_read_counters;

    bool nextImpl() override;

    void finalize();

    bool hasPendingDataToRead();

    std::future<IAsynchronousReader::Result> asyncReadInto(char * data, size_t size, Priority priority);

    void resetPrefetch();

};

}
