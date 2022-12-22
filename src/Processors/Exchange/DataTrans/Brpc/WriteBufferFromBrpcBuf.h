#pragma once

#include <IO/WriteBuffer.h>
#include <butil/iobuf.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int DISTRIBUTE_STAGE_QUERY_EXCEPTION;
}

/// Zero-copy write buffer from butil::IOBuf of brpc library.
/// Add a member IOBuf::epxand(size_t hint) for simplifying code, and very few performance gain
class WriteBufferFromBrpcBuf : public WriteBuffer
{
public:
    WriteBufferFromBrpcBuf() : WriteBuffer(nullptr, 0)
    {
        auto block_view = buf.expand(initial_size);
        if (block_view.empty())
            throw Exception("Cannot resize butil::IOBuf to " + std::to_string(initial_size), ErrorCodes::DISTRIBUTE_STAGE_QUERY_EXCEPTION);
        set(const_cast<Position>(block_view.data()), block_view.size());
    }

    ~WriteBufferFromBrpcBuf() override { finish(); }

    void nextImpl() override
    {
        if (is_finished)
            throw Exception("WriteBufferFromBrpcBuf is finished", ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER);

        auto block_view = buf.expand(buf.size());
        if (block_view.empty())
            throw Exception("Cannot resize butil::IOBuf to " + std::to_string(initial_size), ErrorCodes::DISTRIBUTE_STAGE_QUERY_EXCEPTION);
        set(const_cast<Position>(block_view.data()), block_view.size());
    }

    void finish()
    {
        if (is_finished)
            return;
        is_finished = true;

        buf.resize(buf.size() - available());
        /// Prevent further writes.
        set(nullptr, 0);
    }

    const auto & getIntermediateBuf() const { return buf; }

    const auto & getFinishedBuf()
    {
        finish();
        return buf;
    }

private:
    static constexpr size_t initial_size = 32;

    butil::IOBuf buf;
    bool is_finished = false;
};
}
