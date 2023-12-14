/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#pragma once

#include <IO/ReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class SeekableReadBuffer : public ReadBuffer
{
public:
    SeekableReadBuffer(Position ptr, size_t size)
        : ReadBuffer(ptr, size) {}
    SeekableReadBuffer(Position ptr, size_t size, size_t offset)
        : ReadBuffer(ptr, size, offset) {}

    /**
     * Shifts buffer current position to given offset.
     * @param off Offset.
     * @param whence Seek mode (@see SEEK_SET, @see SEEK_CUR).
     * @return New position from the begging of underlying buffer / file.
     */
    virtual off_t seek(off_t off, int whence = SEEK_SET) = 0;

    /**
     * Keep in mind that seekable buffer may encounter eof() once and the working buffer
     * may get into inconsistent state. Don't forget to reset it on the first nextImpl()
     * after seek().
     */

    /**
     * @return Offset from the begin of the underlying buffer / file corresponds to the buffer current position.
     */
    virtual off_t getPosition() = 0;

    virtual size_t getFileOffsetOfBufferEnd() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getFileOffsetOfBufferEnd() not implemented"); }

    /// Returns true if seek() actually works, false if seek() will always throw (or make subsequent
    /// nextImpl() calls throw).
    ///
    /// This is needed because:
    ///  * Sometimes there's no cheap way to know in advance whether the buffer is really seekable.
    ///    Specifically, HTTP read buffer needs to send a request to check whether the server
    ///    supports byte ranges.
    ///  * Sometimes when we create such buffer we don't know in advance whether we'll need it to be
    ///    seekable or not. So we don't want to pay the price for this check in advance.
    virtual bool checkIfActuallySeekable() { return true; }

    /// Unbuffered positional read.
    /// Doesn't affect the buffer state (position, working_buffer, etc).
    ///
    /// `progress_callback` may be called periodically during the read, reporting that to[0..m-1]
    /// has been filled. If it returns true, reading is stopped, and readBigAt() returns bytes read
    /// so far. Called only from inside readBigAt(), from the same thread, with increasing m.
    ///
    /// Stops either after n bytes, or at end of file, or on exception. Returns number of bytes read.
    /// If offset is past the end of file, may return 0 or throw exception.
    ///
    /// Caller needs to be careful:
    ///  * supportsReadAt() must be checked (called and return true) before calling readBigAt().
    ///    Otherwise readBigAt() may crash.
    ///  * Thread safety: multiple readBigAt() calls may be performed in parallel.
    ///    But readBigAt() may not be called in parallel with any other methods
    ///    (e.g. next() or supportsReadAt()).
    ///  * Performance: there's no buffering. Each readBigAt() call typically translates into actual
    ///    IO operation (e.g. HTTP request). Don't use it for small adjacent reads.
    virtual size_t readBigAt(char * /*to*/, size_t /*n*/, size_t /*offset*/, const std::function<bool(size_t m)> & /*progress_callback*/ = nullptr)
        { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method readBigAt() not implemented"); }

    /// Checks if readBigAt() is allowed. May be slow, may throw (e.g. it may do an HTTP request or an fstat).
    virtual bool supportsReadAt() { return false; }
};

/// Helper for implementing readBigAt().
size_t copyFromIStreamWithProgressCallback(std::istream & istr, char * to, size_t n, const std::function<bool(size_t)> & progress_callback, bool * out_cancelled = nullptr);

}
