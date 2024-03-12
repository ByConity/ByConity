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

#include <algorithm>
#include <cstring>
#include <memory>
#include <iostream>
#include <cassert>

#include <Common/MemoryTracker.h>
#include <Common/Exception.h>
#include <IO/BufferBase.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
}


/** A simple abstract class for buffered data writing (char sequences) somewhere.
  * Unlike std::ostream, it provides access to the internal buffer,
  *  and also allows you to manually manage the position inside the buffer.
  *
  * Derived classes must implement the nextImpl() method.
  */
class WriteBuffer : public BufferBase
{
public:
    using BufferBase::set;
    using BufferBase::position;
    WriteBuffer(Position ptr, size_t size) : BufferBase(ptr, size, 0) {}
    void set(Position ptr, size_t size) { BufferBase::set(ptr, size, 0); }

    /** write the data in the buffer (from the beginning of the buffer to the current position);
      * set the position to the beginning; throw an exception, if something is wrong
      */
    inline void next()
    {
        if (!offset())
            return;
        bytes += offset();

        try
        {
            nextImpl();
        }
        catch (...)
        {
            /** If the nextImpl() call was unsuccessful, move the cursor to the beginning,
              * so that later (for example, when the stack was expanded) there was no second attempt to write data.
              */
            pos = working_buffer.begin();
            throw;
        }

        pos = working_buffer.begin();
    }

    /** it is desirable in the derived classes to place the next() call in the destructor,
      * so that the last data is written
      */
    virtual ~WriteBuffer() override = default;

    inline void nextIfAtEnd()
    {
        if (!hasPendingData())
            next();
    }


    void write(const char * from, size_t n)
    {
        size_t bytes_copied = 0;

        /// Produces endless loop
        assert(!working_buffer.empty());

        while (bytes_copied < n)
        {
            nextIfAtEnd();
            size_t bytes_to_copy = std::min(static_cast<size_t>(working_buffer.end() - pos), n - bytes_copied);
            memcpy(pos, from + bytes_copied, bytes_to_copy);
            pos += bytes_to_copy;
            bytes_copied += bytes_to_copy;
        }
    }


    inline void write(char x)
    {
        nextIfAtEnd();
        *pos = x;
        ++pos;
    }

    virtual void sync()
    {
        next();
    }

    /// This method may be called before finalize() to tell there would not be any more data written.
    /// Used does not have to call it, implementation should check it itself if needed.
    ///
    /// The idea is similar to prefetch. In case if all data is written, we can flush the buffer
    /// and start sending data asynchronously. It may improve writing performance in case you have
    /// multiple files to finalize. Mainly, for blob storage, finalization has high latency,
    /// and calling preFinalize in a loop may parallelize it.
    virtual void preFinalize() { next(); }

    virtual WriteBuffer * inplaceReconstruct([[maybe_unused]] const String & out_path, [[maybe_unused]] std::unique_ptr<WriteBuffer> nested)
    {
        throw Exception("Corresponding write buffer doesn't implement inplaceReconstruct().", ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Write the last data.
    virtual void finalize()
    {
        if (finalized)
            return;

        MemoryTracker::LockExceptionInThread lock(VariableContext::Global);
        try
        {
            finalizeImpl();
            finalized = true;
        }
        catch (...)
        {
            pos = working_buffer.begin();
            finalized = true;
            throw;
        }
    }

protected:

    virtual void finalizeImpl()
    {
        next();
    }

    bool finalized = false;

private:
    /** Write the data in the buffer (from the beginning of the buffer to the current position).
      * Throw an exception if something is wrong.
      */
    virtual void nextImpl() { throw Exception("Cannot write after end of buffer.", ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER); }
};


using WriteBufferPtr = std::shared_ptr<WriteBuffer>;


}
