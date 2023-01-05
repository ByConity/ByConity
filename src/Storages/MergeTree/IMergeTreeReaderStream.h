#pragma once

#include <Core/Types.h>
#include <IO/ReadBuffer.h>

namespace DB
{

class IMergeTreeReaderStream
{
public:
    IMergeTreeReaderStream(): data_buffer(nullptr) {}

    virtual ~IMergeTreeReaderStream() {}

    virtual void seekToMark(size_t index) = 0;
    virtual void seekToStart() = 0;

    ReadBuffer * data_buffer;
};

}
