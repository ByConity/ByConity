/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <Disks/IDisk.h>

namespace DB
{

class IMergeTreeReaderStream
{
public:
    struct StreamFileMeta
    {
        DiskPtr disk;
        String rel_path;
        off_t offset;
        size_t size;
    };

    IMergeTreeReaderStream(): data_buffer(nullptr) {}

    virtual ~IMergeTreeReaderStream() {}

    virtual void seekToMark(size_t index) = 0;
    virtual void seekToStart() = 0;

    ReadBuffer * data_buffer;
};

}
