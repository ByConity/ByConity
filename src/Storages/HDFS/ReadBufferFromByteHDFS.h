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

#include "Common/ObjectPool.h"
#include "Common/config.h"
#include "Storages/HDFS/HDFSCommon.h"
#if USE_HDFS

#include "Core/Defines.h"
#include "Common/Throttler.h"
#include "IO/ReadBufferFromFileBase.h"
#include "IO/ReadSettings.h"

namespace DB
{
class HDFSConnectionParams;

class ReadBufferFromByteHDFS : public ReadBufferFromFileBase
{
struct ReadBufferFromHDFSImpl;

public:
    ReadBufferFromByteHDFS(
        const String & hdfs_file_path,
        const HDFSConnectionParams & hdfs_params,
        const ReadSettings & read_settings = {},
        char * existing_memory = nullptr,
        size_t alignment = 0,
        bool use_external_buffer_ = false,
        off_t read_until_position_ = 0,
        std::optional<size_t> file_size = std::nullopt);

    ~ReadBufferFromByteHDFS() override;

    bool nextImpl() override;
    off_t seek(off_t offset_, int whence) override;

    size_t getFileOffsetOfBufferEnd() const override;

    IAsynchronousReader::Result readInto(char * data, size_t size, size_t offset, size_t ignore) override;

    off_t getPosition() override;
    size_t getFileSize() override;
    String getFileName() const override;

    bool supportsReadAt() override { return true; }
    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) override;

    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;

    bool isSeekCheap() override { return true; }

private:

    void initImpl();

    const String hdfs_file_path;
    const HDFSConnectionParams hdfs_params;
    const off_t read_until_position;
    ReadSettings settings;
    std::unique_ptr<ReadBufferFromHDFSImpl> impl;
    ThrottlerPtr total_network_throttler;
     // Pool for reusing ReadBufferFromHDFSImpl under concurrent readBigAt
    SimpleObjectPool<ReadBufferFromHDFSImpl> impl_pool;
};

}

#endif
