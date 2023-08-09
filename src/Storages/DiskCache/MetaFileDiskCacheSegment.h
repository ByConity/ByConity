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

#include <Storages/DiskCache/IDiskCacheSegment.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include "common/types.h"
#include "Storages/IStorage_fwd.h"

namespace DB
{
class IDiskCache;

class ChecksumsDiskCacheSegment : public IDiskCacheSegment
{
public:
    explicit ChecksumsDiskCacheSegment(IMergeTreeDataPartPtr data_part_, UInt64 preload_level_ = 0);

    String getSegmentName() const override;
    void cacheToDisk(IDiskCache & diskcache, bool throw_exception) override;

private:
    IMergeTreeDataPartPtr data_part;
    ConstStoragePtr storage;
    String segment_name;
    UInt64 preload_level;
};

class PrimaryIndexDiskCacheSegment : public IDiskCacheSegment
{
public:
    explicit PrimaryIndexDiskCacheSegment(IMergeTreeDataPartPtr data_part_, UInt64 preload_level_ = 0);

    String getSegmentName() const override;
    void cacheToDisk(IDiskCache & diskcache, bool throw_exception) override;

private:
    IMergeTreeDataPartPtr data_part;
    ConstStoragePtr storage;
    String segment_name;
    UInt64 preload_level;
};

class MetaInfoDiskCacheSegment : public IDiskCacheSegment
{
public:
    explicit MetaInfoDiskCacheSegment(IMergeTreeDataPartPtr data_part_, UInt64 preload_level_ = 0);

    String getSegmentName() const override;
    void cacheToDisk(IDiskCache & diskcache, bool throw_exception) override;

private:
    IMergeTreeDataPartPtr data_part;
    ConstStoragePtr storage;
    String segment_name;
    UInt64 preload_level;
};

}
