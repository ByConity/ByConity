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

#include <Common/config.h>
#if USE_HIVE

#include <Core/Field.h>

namespace Apache::Hadoop::Hive
{
class Partition;
class StorageDescriptor;
}

namespace DB
{

struct KeyDescription;
struct FormatSettings;

struct HivePartition
{
    Row value;
    /// partition values in csv format
    String partition_id;

    /// deserialize from hive partition value
    void load(const Apache::Hadoop::Hive::Partition & apache_partition, const KeyDescription & description);
    void load(const String & partition_id_, const KeyDescription & description);
    void load(ReadBuffer & buf, const KeyDescription & description);
    void loadFromBinary(ReadBuffer & buf, const KeyDescription & description);

    /// non partition case
    void load(const Apache::Hadoop::Hive::StorageDescriptor & sd);

    void store(WriteBuffer & buf, const KeyDescription & description) const;

    void serializeText(const KeyDescription & description, WriteBuffer & out, const FormatSettings & format_settings) const;

    /// just for listing
    String file_format;
    String location;
};

using HivePartitionPtr = std::shared_ptr<HivePartition>;
using HivePartitions = std::vector<HivePartitionPtr>;
}

#endif
