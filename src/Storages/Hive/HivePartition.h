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

#include "Common/config.h"
#if USE_HIVE

#include "Core/Field.h"

namespace Apache::Hadoop::Hive
{
<<<<<<< HEAD
    String db_name;
    String table_name;
    String hdfs_uri;
    String partition_path;
    String table_path;
    int32_t create_time;
    int32_t last_access_time;
    std::vector<String> values;
    String input_format;
    String output_format;
    std::vector<FieldSchema> cols;
    std::vector<String> parts_name;

    const std::vector<String> & getPartsName() const { return parts_name; }
    const String & getLocation() const { return partition_path; }
};
=======
    class Partition;
    class StorageDescriptor;
}

namespace DB
{
namespace Protos
{
    class ProtoHiveFile;
    class ProtoHiveFiles;
}

struct KeyDescription;
>>>>>>> e22f20f6c2 (Merge branch 'pick-hive' into 'cnch-ce-merge')

struct HivePartition
{
    Row value;
    /// partition values in csv format
    String partition_id;

<<<<<<< HEAD
    const String & getID() const;
    const String & getTablePath() const;
    const String & getPartitionPath();
    const String & getTableName() const;
    const String & getDBName() const;
    int32_t getCreateTime() const;
    int32_t getLastAccessTime() const;
    const std::vector<String> & getValues() const;
    const String & getInputFormat() const;
    const String & getOutputFromat() const;
    const std::vector<String> & getPartsName() const;
    const String & getHDFSUri() const;
=======
    /// deserialize from hive partition value
    void load(const Apache::Hadoop::Hive::Partition & apache_partition, const KeyDescription & description);
    void load(const String & partition_id_, const KeyDescription & description);

    /// non partition case
    void load(const Apache::Hadoop::Hive::StorageDescriptor & sd);
>>>>>>> e22f20f6c2 (Merge branch 'pick-hive' into 'cnch-ce-merge')

    /// just for listing
    String file_format;
    String location;
private:
    void load(ReadBuffer & buf, const KeyDescription & description);
};

using HivePartitionPtr = std::shared_ptr<HivePartition>;
using HivePartitons = std::vector<HivePartitionPtr>;
}

#endif
