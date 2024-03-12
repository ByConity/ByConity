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


#include "Storages/Hive/HivePartition.h"
#if USE_HIVE

#include "Formats/FormatFactory.h"
#include "IO/ReadBufferFromString.h"
#include "IO/ReadHelpers.h"
#include "IO/WriteBufferFromString.h"
#include "IO/WriteHelpers.h"
#include "Processors/Formats/Impl/CSVRowInputFormat.h"
#include "Processors/Formats/InputStreamFromInputFormat.h"
#include "Storages/KeyDescription.h"

#include <hive_metastore_types.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

void HivePartition::load(const Apache::Hadoop::Hive::Partition & apache_partition, const KeyDescription & description)
{
    partition_id = fmt::format("{}\n", fmt::join(apache_partition.values, ","));
    ReadBufferFromString rb(partition_id);
    load(rb, description);

    file_format = apache_partition.sd.inputFormat;
    location = apache_partition.sd.location;
}

void HivePartition::load(const String & partition_id_, const KeyDescription & description)
{
    partition_id = partition_id_;
    ReadBufferFromString rb(partition_id);
    load(rb, description);
}

void HivePartition::load(ReadBuffer & buffer, const KeyDescription & description)
{
    auto format = std::make_shared<CSVRowInputFormat>(
        description.sample_block, buffer, IRowInputFormat::Params{.max_block_size = 1}, false, FormatSettings{});
    auto reader = std::make_shared<InputStreamFromInputFormat>(format);
    auto block = reader->read();

    if (!block.rows())
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Can not parse partition value {}", partition_id);
    }

    value.resize(block.columns());
    for (size_t i = 0; i < block.columns(); ++i)
    {
        block.getByPosition(i).column->get(0, value[i]);
    }
}

void HivePartition::load(const Apache::Hadoop::Hive::StorageDescriptor & sd)
{
    file_format = sd.inputFormat;
    location = sd.location;
}

void HivePartition::store(WriteBuffer & buf, const KeyDescription & description) const
{
    const auto & partition_key_sample = description.sample_block;
    if (!partition_key_sample)
        return;

    for (size_t i = 0; i < value.size(); ++i)
        partition_key_sample.getByPosition(i).type->getDefaultSerialization()->serializeBinary(value[i], buf);
}

}

#endif
