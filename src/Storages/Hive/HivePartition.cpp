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

#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Storages/KeyDescription.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnTuple.h>
#include <hive_metastore_types.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

void HivePartition::load(const Apache::Hadoop::Hive::Partition & apache_partition, const KeyDescription & description)
{
    {
        WriteBufferFromString write_buf(partition_id);
        bool is_first = true;
        for (const auto & key : apache_partition.values)
        {
            if (!std::exchange(is_first, false))
                writeString(",", write_buf);

            if (key != "__HIVE_DEFAULT_PARTITION__")
                writeString(key, write_buf);
            else
                writeString("\\N", write_buf);
        }
        writeString("\n", write_buf);
    }

    ReadBufferFromString read_buf(partition_id);
    load(read_buf, description);

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

void HivePartition::loadFromBinary(ReadBuffer & buf, const KeyDescription & description)
{
    const auto & partition_key_sample = description.sample_block;
    value.resize(partition_key_sample.columns());
    for (size_t i = 0; i < partition_key_sample.columns(); ++i)
        partition_key_sample.getByPosition(i).type->getDefaultSerialization()->deserializeBinary(value[i], buf);
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

void HivePartition::serializeText(const KeyDescription & description, WriteBuffer & out, const FormatSettings & format_settings) const
{
    const auto & partition_key_sample = description.sample_block;
    size_t key_size = partition_key_sample.columns();

    // In some cases we create empty parts and then value is empty.
    if (value.empty())
    {
        writeString("tuple()", out);
        return;
    }

    if (key_size == 0)
    {
        writeCString("tuple()", out);
    }
    else if (key_size == 1)
    {
        const DataTypePtr & type = partition_key_sample.getByPosition(0).type;
        auto column = type->createColumn();
        column->insert(value[0]);
        type->getDefaultSerialization()->serializeTextQuoted(*column, 0, out, format_settings);
    }
    else
    {
        DataTypes types;
        Columns columns;
        for (size_t i = 0; i < key_size; ++i)
        {
            const auto & type = partition_key_sample.getByPosition(i).type;
            types.push_back(type);
            auto column = type->createColumn();
            column->insert(value[i]);
            columns.push_back(std::move(column));
        }

        auto tuple_serialization = DataTypeTuple(types).getDefaultSerialization();
        auto tuple_column = ColumnTuple::create(columns);
        tuple_serialization->serializeText(*tuple_column, 0, out, format_settings);
    }
}

}

#endif
