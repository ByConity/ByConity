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

#include <Storages/System/StorageSystemDisks.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
}


StorageSystemDisks::StorageSystemDisks(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
    {
        {"name", std::make_shared<DataTypeString>()},
        {"id", std::make_shared<DataTypeUInt64>()},
        {"path", std::make_shared<DataTypeString>()},
        {"free_space", std::make_shared<DataTypeUInt64>()},
        {"total_space", std::make_shared<DataTypeUInt64>()},
        {"keep_free_space", std::make_shared<DataTypeUInt64>()},
        {"unreserved_space", std::make_shared<DataTypeUInt64>()},
        {"free_inodes", std::make_shared<DataTypeUInt64>()},
        {"total_inodes", std::make_shared<DataTypeUInt64>()},
        {"keep_free_inodes", std::make_shared<DataTypeUInt64>()},
        {"unreserved_inode", std::make_shared<DataTypeUInt64>()},
        {"type", std::make_shared<DataTypeString>()},
    }));
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemDisks::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    storage_snapshot->check(column_names);

    MutableColumnPtr col_name = ColumnString::create();
    MutableColumnPtr col_id = ColumnUInt64::create();
    MutableColumnPtr col_path = ColumnString::create();
    MutableColumnPtr col_free_bytes = ColumnUInt64::create();
    MutableColumnPtr col_total_bytes = ColumnUInt64::create();
    MutableColumnPtr col_keep_bytes = ColumnUInt64::create();
    MutableColumnPtr col_unreserved_bytes = ColumnUInt64::create();
    MutableColumnPtr col_free_inodes = ColumnUInt64::create();
    MutableColumnPtr col_total_inodes = ColumnUInt64::create();
    MutableColumnPtr col_keep_inodes = ColumnUInt64::create();
    MutableColumnPtr col_unreserved_inodes = ColumnUInt64::create();
    MutableColumnPtr col_type = ColumnString::create();

    for (const auto & [disk_name, disk_ptr] : context->getDisksMap())
    {
        col_name->insert(disk_name);
        col_id->insert(disk_ptr->getID());
        col_path->insert(disk_ptr->getPath());
        col_free_bytes->insert(disk_ptr->getAvailableSpace().bytes);
        col_total_bytes->insert(disk_ptr->getTotalSpace().bytes);
        col_keep_bytes->insert(disk_ptr->getKeepingFreeSpace().bytes);
        col_unreserved_bytes->insert(disk_ptr->getUnreservedSpace().bytes);
        col_free_inodes->insert(disk_ptr->getAvailableSpace().inodes);
        col_total_inodes->insert(disk_ptr->getTotalSpace().inodes);
        col_keep_inodes->insert(disk_ptr->getKeepingFreeSpace().inodes);
        col_unreserved_inodes->insert(disk_ptr->getUnreservedSpace().inodes);
        col_type->insert(DiskType::toString(disk_ptr->getType()));
    }

    Columns res_columns;
    res_columns.emplace_back(std::move(col_name));
    res_columns.emplace_back(std::move(col_id));
    res_columns.emplace_back(std::move(col_path));
    res_columns.emplace_back(std::move(col_free_bytes));
    res_columns.emplace_back(std::move(col_total_bytes));
    res_columns.emplace_back(std::move(col_keep_bytes));
    res_columns.emplace_back(std::move(col_unreserved_bytes));
    res_columns.emplace_back(std::move(col_free_inodes));
    res_columns.emplace_back(std::move(col_total_inodes));
    res_columns.emplace_back(std::move(col_keep_inodes));
    res_columns.emplace_back(std::move(col_unreserved_inodes));
    res_columns.emplace_back(std::move(col_type));

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(storage_snapshot->metadata->getSampleBlock(), std::move(chunk)));
}

}
