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

#include <Common/Logger.h>
#include <vector>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <Processors/Exchange/DataTrans/DataTrans_fwd.h>
#include <Poco/Logger.h>


namespace DB
{
class ExchangeBufferedSender
{
public:
    ExchangeBufferedSender(const Block & header, BroadcastSenderPtr sender_, UInt64 threshold_in_bytes, UInt64 threshold_in_row_num);
    void appendSelective(size_t column_idx, const IColumn & source, const IColumn::Selector & selector, size_t from, size_t length);
    BroadcastStatus sendThrough(Chunk chunk);
    BroadcastStatus flush(bool force, const ChunkInfoPtr & chunk_info);
private:
    const Block & header;
    size_t column_num;
    BroadcastSenderPtr sender;
    UInt64 threshold_in_bytes;
    UInt64 threshold_in_row_num;
    MutableColumns partition_buffer;
    LoggerPtr logger;
    void resetBuffer();
    inline size_t bufferBytes() const;
};

using ExchangeBufferedSenders = std::vector<ExchangeBufferedSender>;

}
