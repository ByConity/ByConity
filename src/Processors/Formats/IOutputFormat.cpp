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

#include <IO/WriteBuffer.h>
#include <Interpreters/DistributedStages/MPPQueryCoordinator.h>
#include <Processors/Formats/IOutputFormat.h>
#include <common/scope_guard.h>


namespace DB
{

IOutputFormat::IOutputFormat(const Block & header_, WriteBuffer & out_)
    : IProcessor({header_, header_, header_}, {}), out(out_)
{
}

IOutputFormat::Status IOutputFormat::prepare()
{
    if (has_input)
        return Status::Ready;

    for (auto kind : {Main, Totals, Extremes})
    {
        auto & input = getPort(kind);

        if (kind != Main && !input.isConnected())
            continue;

        if (input.isFinished())
            continue;

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        current_chunk = input.pull(true);
        current_block_kind = kind;
        has_input = true;
        return Status::Ready;
    }

    finished = true;

    if (!finalized)
        return Status::Ready;

    return Status::Finished;
}

Chunk IOutputFormat::prepareTotals(Chunk chunk)
{
    if (!chunk.hasRows())
        return {};

    if (chunk.getNumRows() > 1)
    {
        /// This may happen if something like ARRAY JOIN was executed on totals.
        /// Skip rows except the first one.
        auto columns = chunk.detachColumns();
        for (auto & column : columns)
            column = column->cut(0, 1);

        chunk.setColumns(std::move(columns), 1);
    }

    return chunk;
}

void IOutputFormat::work()
{
    if (!prefix_written)
    {
        doWritePrefix();
        prefix_written = true;
    }

    if (finished && !finalized)
    {
        if (rows_before_limit_counter && rows_before_limit_counter->hasAppliedLimit())
            setRowsBeforeLimit(rows_before_limit_counter->get());

        /// needed for http json, as out->onProgress(out is json format) is not set in coordinator's progress_callback
        if (coordinator)
        {
            coordinator->waitUntilAllPostProcessingRPCReceived();
            onProgress(coordinator->getFinalProgress());
        }

        finalize();
        finalized = true;
        return;
    }

    switch (current_block_kind)
    {
        case Main: {
            result_rows += current_chunk.getNumRows();
            auto bytes = current_chunk.allocatedBytes();
            result_bytes += bytes;
            consume(std::move(current_chunk));

            processMultiOutFileIfNeeded(bytes);
            break;
        }
        case Totals:
            if (auto totals = prepareTotals(std::move(current_chunk)))
                consumeTotals(std::move(totals));
            break;
        case Extremes:
            consumeExtremes(std::move(current_chunk));
            break;
    }

    if (auto_flush)
        flush();

    has_input = false;
}

void IOutputFormat::flush()
{
    out.next();
}

void IOutputFormat::closeFile()
{
    if (outfile_target)
        outfile_target->flushFile();
}

void IOutputFormat::write(const Block & block)
{
    auto bytes = block.allocatedBytes();
    consume(Chunk(block.getColumns(), block.rows()));

    processMultiOutFileIfNeeded(bytes);

    if (auto_flush)
        flush();
}

void IOutputFormat::setOutFileTarget(OutfileTargetPtr outfile_target_ptr)
{
    assert(outfile_target_ptr);
    outfile_target = outfile_target_ptr;
}

void IOutputFormat::processMultiOutFileIfNeeded(size_t bytes)
{
    if (!outfile_target || !outfile_target->outToMultiFile())
        return;

    outfile_target->accumulateBytes(bytes);

    if (outfile_target->needSplit())
    {
        outfile_target->flushFile();
        outfile_target->updateOutPathIfNeeded();

        /// for OutputFormat implementation like 'Parquet', there is a inner file descriptor,
        /// when out is reset, release the file descriptor either
        customReleaseBuffer();
        out.swap(*outfile_target->updateBuffer());
    }
}

}

