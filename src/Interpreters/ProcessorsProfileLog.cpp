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

#include <Interpreters/ProcessorsProfileLog.h>

#include <Common/ClickHouseRevision.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <common/logger_useful.h>

#include <array>

namespace DB
{

NamesAndTypesList ProcessorProfileLogElement::getNamesAndTypes()
{
    return
    {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},

        {"id", std::make_shared<DataTypeUInt64>()},
        {"parent_ids", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
        {"plan_step", std::make_shared<DataTypeUInt64>()},
        {"plan_group", std::make_shared<DataTypeUInt64>()},

        {"query_id", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"elapsed_us", std::make_shared<DataTypeUInt64>()},
        {"input_wait_elapsed_us", std::make_shared<DataTypeUInt64>()},
        {"output_wait_elapsed_us", std::make_shared<DataTypeUInt64>()},
        {"input_rows", std::make_shared<DataTypeUInt64>()},
        {"input_bytes", std::make_shared<DataTypeUInt64>()},
        {"output_rows", std::make_shared<DataTypeUInt64>()},
        {"output_bytes", std::make_shared<DataTypeUInt64>()},
    };
}

void ProcessorProfileLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(DateLUT::serverTimezoneInstance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);

    columns[i++]->insert(id);
    {
        Array parent_ids_array;
        parent_ids_array.reserve(parent_ids.size());
        for (const UInt64 parent : parent_ids)
            parent_ids_array.emplace_back(parent);
        columns[i++]->insert(parent_ids_array);
    }

    columns[i++]->insert(plan_step);
    columns[i++]->insert(plan_group);
    columns[i++]->insertData(query_id.data(), query_id.size());
    columns[i++]->insertData(processor_name.data(), processor_name.size());
    columns[i++]->insert(elapsed_us);
    columns[i++]->insert(input_wait_elapsed_us);
    columns[i++]->insert(output_wait_elapsed_us);
    columns[i++]->insert(input_rows);
    columns[i++]->insert(input_bytes);
    columns[i++]->insert(output_rows);
    columns[i++]->insert(output_bytes);
}

ProcessorsProfileLog::ProcessorsProfileLog(ContextPtr context_, const String & database_name_,
        const String & table_name_, const String & storage_def_,
        size_t flush_interval_milliseconds_)
  : SystemLog<ProcessorProfileLogElement>(context_, database_name_, table_name_,
        storage_def_, flush_interval_milliseconds_)
{
}

void ProcessorsProfileLog::addLogs(const QueryPipeline *pipeline, const String& query_id, UInt64 event_time, UInt64 event_time_microseconds, int segment_id)
{
    ProcessorProfileLogElement processor_elem;
    processor_elem.event_time = event_time;
    processor_elem.event_time_microseconds = event_time_microseconds;
    processor_elem.query_id = query_id;

    auto get_proc_id = [](const IProcessor & proc) -> UInt64
    {
        return reinterpret_cast<std::uintptr_t>(&proc);
    };
    for (const auto & processor : pipeline->getProcessors())
    {
        std::vector<UInt64> parents;
        for (const auto & port : processor->getOutputs())
        {
            if (!port.isConnected())
                continue;
            const IProcessor & next = port.getInputPort().getProcessor();
            parents.push_back(get_proc_id(next));
        }

        processor_elem.id = get_proc_id(*processor);
        processor_elem.parent_ids = std::move(parents);
        processor_elem.plan_step = reinterpret_cast<std::uintptr_t>(processor->getQueryPlanStep());
        /// plan_group is set differently to community CH,
        /// which is processor->getQueryPlanStepGroup();
        /// here, it is combined with the segment_id and
        /// invoking count for visualizing processors in the profiling website
        uint64_t count = processor->getWorkCount();
        processor_elem.plan_group = processor->getQueryPlanStepGroup() | (segment_id << 16) | (count << 32);

        processor_elem.processor_name = processor->getName();

        processor_elem.elapsed_us = processor->getElapsedUs();
        processor_elem.input_wait_elapsed_us = processor->getInputWaitElapsedUs();
        processor_elem.output_wait_elapsed_us = processor->getOutputWaitElapsedUs();

        auto stats = processor->getProcessorDataStats();
        processor_elem.input_rows = stats.input_rows;
        processor_elem.input_bytes = stats.input_bytes;
        processor_elem.output_rows = stats.output_rows;
        processor_elem.output_bytes = stats.output_bytes;

        add(processor_elem);
    }
}

}
