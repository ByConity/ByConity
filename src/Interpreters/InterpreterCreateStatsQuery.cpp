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

#include <variant>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/InterpreterCreateStatsQuery.h>
#include <Parsers/ASTStatsQuery.h>
#include <Protos/optimizer_statistics.pb.h>
#include <Statistics/ASTHelpers.h>
#include <Statistics/AutoStatsTaskLogHelper.h>
#include <Statistics/CollectTarget.h>
#include <Statistics/StatisticsCollector.h>
#include <Statistics/StatsTableBasic.h>
#include <Statistics/TypeUtils.h>
#include <Poco/Exception.h>
#include <Common/Stopwatch.h>
#include <Statistics/ASTHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int INCORRECT_DATA;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int QUERY_WAS_CANCELLED;
}
using namespace Statistics;
using namespace Statistics::AutoStats;

static Block constructInfoBlock(
    ContextPtr context, const String & table_name, UInt64 column_count, String row_count_or_error, double time, bool only_header = false)
{
    Block block;
    auto append_str_column = [&](String header, String value) {
        ColumnWithTypeAndName tuple;
        tuple.name = header;
        tuple.type = std::make_shared<DataTypeString>();
        auto col = tuple.type->createColumn();
        if (!only_header)
        {
            col->insertData(value.data(), value.size());
        }
        tuple.column = std::move(col);
        block.insert(std::move(tuple));
    };

    auto append_num_column = [&]<typename T>(String header, T value) {
        static_assert(std::is_trivial_v<T>);
        ColumnWithTypeAndName tuple;
        tuple.name = header;
        tuple.type = std::make_shared<DataTypeNumber<T>>();
        auto col = ColumnVector<T>::create();
        if (!only_header)
        {
            col->insertValue(value);
        }
        tuple.column = std::move(col);
        block.insert(std::move(tuple));
    };

    append_str_column("table_name", table_name);
    append_num_column("column_count", column_count);
    append_str_column("row_count_or_error", row_count_or_error);
    if (context->getSettingsRef().create_stats_time_output)
    {
        append_num_column("elapsed_time", time);
    }
    return block;
}


namespace
{
    class CreateStatsBlockInputStream : public IBlockInputStream, WithContext
    {
    public:
        CreateStatsBlockInputStream(ContextPtr context_, std::vector<CollectTarget> collect_targets_)
            : WithContext(context_), collect_targets(std::move(collect_targets_))
        {
        }
        String getName() const override { return "Statistics"; }
        Block getHeader() const override
        {
            auto sample_block = constructInfoBlock(getContext(), "", 0, "", 0, true);
            return sample_block;
        }

    private:
        Block readImpl() override
        {
            auto context = getContext();
            Stopwatch watch;
            auto logger = getLogger("CreateStats");
            while (counter < collect_targets.size())
            {
                auto collect_target = collect_targets.at(counter++);
                auto exception_handler = [&] {
                    auto elapsed_time = watch.elapsedSeconds();
                    auto err_info_with_stack = getCurrentExceptionMessage(true);
                    LOG_ERROR(logger, err_info_with_stack);

                    auto err_info = getCurrentExceptionMessage(false);
                    error_infos.emplace(collect_target.table_identifier.getDbTableName(), err_info_with_stack);

                    return constructInfoBlock(
                        context,
                        collect_target.table_identifier.getTableName(),
                        collect_target.columns_desc.size(),
                        err_info,
                        elapsed_time);
                };

                try
                {
                    auto row_count_opt = collectStatsOnTarget(context, collect_target);
                    if (!row_count_opt)
                        continue;
                    auto row_count = row_count_opt.value();
                    auto elapsed_time = watch.elapsedSeconds();
                    return constructInfoBlock(
                        context,
                        collect_target.table_identifier.getTableName(),
                        collect_target.columns_desc.size(),
                        std::to_string(row_count),
                        elapsed_time);
                }
                catch (Poco::Exception & e)
                {
                    if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED)
                    {
                        LOG_INFO(logger, "create stast is cancelled");
                        throw;
                    }

                    return exception_handler();
                }
                catch (...)
                {
                    return exception_handler();
                }
            }

            if (error_infos.empty())
            {
                // succeed
                return {};
            }

            // handle errors
            String total_error;
            for (const auto & [k, v] : error_infos)
            {
                total_error += fmt::format(FMT_STRING("when collecting table {} having the following error: {}\n"), k, v);
            }
            throw Exception(total_error, ErrorCodes::INCORRECT_DATA);
        }

    private:
        std::map<String, String> error_infos;
        std::vector<CollectTarget> collect_targets;
        size_t counter = 0;
    };
}

static void submitAsyncTasks(ContextPtr context, const std::vector<CollectTarget> & collect_targets)
{
    for (const auto & target : collect_targets)
    {
        TaskInfoCore core{
            .task_uuid = UUIDHelpers::generateV4(),
            .task_type = TaskType::Manual,
            .table = target.table_identifier,
            .settings_json = target.settings.toJsonStr(),
            .stats_row_count = 0,
            .udi_count = 0,
            .priority = 100,
            .retry_times = 0,
            .status = Status::Created};
        if (target.implicit_all_columns)
            core.columns_name = {};
        else
        {
            core.columns_name.clear();
            for (const auto & col : target.columns_desc)
            {
                core.columns_name.emplace_back(col.name);
            }
        }

        AutoStats::writeTaskLog(context, core);
    }
}


CollectorSettings analyzeSettings(ContextPtr context, const ASTCreateStatsQuery * query)
{
    auto query_settings = context->getSettings();
    if (query->settings_changes_opt)
    {
        auto settings_changes = query->settings_changes_opt.value();
        applyStatisticsSettingsChanges(query_settings, std::move(settings_changes));
    }

    CollectorSettings settings;
    settings.fromContextSettings(query_settings);
    using SampleType = ASTCreateStatsQuery::SampleType;

    // old style to specify settings, maximun priority
    if (query->sample_type == SampleType::FullScan)
    {
        settings.set_enable_sample(false);
    }
    else if (query->sample_type == SampleType::Sample)
    {
        settings.set_enable_sample(true);

        if (query->sample_rows)
        {
            settings.set_sample_row_count(*query->sample_rows);
        }

        if (query->sample_ratio)
        {
            settings.set_sample_ratio(*query->sample_ratio);
        }
    }
    settings.set_if_not_exists(query->if_not_exists);

    return settings;
}

BlockIO InterpreterCreateStatsQuery::execute()
{
    auto context = getContext();
    const auto * query = query_ptr->as<const ASTCreateStatsQuery>();
    if (!query)
    {
        throw Exception("Create stats query logical error", ErrorCodes::LOGICAL_ERROR);
    }

    auto catalog = createCatalogAdaptor(context);
    catalog->checkHealth(/*is_write=*/true);

    CollectorSettings settings = analyzeSettings(context, query);

    auto tables = getTablesFromAST(context, query);
    std::vector<CollectTarget> valid_targets;
    for (const auto & table : tables)
    {
        if (catalog->isTableCollectable(table))
        {
            if (settings.if_not_exists() && catalog->hasStatsData(table))
            {
                // skip when if_not_exists is on
                continue;
            }
            CollectTarget target(context, table, settings, query->columns);
            valid_targets.emplace_back(std::move(target));
        }
    }

    if (valid_targets.empty())
    {
        return {};
    }

    using SyncMode = ASTCreateStatsQuery::SyncMode;
    auto use_sync_mode
        = query->sync_mode == SyncMode::Default ? context->getSettingsRef().statistics_enable_async : query->sync_mode == SyncMode::Async;

    if (use_sync_mode)
    {
        submitAsyncTasks(context, std::move(valid_targets));
        return {};
    }
    else
    {
        BlockIO io;
        io.in = std::make_shared<CreateStatsBlockInputStream>(context, std::move(valid_targets));
        return io;
    }
}
}
