#include <boost/rational.hpp>   /// For calculations related to sampling coefficients.
#include <optional>
#include <random>
#include <algorithm>

#include <Poco/File.h>

#include <Storages/Hive/HiveDataSelectExecutor.h>
#include <Common/FieldVisitors.h>
#include <common/scope_guard.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/HDFS/ReadBufferFromByteHDFS.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Interpreters/Context.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <Storages/StorageCloudHive.h>
#include <Storages/MergeTree/CnchHiveReadPool.h>
#include <Storages/MergeTree/CnchHiveThreadSelectBlockInputProcessor.h>
#include <QueryPlan/ReadFromCnchHive.h>


namespace DB
{

HiveDataSelectExecutor::HiveDataSelectExecutor(const StorageCloudHive & data_)
    : data(data_), log(&Poco::Logger::get("HiveDataSelectExecutor"))
{
}

QueryPlanPtr HiveDataSelectExecutor::read(
    const Names & column_names_to_return,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    ContextPtr & context,
    UInt64 max_block_size,
    size_t num_streams) const
{

    LOG_TRACE(log, " HiveDataSelectExecutor::read ");
    // Stopwatch stopwatch;
    // SCOPE_EXIT({
    //     LOG_TRACE(
    //         log,
    //         "Constructed input stream, elapsed " << std::fixed << std::setprecision(2) << stopwatch.elapsedMicroseconds() / 1000.0
    //                                              << " ms.");
    // });

    auto data_parts = data.getDataPartsVector();

    LOG_DEBUG(log, "CloudHive Loaded  {} parts ", data_parts.size());

    if(data_parts.empty())
        return {};

    Names real_column_names = column_names_to_return;
    NamesAndTypesList available_real_columns = metadata_snapshot->getColumns().getAllPhysical();
    if (real_column_names.empty())
        real_column_names.push_back(ExpressionActions::getSmallestColumn(available_real_columns));

    metadata_snapshot->check(real_column_names, data.getVirtuals(), data.getStorageID());

    auto read_from_cnch_hive = std::make_unique<ReadFromCnchHive>(
        data_parts,
        real_column_names,
        data,
        query_info,
        metadata_snapshot,
        context,
        max_block_size,
        num_streams,
        log
    );


    QueryPlanPtr plan = std::make_unique<QueryPlan>();
    plan->addStep(std::move(read_from_cnch_hive));
    return plan;
}

}
