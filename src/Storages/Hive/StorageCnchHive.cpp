#include "Storages/Hive/StorageCnchHive.h"
#include "common/defines.h"
#if USE_HIVE

#include <Protos/lake_models.pb.h>
#include "CloudServices/CnchServerResource.h"
#include "DataStreams/narrowBlockInputStreams.h"
#include "Functions/FunctionFactory.h"
#include "Interpreters/ClusterProxy/SelectStreamFactory.h"
#include "Interpreters/ClusterProxy/executeQuery.h"
#include "Interpreters/InterpreterSelectQuery.h"
#include "Interpreters/PushFilterToStorage.h"
#include "Interpreters/SelectQueryOptions.h"
#include "Interpreters/evaluateConstantExpression.h"
#include "Interpreters/trySetVirtualWarehouse.h"
#include "MergeTreeCommon/CnchStorageCommon.h"
#include "Optimizer/PredicateUtils.h"
#include "Optimizer/SelectQueryInfoHelper.h"
#include "Parsers/ASTClusterByElement.h"
#include "Parsers/ASTCreateQuery.h"
#include "Parsers/ASTLiteral.h"
#include "Parsers/ASTSelectQuery.h"
#include "Parsers/ASTSetQuery.h"
#include "Parsers/formatAST.cpp"
#include "Parsers/queryToString.h"
#include "Processors/Sources/NullSource.h"
#include "QueryPlan/BuildQueryPipelineSettings.h"
#include "QueryPlan/Optimizations/QueryPlanOptimizationSettings.h"
#include "QueryPlan/ReadFromPreparedSource.h"
#include "ResourceManagement/CommonData.h"
#include "Storages/AlterCommands.h"
#include "Storages/DataLakes/HudiDirectoryLister.h"
#include "Storages/Hive/CnchHiveSettings.h"
#include "Storages/Hive/DirectoryLister.h"
#include "Storages/Hive/HivePartition.h"
#include "Storages/Hive/HiveSchemaConverter.h"
#include "Storages/Hive/HiveVirtualColumns.h"
#include "Storages/Hive/HiveWhereOptimizer.h"
#include "Storages/Hive/Metastore/HiveMetastore.h"
#include "Storages/MergeTree/MergeTreeWhereOptimizer.h"
#include "Storages/MergeTree/PartitionPruner.h"
#include "Storages/StorageFactory.h"
#include "Storages/StorageInMemoryMetadata.h"
#include "Storages/DataLakes/HudiDirectoryLister.h"

#include <boost/lexical_cast.hpp>
#include <thrift/TToString.h>
#include "common/scope_guard_safe.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_FORMAT;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TOO_MANY_PARTITIONS;
    extern const int INDEX_NOT_USED;
}

static std::optional<UInt64> get_file_hash_index(const String & hive_file_path)
{
    auto get_hash_index_from_position = [&hive_file_path](size_t pos) -> std::optional<Int64> {
        size_t l = pos, r = pos;
        for (; r < hive_file_path.size(); ++r)
        {
            if (!std::isdigit(hive_file_path[r]))
                break;
        }
        if (l < r)
            return std::stoi(hive_file_path.substr(l, r - l));
        return {};
    };

    /// This is special format used by tea
    /// part-00000-5cf7580f-a3f6-4beb-90a6-e9f4de61c887_00003.c000
    /// 00003 : part hash index
    if (auto res = get_hash_index_from_position(hive_file_path.find_last_of('_') + 1); res)
        return res;

    /// The naming convention has the bucket number as the start of the file name
    /// Used mostly by Hive and Trino
    /// /000003_0_66add4ef-d1fc-4015-87b4-6962de044323_20240229_033029_00033_erdcf
    if (auto res = get_hash_index_from_position(hive_file_path.find_last_of('/') + 1); res)
        return res;

    return {};
}

StorageCnchHive::StorageCnchHive(
    const StorageID & table_id_,
    const String & hive_metastore_url_,
    const String & hive_db_name_,
    const String & hive_table_name_,
    std::optional<StorageInMemoryMetadata> metadata_,
    ContextPtr context_,
    IMetaClientPtr meta_client,
    std::shared_ptr<CnchHiveSettings> settings_)
    : StorageCnchLakeBase(table_id_, hive_db_name_, hive_table_name_, context_, settings_)
    , hive_metastore_url(hive_metastore_url_)
    , hive_client(meta_client)
{
    if (metadata_)
        initialize(*metadata_);
}

void StorageCnchHive::setHiveMetaClient(const IMetaClientPtr & client)
{
    hive_client = client;
}

void StorageCnchHive::initialize(StorageInMemoryMetadata metadata_)
{
    try
    {
        if (!hive_client)
            hive_client = LakeMetaClientFactory::create(hive_metastore_url, storage_settings);

        hive_table = hive_client->getTable(db_name, table_name);
    }
    catch (...)
    {
        hive_exception = std::current_exception();
        return;
    }

    HiveSchemaConverter converter(getContext(), hive_table);
    if (metadata_.columns.empty())
    {
        converter.convert(metadata_);
        setInMemoryMetadata(metadata_);
    }
    else
    {
        converter.check(metadata_);
        setInMemoryMetadata(metadata_);
    }
}

void StorageCnchHive::startup()
{
    /// for some reason, we do not what to throw exceptions in ctor
    if (hive_exception)
    {
        std::rethrow_exception(hive_exception);
    }
}

size_t StorageCnchHive::maxStreams(ContextPtr local_context) const
{
    /**
     * With distributed query processing, almost no computations are done in the threads,
     *  but wait and receive data from remote servers.
     *  If we have 20 remote servers, and max_threads = 8, then it would not be very good
     *  connect and ask only 8 servers at a time.
     *  To simultaneously query more remote servers,
     *  instead of max_threads, max_distributed_connections is used.
     */
    return local_context->getSettingsRef().max_distributed_connections;
}

PrepareContextResult StorageCnchHive::prepareReadContext(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr & local_context,
    unsigned num_streams)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());
    HivePartitions partitions = selectPartitions(local_context, metadata_snapshot, query_info);

    const auto & settings = local_context->getSettingsRef();
    if (settings.max_partitions_to_read > 0)
    {
        if (partitions.size() > static_cast<size_t>(settings.max_partitions_to_read))
        {
            throw Exception(
                ErrorCodes::TOO_MANY_PARTITIONS,
                "Too many partitions to read. Current {}, max {}",
                partitions.size(),
                settings.max_partitions_to_read);
        }
    }
    LakeScanInfos lake_scan_infos;
    std::mutex mu;

    auto lister = getDirectoryLister(local_context);
    auto list_partition = [&](const HivePartitionPtr & partition) {
        LakeScanInfos files = lister->list(partition);
        {
            std::lock_guard lock(mu);
            lake_scan_infos.insert(lake_scan_infos.end(), std::make_move_iterator(files.begin()), std::make_move_iterator(files.end()));
        }
    };

    if (num_streams <= 1 || partitions.size() == 1)
    {
        for (const auto & partition : partitions)
        {
            list_partition(partition);
        }
    }
    else
    {
        size_t num_threads = std::min(static_cast<size_t>(num_streams), partitions.size());

        ThreadPool pool(num_threads);
        for (const auto & partition : partitions)
        {
            pool.scheduleOrThrowOnError([&, partition, thread_group = CurrentThread::getGroup()] {
                SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachQueryIfNotDetached(););
                if (thread_group)
                    CurrentThread::attachTo(thread_group);
                list_partition(partition);
            });
        }
        pool.wait();
    }

    size_t total_scan_infos = lake_scan_infos.size();
    if (isBucketTable() && settings.use_hive_cluster_key_filter)
    {
        auto required_bucket = getSelectedBucketNumber(local_context, query_info, metadata_snapshot);

        /// set bucket id for hive files
        std::for_each(lake_scan_infos.begin(), lake_scan_infos.end(), [](auto & lake_scan_info) {
            auto hash_index = get_file_hash_index(lake_scan_info->identifier());
            if (!hash_index)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "failed to parse hash index from hive file path {}", lake_scan_info->identifier());
            lake_scan_info->setDistributionId(*hash_index);
        });

        /// prune files with bucket id
        auto end = std::remove_if(lake_scan_infos.begin(), lake_scan_infos.end(), [&](auto & lake_scan_info) {
            if (!required_bucket)
                return false;
            return *lake_scan_info->getDistributionId() != *required_bucket;
        });
        lake_scan_infos.erase(end, lake_scan_infos.end());
    }

    LOG_DEBUG(log, "Read from {}/{} hive files", lake_scan_infos.size(), total_scan_infos);
    PrepareContextResult result{.lake_scan_infos = std::move(lake_scan_infos)};

    collectResource(local_context, result);
    return result;
}

NamesAndTypesList StorageCnchHive::getVirtuals() const
{
    return getHiveVirtuals();
}

ASTPtr StorageCnchHive::applyFilter(
    ASTPtr query_filter, SelectQueryInfo & query_info, ContextPtr local_context, PlanNodeStatisticsPtr storage_statistics) const
{
    const auto & settings = local_context->getSettingsRef();
    auto * select_query = query_info.getSelectQuery();
    PushFilterToStorage push_filter_to_storage(shared_from_this(), local_context);
    ASTs conjuncts;
    /// Set partition_filter
    /// this should be done before setting query.where() to avoid partition filters being chosen as prewhere
    if (settings.external_enable_partition_filter_push_down)
    {
        auto [push_predicates, remain_predicates] = push_filter_to_storage.extractPartitionFilter(query_filter, true);

        query_info.appendPartitonFilters(push_predicates);
        conjuncts.swap(remain_predicates);
    }

    /// Set query.where()
    // IStorage::applyFilter(PredicateUtils::combineConjuncts(conjuncts), query_info, local_context, storage_statistics);
    select_query->setExpression(ASTSelectQuery::Expression::WHERE, PredicateUtils::combineConjuncts(conjuncts));

    /// Set prewhere()
    if (supportsPrewhere() && settings.optimize_move_to_prewhere && select_query->where() && !select_query->prewhere()
        && (!select_query->final() || settings.optimize_move_to_prewhere_if_final))
    {
        if (HiveMoveToPrewhereMethod::ALL == settings.hive_move_to_prewhere_method)
        {
            select_query->setExpression(ASTSelectQuery::Expression::PREWHERE, PredicateUtils::combineConjuncts(conjuncts));
        }
        else if (HiveMoveToPrewhereMethod::COLUMN_SIZE == settings.hive_move_to_prewhere_method)
        {
            /// PREWHERE optimization: transfer some condition from WHERE to PREWHERE if enabled and viable
            if (const auto & column_sizes_copy = getColumnSizes(); !column_sizes_copy.empty())
            {
                /// Extract column compressed sizes.
                std::unordered_map<std::string, UInt64> column_compressed_sizes;
                for (const auto & [name, sizes] : column_sizes_copy)
                    column_compressed_sizes[name] = sizes.data_compressed;

                auto current_info = buildSelectQueryInfoForQuery(query_info.query, local_context);
                MergeTreeWhereOptimizer{
                    current_info,
                    local_context,
                    std::move(column_compressed_sizes),
                    getInMemoryMetadataPtr(),
                    current_info.syntax_analyzer_result->requiredSourceColumns(),
                    getLogger("OptimizerEarlyPrewherePushdown")};
            }
        }
        else if (HiveMoveToPrewhereMethod::STATS == settings.hive_move_to_prewhere_method)
        {
            if (storage_statistics)
            {
                auto [pre_conjuncts, where_conjuncts]
                    = push_filter_to_storage.extractPrewhereWithStats(select_query->getWhere(), storage_statistics);

                if (!pre_conjuncts.empty())
                    select_query->setExpression(ASTSelectQuery::Expression::PREWHERE, PredicateUtils::combineConjuncts(pre_conjuncts));

                if (!where_conjuncts.empty())
                    select_query->setExpression(ASTSelectQuery::Expression::WHERE, PredicateUtils::combineConjuncts(where_conjuncts));
                else
                    select_query->setExpression(ASTSelectQuery::Expression::WHERE, nullptr);
            }
        }
        else if (HiveMoveToPrewhereMethod::NEVER == settings.hive_move_to_prewhere_method)
        {
            // do nothing
        }
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unimplemented move to prewhere method");
    }

    /// remove prewhere from query plan
    if (auto prewhere = select_query->prewhere())
        PredicateUtils::subtract(conjuncts, PredicateUtils::extractConjuncts(prewhere));

    return PredicateUtils::combineConjuncts(conjuncts);
}

static void filterPartitions(
    const ASTPtr & partition_filter,
    const StorageMetadataPtr & storage_metadata,
    ContextPtr local_context,
    HivePartitions & hive_partitions)
{
    chassert(partition_filter);
    chassert(storage_metadata->hasPartitionKey());

    const auto & partition_key = storage_metadata->getPartitionKey();

    MutableColumns columns = partition_key.sample_block.cloneEmptyColumns();
    for (auto & col : columns)
        col->reserve(hive_partitions.size());

    auto index_column = ColumnUInt32::create();
    index_column->reserve(hive_partitions.size());
    int idx = 0;
    for (const auto & partition : hive_partitions)
    {
        chassert(columns.size() == partition->value.size());
        for (size_t i = 0; i < columns.size(); i++)
            columns[i]->insert(partition->value[i]);
        index_column->insert(idx++);
    }

    Block block_with_filter = partition_key.sample_block.cloneWithColumns(std::move(columns));
    {
        BuildQueryPipelineSettings settings;
        settings.fromContext(local_context);
        auto action_dag = IQueryPlanStep::createFilterExpressionActions(local_context, partition_filter, partition_key.sample_block);
        auto expression = std::make_shared<ExpressionActions>(action_dag, settings.getActionsSettings());
        expression->execute(block_with_filter);
    }

    String filter_column_name = partition_filter->getColumnName();
    ColumnPtr filter_column = block_with_filter.getByName(filter_column_name).column;

    ConstantFilterDescription constant_filter(*filter_column);
    if (constant_filter.always_true)
    {
        return;
    }
    else if (constant_filter.always_false)
    {
        hive_partitions.clear();
        return;
    }

    FilterDescription filter(*filter_column);
    auto result_column = filter.filter(*index_column, -1);
    std::unordered_set<int> result_set;
    for (size_t i = 0; i < result_column->size(); i++)
    {
        result_set.emplace(result_column->getUInt(i));
    }

    idx = 0;
    std::erase_if(hive_partitions, [&result_set, &idx](const auto &) { return result_set.find(idx++) == result_set.end(); });
}

HivePartitions StorageCnchHive::selectPartitions(
    ContextPtr local_context, const StorageMetadataPtr & metadata_snapshot, const SelectQueryInfo & query_info)
{
    /// non-partition table
    if (!metadata_snapshot->hasPartitionKey())
    {
        auto partition = std::make_shared<HivePartition>();
        partition->load(hive_table->sd);
        return {partition};
    }

    String filter_str;
    ASTPtr partition_filter;
    if (query_info.partition_filter)
    {
        HiveWhereOptimizer where_optimizer(metadata_snapshot, query_info.partition_filter);
        filter_str = where_optimizer.partition_key_conds == nullptr ? "" : serializeASTWithOutAlias(*where_optimizer.partition_key_conds);
        partition_filter = query_info.partition_filter;
    }
    else if (auto filters = getFilterFromQueryInfo(query_info, false); filters)
    {
        HiveWhereOptimizer where_optimizer(metadata_snapshot, filters);
        filter_str = where_optimizer.partition_key_conds == nullptr ? "" : serializeASTWithOutAlias(*where_optimizer.partition_key_conds);
        partition_filter = where_optimizer.partition_key_conds;
    }

    // runtime filters here has not been rewriten rewritten to be executable
    if (partition_filter)
        partition_filter = RuntimeFilterUtils::removeAllInternalRuntimeFilters(partition_filter);

    Stopwatch watch;
    // push filter to hive metastore
    auto apache_partitions = hive_client->getPartitionsByFilter(db_name, table_name, filter_str);
    auto fetch_partitions_elapsed = watch.elapsedMilliseconds();

    watch.restart();
    // eval filters that cannot be pushed, i.e., expression with functions.

    HivePartitions hive_partitions;
    hive_partitions.reserve(apache_partitions.size());
    for (const auto & apache_partition : apache_partitions)
    {
        auto & partition = *hive_partitions.emplace_back(std::make_shared<HivePartition>());
        partition.load(apache_partition, metadata_snapshot->getPartitionKey());
    }

    if (partition_filter)
    {
        filterPartitions(partition_filter, metadata_snapshot, local_context, hive_partitions);
    }

    auto filter_partitions_elapsed = watch.elapsedMilliseconds();

    LOG_DEBUG(
        log,
        "filtered partition {}/{}, filter_str={}, partition_filter={}, elapsed {} ms to fetch partitions, elpased {} ms to filter "
        "partitions",
        hive_partitions.size(),
        apache_partitions.size(),
        filter_str,
        query_info.partition_filter ? queryToString(query_info.partition_filter) : "",
        fetch_partitions_elapsed,
        filter_partitions_elapsed);

    if (local_context->getSettingsRef().force_index_by_date && filter_str.empty() && hive_partitions.size() == apache_partitions.size())
    {
        throw Exception(
            ErrorCodes::INDEX_NOT_USED,
            "No partitions get filtered (total {}) and setting 'force_index_by_date' is set",
            hive_partitions.size());
    }

    return hive_partitions;
}

std::optional<UInt64> StorageCnchHive::getSelectedBucketNumber(
    [[maybe_unused]] ContextPtr local_context,
    [[maybe_unused]] SelectQueryInfo & query_info,
    const StorageMetadataPtr & metadata_snapshot) const
{
    if (!isBucketTable())
        return {};

    HiveWhereOptimizer optimizer(metadata_snapshot, getFilterFromQueryInfo(query_info, false));
    if (!optimizer.cluster_key_conds)
        return {};

    ExpressionActionsPtr cluster_by_expression = metadata_snapshot->cluster_by_key.expression;
    const auto & required_cols = cluster_by_expression->getRequiredColumnsWithTypes();
    Block block;
    for (const auto & item : required_cols)
        block.insert(ColumnWithTypeAndName{item.type, item.name});

    MutableColumns columns = block.mutateColumns();
    ASTPtr cluster_by_conds = optimizer.cluster_key_conds;
    LOG_DEBUG(
        log,
        "Useful cluster by conditions {}. Cluster key actions {}. Input block {}",
        queryToString(cluster_by_conds),
        cluster_by_expression->dumpActions(),
        block.dumpStructure());

    std::function<void(ASTPtr)> parse_cluster_by_cond = [&](const ASTPtr & ast) -> void {
        auto * func = ast->as<ASTFunction>();
        if (!func || !func->arguments)
            return;

        if (func->name == "equals" && func->arguments->children.size() == 2)
        {
            ASTPtr column = evaluateConstantExpressionOrIdentifierAsLiteral(func->arguments->children[0], local_context);
            ASTPtr field = evaluateConstantExpressionOrIdentifierAsLiteral(func->arguments->children[1], local_context);

            String column_name = column->as<ASTLiteral &>().value.safeGet<String>();
            auto & value = field->as<ASTLiteral &>().value;
            if (block.has(column_name))
            {
                size_t pos = block.getPositionByName(column_name);
                if (columns[pos]->empty())
                    columns[pos]->insert(value);
            }
        }
        else if (func->name == "and")
        {
            for (const auto & child : func->arguments->children)
            {
                parse_cluster_by_cond(child);
            }
        }
    };
    parse_cluster_by_cond(cluster_by_conds);

    if (std::any_of(columns.begin(), columns.end(), [](auto & column) { return column->empty(); }))
        return {};

    block.setColumns(std::move(columns));
    cluster_by_expression->execute(block);
    String result_column_name = metadata_snapshot->cluster_by_key.expression_list_ast->children[0]->getColumnName();
    auto result_column = block.getByName(result_column_name).column;
    UInt64 required_bucket = result_column->get64(0);
    LOG_DEBUG(log, "result column: {} required bucket hash index is {}", result_column_name, required_bucket);
    return required_bucket;
}

std::optional<TableStatistics> StorageCnchHive::getTableStats(const Strings & columns, ContextPtr local_context)
{
    bool merge_partition_stats = local_context->getSettingsRef().merge_partition_stats;

    auto stats = hive_client->getTableStats(db_name, table_name, columns, merge_partition_stats);
    if (stats)
        LOG_TRACE(log, "row_count {}", stats->row_count);
    else
        LOG_TRACE(log, "no stats");
    return stats;
}

std::vector<std::pair<String, UInt64>>
StorageCnchHive::getPartitionLastModificationTime(const StorageMetadataPtr & metadata_snapshot, bool binary_format)
{
    String filter = {};
    auto apache_hive_partitions = hive_client->getPartitionsByFilter(db_name, table_name, filter);
    std::vector<std::pair<String, UInt64>> partition_last_modification_times;
    partition_last_modification_times.reserve(apache_hive_partitions.size());
    for (const auto & apache_partition : apache_hive_partitions)
    {
        auto partition = std::make_shared<HivePartition>();
        partition->load(apache_partition, metadata_snapshot->getPartitionKey());
        if (binary_format)
        {
            String partition_str;
            WriteBufferFromString write_buffer(partition_str);
            partition->store(write_buffer, metadata_snapshot->getPartitionKey());
            partition_last_modification_times.emplace_back(partition_str, apache_partition.createTime);
        }
        else
            partition_last_modification_times.emplace_back(partition->partition_id, apache_partition.createTime);
    }
    return partition_last_modification_times;
}

std::shared_ptr<IDirectoryLister> StorageCnchHive::getDirectoryLister(ContextPtr local_context)
{
    auto disk = HiveUtil::getDiskFromURI(hive_table->sd.location, local_context, *storage_settings);
    const auto & input_format = hive_table->sd.inputFormat;
    if (input_format == "org.apache.hudi.hadoop.HoodieParquetInputFormat")
    {
        return std::make_shared<HudiCowDirectoryLister>(disk);
    }
    else if (input_format == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
    {
        return std::make_shared<DiskDirectoryLister>(disk, ILakeScanInfo::StorageType::Hive, FileScanInfo::FormatType::PARQUET);
    }
    else if (input_format == "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
    {
        return std::make_shared<DiskDirectoryLister>(disk, ILakeScanInfo::StorageType::Hive, FileScanInfo::FormatType::ORC);
    }
    else
        throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Unknown hive format {}", input_format);
}

void registerStorageCnchHive(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_projections = true,
        .supports_sort_order = true,
        .supports_schema_inference = true,
    };

    factory.registerStorage(
        "CnchHive",
        [](const StorageFactory::Arguments & args) {
            ASTs & engine_args = args.engine_args;
            if (engine_args.size() != 3)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Storage CnchHive require 3 arguments: hive_metastore_url, hive_db_name and hive_table_name.");

            for (auto & engine_arg : engine_args)
                engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

            String hive_metastore_url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
            String hive_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
            String hive_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();

            StorageInMemoryMetadata metadata;
            std::shared_ptr<CnchHiveSettings> hive_settings = std::make_shared<CnchHiveSettings>(args.getContext()->getCnchHiveSettings());
            if (args.storage_def->settings)
            {
                hive_settings->loadFromQuery(*args.storage_def);
                metadata.settings_changes = args.storage_def->settings->ptr();
            }

            if (!args.columns.empty())
                metadata.setColumns(args.columns);

            metadata.setComment(args.comment);

            if (args.storage_def->partition_by)
            {
                ASTPtr partition_by_key = args.storage_def->partition_by->ptr();
                metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_key, metadata.columns, args.getContext());
            }

            if (args.storage_def->cluster_by)
            {
                ASTPtr cluster_by_ast = args.storage_def->cluster_by->ptr();
                chassert(cluster_by_ast->children.size() == 2);
                auto bucket_num = cluster_by_ast->children[1];
                auto func_hash = makeASTFunction("javaHash", cluster_by_ast->children[0]);
                auto func_mod = makeASTFunction("hiveModulo", ASTs{func_hash, bucket_num});
                auto cluster_by_key = std::make_shared<ASTClusterByElement>(func_mod, bucket_num, -1, false, false);
                metadata.cluster_by_key = KeyDescription::getClusterByKeyFromAST(cluster_by_key, metadata.columns, args.getContext());
            }

            return StorageCnchHive::create(
                args.table_id, hive_metastore_url, hive_database, hive_table, metadata, args.getContext(), args.hive_client, hive_settings);
        },
        features);
}
}
#endif
