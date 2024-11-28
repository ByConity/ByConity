#include "IStorageCnchFile.h"

#include <utility>
#include <CloudServices/CnchServerResource.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabasesCommon.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/WorkerGroupHandle.h>
#include <Interpreters/predicateExpressionsUtils.h>
#include <Interpreters/trySetVirtualWarehouse.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPlan/ISourceStep.h>
#include <QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPlan/ReadFromPreparedSource.h>
#include <ResourceManagement/CommonData.h>
#include <ServiceDiscovery/IServiceDiscovery.h>
#include <Storages/AlterCommands.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Storages/RemoteFile/CnchFileCommon.h>
#include <Storages/RemoteFile/IStorageCloudFile.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/getVirtualsForStorage.h>
#include <Common/Exception.h>
#include <Common/RemoteHostFilter.h>
#include <Common/parseAddress.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

/// Get basic select query to read from prepared pipe: remove prewhere, sampling, offset, final
static ASTPtr getBasicSelectQuery(const ASTPtr & original_query)
{
    auto query = original_query->clone();
    auto & select = query->as<ASTSelectQuery &>();
    auto & tables_in_select_query = select.refTables()->as<ASTTablesInSelectQuery &>();
    if (tables_in_select_query.children.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tables list is empty, it's a bug");
    auto & tables_element = tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();
    if (!tables_element.table_expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no table expression, it's a bug");
    tables_element.table_expression->as<ASTTableExpression &>().final = false;
    tables_element.table_expression->as<ASTTableExpression &>().sample_size = nullptr;
    tables_element.table_expression->as<ASTTableExpression &>().sample_offset = nullptr;

    /// TODO @canh: can we just throw prewhere away?
    if (select.prewhere() && select.where())
        select.setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("and", select.where(), select.prewhere()));
    else if (select.prewhere())
        select.setExpression(ASTSelectQuery::Expression::WHERE, select.prewhere()->clone());
    select.setExpression(ASTSelectQuery::Expression::PREWHERE, nullptr);
    return query;
}

IStorageCnchFile::IStorageCnchFile(
    ContextPtr context_,
    const StorageID & table_id_,
    const ColumnsDescription & required_columns_,
    const ConstraintsDescription & constraints_,
    const ASTPtr & setting_changes_,
    const CnchFileArguments & arguments_,
    const CnchFileSettings & settings_)
    : IStorage(table_id_)
    , WithMutableContext(context_->getGlobalContext())
    , CnchStorageCommonHelper(table_id_, table_id_.getDatabaseName(), table_id_.getTableName())
    , arguments(std::move(arguments_))
    , settings(std::move(settings_))
{
    context_->getRemoteHostFilter().checkURL(Poco::URI(arguments.url));

    if (arguments.format_name.empty())
        arguments.format_name = "auto";
    if (arguments.compression_method.empty())
        arguments.compression_method = "auto";

    StorageInMemoryMetadata metadata;
    metadata.setSettingsChanges(setting_changes_);
    metadata.setColumns(required_columns_);
    metadata.setConstraints(constraints_);
    setInMemoryMetadata(metadata);

    String path = arguments.url.substr(arguments.url.find('/', arguments.url.find("//") + 2));
    arguments.is_glob_path = path.find_first_of("*?{") != std::string::npos;

    file_list.emplace_back(arguments.url);

    auto default_virtuals = NamesAndTypesList{
        {"_path", std::make_shared<DataTypeString>()},
        {"_file", std::make_shared<DataTypeString>()},
        {"_size", std::make_shared<DataTypeUInt64>()}};
    virtual_columns = getVirtualsForStorage(metadata.getSampleBlock().getNamesAndTypesList(), default_virtuals);
    for (const auto & column : virtual_columns)
        virtual_header.insert({column.type->createColumn(), column.type, column.name});
}

Pipe IStorageCnchFile::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr query_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, storage_snapshot, query_info, query_context, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(
        QueryPlanOptimizationSettings::fromContext(query_context), BuildQueryPipelineSettings::fromContext(query_context));
}

void IStorageCnchFile::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr query_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    auto prepare_result = prepareReadContext(column_names, storage_snapshot->metadata, query_info, query_context);

    /// If no parts to read from - execute locally, must make sure that all stages are executed
    /// because CnchMergeTree is a high order storage
    if (prepare_result.file_parts.empty())
    {
        /// Stage 1: read from source table, just assume we read everything
        const auto & source_columns = query_info.syntax_analyzer_result->required_source_columns;
        auto fetch_column_header = Block(NamesAndTypes{source_columns.begin(), source_columns.end()});
        Pipe pipe(std::make_shared<NullSource>(std::move(fetch_column_header)));
        /// Stage 2: (partial) aggregation and projection if any
        auto query = getBasicSelectQuery(query_info.query);
        InterpreterSelectQuery(query, query_context, std::move(pipe), SelectQueryOptions(processed_stage)).buildQueryPlan(query_plan);
        return;
    }

    // todo(jiashuo): table function hasn't supported distributed query
    if (arguments.is_function_table || settings.resourcesAssignType() == StorageResourcesAssignType::SERVER_LOCAL)
    {
        readByLocal(prepare_result.file_parts, query_plan, column_names, storage_snapshot, query_info, query_context, processed_stage, max_block_size, num_streams);
        return;
    }

    Block header = InterpreterSelectQuery(query_info.query, query_context, SelectQueryOptions(processed_stage)).getSampleBlock();
    auto worker_group = query_context->getCurrentWorkerGroup();
    /// Return directly (with correct header) if no shard read from
    if (!worker_group || worker_group->getShardsInfo().empty())
    {
        LOG_TRACE(log, " worker group empty ");
        Pipe pipe(std::make_shared<NullSource>(header));
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        read_from_pipe->setStepDescription("Read from NullSource (CnchMergeTree)");
        query_plan.addStep(std::move(read_from_pipe));
        return;
    }

    LOG_TRACE(log, "Original query before rewrite: {}", queryToString(query_info.query));
    auto modified_query_ast = rewriteSelectQuery(query_info.query, getDatabaseName(), getCloudTableName(query_context));

    const Scalars & scalars = query_context->hasQueryContext() ? query_context->getQueryContext()->getScalars() : Scalars{};

    ClusterProxy::SelectStreamFactory select_stream_factory = ClusterProxy::SelectStreamFactory(
        header,
        {},
        storage_snapshot,
        processed_stage,
        StorageID::createEmpty(), /// Don't check whether table exists in cnch-worker
        scalars,
        false,
        query_context->getExternalTables());

    ClusterProxy::executeQuery(query_plan, select_stream_factory, log, modified_query_ast, query_context, worker_group);

    if (!query_plan.isInitialized())
        throw Exception("Pipeline is not initialized", ErrorCodes::LOGICAL_ERROR);
}

PrepareContextResult IStorageCnchFile::prepareReadContext(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr query_context)
{
    auto txn = query_context->getCurrentTransaction();
    if (query_context->getServerType() == ServerType::cnch_server && txn && txn->isReadOnly())
        query_context->getCnchTransactionCoordinator().touchActiveTimestampByTable(getStorageID(), txn);

    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    auto filter_files = getPrunedFiles(query_context, query_info.query);
    LOG_INFO(log, "Number of files to read: {}", filter_files.size());

    // We sort files in descending order by file size, so that big files can be processes at first to avoid long-tail
    if (filter_files.size() > 1)
        std::sort(filter_files.begin(), filter_files.end(), [](const FilePartInfo & a, const FilePartInfo & b) { return a.size > b.size; });

    FileDataPartsCNCHVector parts;
    for (const auto & file : filter_files)
    {
        parts.emplace_back(std::make_shared<FileDataPart>(file));
    }

    if (arguments.is_function_table)
        return {{}, {}, {}, std::move(parts)};

    auto worker_group = query_context->getCurrentWorkerGroup();
    healthCheckForWorkerGroup(query_context, worker_group);

    String local_table_name = getCloudTableName(query_context);
    collectResource(query_context, parts, local_table_name);

    return {std::move(local_table_name), {}, {}, std::move(parts)};
}

BlockOutputStreamPtr IStorageCnchFile::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context)
{
    return writeByLocal(query, metadata_snapshot, query_context);
}

BlockOutputStreamPtr IStorageCnchFile::writeByLocal(const ASTPtr & /*query*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*query_context*/)
{
    throw Exception("write is not supported now", ErrorCodes::NOT_IMPLEMENTED);
}

void IStorageCnchFile::alter(const AlterCommands & commands, ContextPtr query_context, TableLockHolder & /*table_lock_holder*/)
{
    auto table_id = getStorageID();
    StorageInMemoryMetadata new_metadata = getInMemoryMetadataCopy();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadataCopy();

    TransactionCnchPtr txn = query_context->getCurrentTransaction();
    auto action = txn->createAction<DDLAlterAction>(shared_from_this(), query_context->getSettingsRef(), query_context->getCurrentQueryId());
    auto & alter_act = action->as<DDLAlterAction &>();
    alter_act.setMutationCommands(commands.getMutationCommands(
        old_metadata, false, query_context));

    bool alter_setting = commands.isSettingsAlter();
    /// Check setting changes if has
    auto new_settings = this->settings;
    for (const auto & c : commands)
    {
        if (c.type != AlterCommand::MODIFY_SETTING)
            continue;
        new_settings.applyCnchFileSettingChanges(c.settings_changes);
    }

    /// Apply alter commands to metadata
    commands.apply(new_metadata, query_context);

    /// Apply alter commands to create-sql
    {
        String create_table_query = getCreateTableSql();
        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(parser, create_table_query, query_context->getSettingsRef().max_query_size
            , query_context->getSettingsRef().max_parser_depth);

        applyMetadataChangesToCreateQuery(ast, new_metadata, ParserSettings::valueOf(query_context->getSettingsRef()));
        alter_act.setNewSchema(queryToString(ast));
        txn->appendAction(std::move(action));
    }

    auto & txn_coordinator = query_context->getCnchTransactionCoordinator();
    txn_coordinator.commitV1(txn);

    if (alter_setting)
        this->settings = new_settings;

    setInMemoryMetadata(new_metadata);
}

NamesAndTypesList IStorageCnchFile::getVirtuals() const
{
    return virtual_columns;
}

void IStorageCnchFile::checkAlterIsPossible(const AlterCommands & commands, ContextPtr) const
{
    static std::set<AlterCommand::Type> cnchfile_alter_types = {
        AlterCommand::MODIFY_SETTING,
    };

    for (const auto & command : commands)
    {
        if (!cnchfile_alter_types.count(command.type))
            throw Exception("Alter of type '" + alterTypeToString(command.type) + "' is not supported by CnchFile(S3/HDFS)", ErrorCodes::NOT_IMPLEMENTED);

        LOG_INFO(log, "Executing CnchFile(S3/HDFS) ALTER command: {}", alterTypeToString(command.type));
    }

}

void IStorageCnchFile::checkAlterSettings(const AlterCommands & commands) const
{
    static std::set<String> supported_settings
        = {"cnch_vw_default",
           "cnch_vw_read",
           "cnch_vw_write", //not use currently
           "cnch_vw_task", //not use currently
           "resources_assign_type",
           "simple_hash_resources",
           "prefer_cnch_catalog"};

    /// Check whether the value is legal for Setting.
    /// For example, we have a setting item, `SettingBool setting_test`
    /// If you submit a Alter query: "Alter table test modify setting setting_test='abc'"
    /// Then, it will throw an Exception here, because we can't convert string 'abc' to a Bool.
    auto settings_copy = settings;

    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::MODIFY_SETTING)
            continue;

        for (const auto & change : command.settings_changes)
        {
            if (!supported_settings.count(change.name))
                throw Exception("Setting " + change.name + " cannot be modified", ErrorCodes::LOGICAL_ERROR);

            settings_copy.set(change.name, change.value);
        }
    }
}

FilePartInfos IStorageCnchFile::getPrunedFiles(const ContextPtr & query_context, const ASTPtr & query)
{
    FilePartInfos total_files = readFileList(query_context);

    if (query && virtual_header)
    {
        /// Append "key" column as the file name filter result
        virtual_header.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "_key"});

        auto block = virtual_header.cloneEmpty();
        MutableColumns columns = block.mutateColumns();
        for (auto & column : columns)
            column->insertDefault();
        block.setColumns(std::move(columns));

        ASTPtr filter_ast;
        VirtualColumnUtils::prepareFilterBlockWithQuery(query, query_context, block, filter_ast);

        if (!filter_ast)
            return total_files;

        block = virtual_header.cloneEmpty();
        MutableColumnPtr path_column;
        MutableColumnPtr file_column;
        MutableColumnPtr size_column;
        MutableColumnPtr key_column = block.getByName("_key").column->assumeMutable();

        if (block.has("_path"))
            path_column = block.getByName("_path").column->assumeMutable();

        if (block.has("_file"))
            file_column = block.getByName("_file").column->assumeMutable();

        if (block.has("_size"))
            size_column = block.getByName("_size").column->assumeMutable();

        for (const auto & file : total_files)
        {
            FileURI file_uri(file.name);
            if (path_column)
                path_column->insert(file_uri.file_path);
            if (file_column)
                file_column->insert(file_uri.file_name);
            if (size_column)
                size_column->insert(file.size);
            key_column->insert(file_uri.file_path); // todo(jiashuo): maybe duplicated
        }

        FilePartInfos filtered_files;
        VirtualColumnUtils::filterBlockWithQuery(query, block, query_context, filter_ast);
        const ColumnString & keys_col = typeid_cast<const ColumnString &>(*block.getByName("_key").column);
        const ColumnUInt64 & size_col = typeid_cast<const ColumnUInt64 &>(*block.getByName("_size").column);
        size_t rows = block.rows();
        filtered_files.reserve(rows);
        for (size_t i = 0; i < rows; ++i)
            // todo(jiashuo): github version via insert to append new file into `files`
            filtered_files.emplace_back(keys_col.getDataAt(i).toString(), size_col.getElement(i));
        return filtered_files;
    }
    return total_files;
}

void IStorageCnchFile::collectResource(const ContextPtr & query_context, const FileDataPartsCNCHVector & parts, const DB::String & local_table_name)
{
    auto cnch_resource = query_context->getCnchServerResource();

    Strings args{arguments.url, arguments.format_name, arguments.compression_method};

    auto setting_s3_access_key_id = query_context->getSettingsRef().s3_access_key_id.value;
    auto setting_s3_access_key_secret = query_context->getSettingsRef().s3_access_key_secret.value;

    auto final_ak = setting_s3_access_key_id.empty() ? arguments.access_key_id : setting_s3_access_key_id;
    auto final_sk = setting_s3_access_key_secret.empty() ? arguments.access_key_secret : setting_s3_access_key_secret;

    if (!final_ak.empty() && !final_sk.empty())
    {
        args.emplace_back(final_ak);
        args.emplace_back(final_sk);
    }


    auto create_table_query = getCreateQueryForCloudTable(getCreateTableSql(), local_table_name, query_context, false, {}, args);
    cnch_resource->setWorkerGroup(query_context->getCurrentWorkerGroup());
    cnch_resource->addCreateQuery(query_context, shared_from_this(), create_table_query, local_table_name, false);
    cnch_resource->addDataParts(getStorageUUID(), parts);
}

QueryProcessingStage::Enum IStorageCnchFile::getQueryProcessingStage(
    ContextPtr query_context, QueryProcessingStage::Enum stage, const StorageSnapshotPtr & storage_snapshot, SelectQueryInfo & query_info) const
{
    if (arguments.is_function_table || settings.resourcesAssignType() == StorageResourcesAssignType::SERVER_LOCAL)
        return IStorage::getQueryProcessingStage(query_context, stage, storage_snapshot, query_info);

    const auto & local_settings = query_context->getSettingsRef();

    if (local_settings.distributed_perfect_shard || local_settings.distributed_group_by_no_merge)
    {
        return QueryProcessingStage::Complete;
    }
    else if (auto worker_group = query_context->tryGetCurrentWorkerGroup())
    {
        size_t num_workers = worker_group->getShardsInfo().size();
        size_t result_size = (num_workers * local_settings.max_parallel_replicas);
        return result_size == 1 ? QueryProcessingStage::Complete : QueryProcessingStage::WithMergeableState;
    }
    else
    {
        return QueryProcessingStage::WithMergeableState;
    }
}

StorageID IStorageCnchFile::prepareTableRead(const Names & output_columns, SelectQueryInfo & query_info, ContextPtr local_context)
{
    auto prepare_result = prepareReadContext(output_columns, getInMemoryMetadataPtr(), query_info, local_context);

    StorageID storage_id = getStorageID();
    storage_id.table_name = prepare_result.local_table_name;
    return storage_id;
}
}
