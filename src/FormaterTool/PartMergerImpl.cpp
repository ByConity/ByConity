#include "PartMergerImpl.h"
#include "Poco/Format.h"
#include "CloudServices/CnchPartsHelper.h"
#include "Core/UUID.h"
#include "Storages/HDFS/HDFSCommon.h"
#include "Storages/MergeTree/MergeTreeReaderCompact.h"

namespace DB
{
/**
 * Get next valid (vacated) part name.
 */
String getNextPartName(
    const std::unordered_set<String> & uniqe_names,
    const String & part_name,
    const std::shared_ptr<StorageCloudMergeTree> merge_tree,
    UInt64 & min_max_index)
{
    MergeTreePartInfo info = MergeTreePartInfo::fromPartName(part_name, merge_tree->format_version);
    while (true)
    {
        info.min_block = ++min_max_index;
        info.max_block = min_max_index;

        String new_part_name = info.getPartName();
        if (!uniqe_names.count(new_part_name))
            return new_part_name;
    }
}

void PartMergerImpl::copyPartData(const DiskPtr & from_disk, const String & from_path, const DiskPtr & to_disk, const String & to_path)
{
    if (!from_disk->isDirectory(from_path))
        throw Exception("Source path " + from_disk->getPath() + from_path + " is not directory", ErrorCodes::LOGICAL_ERROR);

    if (to_disk->exists(to_path))
        throw Exception("Target path " + to_disk->getPath() + to_path + " already exists", ErrorCodes::LOGICAL_ERROR);

    to_disk->createDirectory(to_path);
    for (auto it = from_disk->iterateDirectory(from_path); it->isValid(); it->next())
    {
        /// Copy data from the source file to the target directory.
        from_disk->copy(from_path + it->name(), to_disk, to_path);
    }
}

std::shared_ptr<StorageCloudMergeTree> PartMergerImpl::createStorage(const String & path, const String & create_table_query)
{
    auto context = getContext();
    auto ast = getASTCreateQueryFromString(create_table_query, context);
    ASTCreateQuery & create_query = *ast;
    /// CloudMergeTree checks for non-empty UUID in its constructor,
    /// let's fake it (not used in part-merger anyway)
    UUID fake_cnch_uuid = UUIDHelpers::generateV4();
    modifyOrAddSetting(create_query, "cnch_table_uuid", Field(UUIDHelpers::UUIDToString(fake_cnch_uuid)));
    auto storage = createStorageFromQuery(create_query, context);
    auto merge_tree = std::dynamic_pointer_cast<StorageCloudMergeTree>(storage);
    if (!merge_tree)
    {
        /// Must use part-merger with `ENGINE = CloudMergeTree`.
        throw Exception("Please choose `CloudMergeTree` as the engine.", ErrorCodes::INVALID_CONFIG_PARAMETER);
    }
    /// IMPORTANT: reset table relative path to the requested value
    merge_tree->setRelativeDataPath(IStorage::StorageLocation::MAIN, path);

    return merge_tree;
}

auto PartMergerImpl::createStorages(const std::vector<String> & uuids, const String & create_table_query)
    -> std::vector<StorageCloudMergeTreePtr>
{
    std::vector<StorageCloudMergeTreePtr> res;
    res.reserve(uuids.size());
    for (const auto & uuid : uuids)
    {
        res.push_back(createStorage(uuid, create_table_query));
    }

    return res;
}

PartMergerImpl::PartMergerImpl(ContextMutablePtr context_, Poco::Util::AbstractConfiguration & config, LoggerPtr log_)
    : PartToolkitBase(nullptr, context_), log(log_)
{
    // Init arguments passed from CLI.
    params.create_table_query = config.getString("create-table-sql");

    params.source_path = config.getString("source-path");
    params.output_path = config.getString("output-path");
    params.uuids_str = config.getString("uuids");
    if (config.has("concurrency"))
        params.concurrency = config.getInt("concurrency");

    /// Check source path.
    Poco::URI uri_in(params.source_path);
    if (!isHdfsOrCfsScheme(uri_in.getScheme()))
        throw Exception("Source path should be a HDFS or CFS path.", ErrorCodes::BAD_ARGUMENTS);

    if (!DB::HDFSCommon::exists(uri_in.getPath()))
        throw Exception("Source path " + uri_in.getPath() + " doesn't exists.", ErrorCodes::BAD_ARGUMENTS);

    params.source_path = uri_in.getPath();

    /// Check output path
    Poco::URI uri_out(params.output_path);
    if (!isHdfsOrCfsScheme(uri_out.getScheme()))
        throw Exception("Output path should be a HDFS or CFS path.", ErrorCodes::BAD_ARGUMENTS);

    params.output_path = uri_out.getPath();
    if (DB::HDFSCommon::exists(params.output_path))
    {
        LOG_WARNING(log, "Output path {} already exists on target. Will remove it.", params.output_path);
        DB::HDFSCommon::remove(params.output_path, true);
    }

    /// Context settings.
    getContext()->setHdfsUser("clickhouse");
    getContext()->setHdfsNNProxy("nnproxy");

    auto & mergetree_settings = const_cast<MergeTreeSettings &>(getContext()->getMergeTreeSettings());
    auto & settings = const_cast<Settings &>(getContext()->getSettingsRef());

    /// Apply some default settings.
    /// See: `PartToolkitBase::applySettings()` for more details.
    mergetree_settings.set("enable_metastore", false);

    mergetree_settings.set("min_rows_for_compact_part", 0);
    mergetree_settings.set("min_bytes_for_compact_part", 0);
    mergetree_settings.set("enable_local_disk_cache", 0);
    mergetree_settings.set("enable_nexus_fs", 0);
    settings.set("input_format_skip_unknown_fields", true);
    settings.set("skip_nullinput_notnull_col", true);

    /// Parse settings and apply them.
    if (config.has("settings"))
    {
        /// 1. Parse phase.
        ParserSetQuery parser{true};
        constexpr UInt64 max_size = 10000;
        constexpr UInt64 max_depth = 100;
        ASTPtr settings_ast = parseQuery(parser, config.getString("settings"), max_size, max_depth);


        /// 2. Apply phase.
        for (const auto & change : settings_ast->as<ASTSetQuery>()->changes)
        {
            LOG_DEBUG(log, "Apply setting: {} = {}", change.name, change.value);
            if (settings.has(change.name))
                settings.set(change.name, change.value);
            else if (mergetree_settings.has(change.name))
                mergetree_settings.set(change.name, change.value);
            else if (change.name == "hdfs_user")
                getContext()->setHdfsUser(change.value.safeGet<String>());
            else if (change.name == "hdfs_nnproxy")
                getContext()->setHdfsNNProxy(change.value.safeGet<String>());
            else
                user_settings.emplace(change.name, change.value);
        }
    }

    /// Init HDFS params.
    ///
    /// User can bypass nnproxy by passing a string to `hdfs_nnproxy` with prefixs like `hdfs://` or `cfs://`.
    HDFSConnectionParams hdfs_params
        = HDFSConnectionParams::parseFromMisusedNNProxyStr(getContext()->getHdfsNNProxy(), getContext()->getHdfsUser());
    getContext()->setHdfsConnectionParams(hdfs_params);

    /// Register default HDFS file system as well in case of
    /// lower level logic call `getDefaultHdfsFileSystem`.
    /// Default values are the same as those on the ClickHouse server.
    {
        const int hdfs_max_fd_num = user_settings.count("hdfs_max_fd_num") ? user_settings["hdfs_max_fd_num"].safeGet<int>() : 100000;
        const int hdfs_skip_fd_num = user_settings.count("hdfs_skip_fd_num") ? user_settings["hdfs_skip_fd_num"].safeGet<int>() : 100;
        const int hdfs_io_error_num_to_reconnect
            = user_settings.count("hdfs_io_error_num_to_reconnect") ? user_settings["hdfs_io_error_num_to_reconnect"].safeGet<int>() : 10;
        registerDefaultHdfsFileSystem(hdfs_params, hdfs_max_fd_num, hdfs_skip_fd_num, hdfs_io_error_num_to_reconnect);
    }
}

void PartMergerImpl::execute()
{
    LOG_INFO(log, "Start part merger.");


    /// Mock transaction.
    LOG_DEBUG(log, "Prepare transaction.");
    TransactionRecord txn_record;
    txn_record.setMainTableUUID(UUIDHelpers::generateV4());
    txn_record.setID(TxnTimestamp::fallbackTS()).setReadOnly(true).setStatus(CnchTransactionStatus::Running);
    TransactionCnchPtr current_txn = std::make_shared<CnchServerTransaction>(getContext(), txn_record);
    getContext()->setCurrentTransaction(current_txn, false);

    LOG_DEBUG(log, "Prepare table.");
    /// Parse uuids.
    Strings uuids;
    boost::split(uuids, params.uuids_str, boost::is_any_of(","));

    /// Generate a vector of IStorage per UUID.
    std::vector<StorageCloudMergeTreePtr> cloud_trees = createStorages(uuids, params.create_table_query);
    StorageCloudMergeTreePtr output_cloud_tree = createStorage("", params.create_table_query);

    if (cloud_trees.empty())
    {
        LOG_ERROR(log, "Must at least set one uuid. Abort!");
        return;
    }

    LOG_DEBUG(log, "Collecting parts.");

    IMergeTreeDataPartsVector parts = collectSourceParts(cloud_trees);
    LOG_DEBUG(log, "Get total {} parts.", parts.size());

    /// Init remote_disk & target_disk.
    HDFSConnectionParams hdfs_params = getContext()->getHdfsConnectionParams();
    std::shared_ptr<DiskByteHDFS> remote_disk = std::make_shared<DiskByteHDFS>("hdfs", params.source_path, hdfs_params);

    DiskPtr target_disk = std::make_shared<DiskByteHDFS>("target", params.output_path, hdfs_params);

    /// Build a <name->part> index for later usage.
    std::unordered_map<String, IMergeTreeDataPartPtr> name_to_part;
    for (const auto & part : parts)
    {
        name_to_part.emplace(part->get_name(), part);
    }

    /// NOTE: This is a hack that use one merge_tree for all parts.
    ///
    /// Try select merge candidates.
    MergeTreeDataMergerMutator merger_mutator(*cloud_trees[0], 0);

    std::vector<FutureMergedMutatedPart> candidates;
    String disable_reason;
    LOG_DEBUG(log, "Try to generate merge candidates.");
    auto selection_decision = merger_mutator.selectPartsToMergeMulti(
        candidates,
        parts,
        true,
        cloud_trees[0]->getSettings()->max_bytes_to_merge_at_max_space_in_pool,
        [&](const DataPartPtr & lhs, const DataPartPtr & rhs, String *) -> bool {
            /// This predicate is checked for the first part of each range.
            if (!lhs)
                return true;
            auto lhs_commit_time = lhs->get_commit_time();
            auto rhs_commit_time = rhs->get_commit_time();
            return lhs_commit_time == rhs_commit_time;
        },
        false,
        &disable_reason,
        nullptr,
        true,
        false /*check_intersection*/);


    LOG_INFO(log, "Generated merge candidates: {}", candidates.size());

    LOG_INFO(log, "source disk, path: {}, target disk, path: {}", remote_disk->getPath(), target_disk->getPath());
    switch (selection_decision)
    {
        case SelectPartsDecision::SELECTED:
            break;
        case SelectPartsDecision::CANNOT_SELECT:
        case SelectPartsDecision::NOTHING_TO_MERGE:
            LOG_DEBUG(log, "No merge task selected. Directly copy all parts to target directory. Disable reason: {}", disable_reason);
            for (const auto & part : parts)
            {
                copyPartData(remote_disk, part->getFullRelativePath(), target_disk, part->name);
            }
            return;
    }

    // Start to merge.
    auto concurrency = std::min(std::min(params.concurrency, candidates.size()), static_cast<size_t>(32));
    LOG_DEBUG(log, "Executing {} merge tasks with {} threads.", candidates.size(), concurrency);
    ThreadPool thread_pool(concurrency);
    std::unordered_set<String> parts_executing_merge;
    std::atomic<int> succeed_tasks{0};
    ExceptionHandler exception_handler;

    /// NOTE: This is another hack that use first merge tree for all parts.
    for (auto & future_part : candidates)
    {
        ManipulationTaskParams merge_task_params(cloud_trees[0]);
        merge_task_params.type = ManipulationType::Merge;
        /// Manually construct id for tasks.
        merge_task_params.task_id = Poco::format("task-{%s}", future_part.name);
        IMergeTreeDataPartsVector source_parts;
        LOG_DEBUG(log, "Future part's name: {}, type: {}", future_part.name, future_part.type.toString());
        for (auto & server_part : future_part.parts)
        {
            parts_executing_merge.emplace(server_part->get_name());
            source_parts.push_back(server_part);
        }
        merge_task_params.assignSourceParts(source_parts);

        merge_task_params.is_bucket_table = cloud_trees[0]->isBucketTable();

        thread_pool.scheduleOrThrowOnError([&, merge_task_params]() {
            try
            {
                MergeTask task = std::make_unique<CloudMergeTreeMergeTask>(*cloud_trees[0], merge_task_params, getContext());
                task->setManipulationEntry();
                executeMergeTask(*output_cloud_tree, target_disk, task);
                succeed_tasks++;
            }
            catch (...)
            {
                exception_handler.setException(std::current_exception());
            }
        });
    }

    thread_pool.wait();

    /// Return -1 if any merge task fails.
    if (exception_handler.hasException())
    {
        LOG_ERROR(log, "Exception occurs during executing merge tasks. Total : {}, succeeded : {}", candidates.size(), succeed_tasks);
        exception_handler.throwIfException();
    }

    /// For those parts that are not involed in merge task, just copy them to taget directory.
    size_t counter = 0;
    for (auto & it : name_to_part)
    {
        if (!parts_executing_merge.count(it.first))
        {
            counter++;
            copyPartData(remote_disk, it.second->getFullRelativePath(), target_disk, it.first);
        }
    }

    if (counter > 0)
        LOG_INFO(log, "Copied unmerged parts to target directory directly.", counter);

    LOG_INFO(log, "Finish all {} merge tasks, {} in total.", succeed_tasks, candidates.size());
}

void PartMergerImpl::executeMergeTask(MergeTreeMetaBase & merge_tree, DiskPtr & disk, const MergeTask & task)
{
    /// Merge.
    size_t source_part_count = task->getParams().source_data_parts.size();
    LOG_DEBUG(log, "Start merging {} parts.", source_part_count);

    MergeTreeDataMerger merger{merge_tree, task->getParams(), getContext(), task->getManipulationListElement(), [&] { return false; }};

    auto merged_part = merger.mergePartsToTemporaryPart();

    LOG_INFO(log, "Finish merging {} parts into part {}", source_part_count, merged_part->name);

    S3ObjectMetadata::PartGeneratorID part_generator_id(
        S3ObjectMetadata::PartGeneratorID::DUMPER, UUIDHelpers::UUIDToString(UUIDHelpers::generateV4()));

    /// Dump.
    MergeTreeCNCHDataDumper dumper(merge_tree, part_generator_id);

    LOG_DEBUG(log, "Start dumping {} part", merged_part->name);
    auto dumped_part = dumper.dumpTempPart(merged_part, disk);

    LOG_INFO(log, "Finish dumping local part {} to remote part {}", merged_part->name, dumped_part->name);
}

IMergeTreeDataPartsVector PartMergerImpl::collectSourceParts(const std::vector<StorageCloudMergeTreePtr> & merge_trees)
{
    HDFSConnectionParams hdfs_params = getContext()->getHdfsConnectionParams();
    std::shared_ptr<DiskByteHDFS> remote_disk = std::make_shared<DiskByteHDFS>("hdfs", params.source_path, hdfs_params);
    auto volume = std::make_shared<SingleDiskVolume>("volume_single", remote_disk, 0);
    IMergeTreeDataPartsVector parts{};
    std::unordered_set<String> part_name_set;
    UInt64 increment{0};

    /// Prepare a thread pool.
    size_t max_threads = user_settings.count("max_merge_threads")
        ? std::max(1ul, std::min(32ul, user_settings["max_merge_threads"].safeGet<UInt64>()))
        : 1;
    ExceptionHandler exception_handler;
    ThreadPool pool(max_threads);
    std::mutex lock;

    /// collect all data parts from sub directories of input path.
    for (const auto & merge_tree : merge_trees)
    {
        String data_relative_path = merge_tree->getRelativeDataPath(IStorage::StorageLocation::MAIN);
        if (!remote_disk->exists(data_relative_path))
            throw Exception(
                "Data path " + remote_disk->getPath() + data_relative_path + " doesn't exists.", ErrorCodes::DIRECTORY_DOESNT_EXIST);

        Strings part_names;
        remote_disk->listFiles(data_relative_path, part_names);
        for (const auto & part_name : part_names)
        {
            auto relative_path = data_relative_path + "/" + part_name + '/';
            String new_name = part_name;
            LOG_DEBUG(log, "Relative_path: {}, part_name: {}", relative_path, part_name);
            // rename part if there is already one with the same name. Parts from different PW task may have the same name.
            if (part_name_set.count(part_name))
            {
                new_name = getNextPartName(part_name_set, part_name, merge_tree, increment);
            }
            part_name_set.emplace(new_name);
            pool.scheduleOrThrowOnError(createExceptionHandledJob(
                [&merge_tree, new_name, &volume, part_name, &parts, &lock]() {
                    auto part = std::make_shared<MergeTreeDataPartCNCH>(*merge_tree, new_name, volume, part_name + '/');
                    part->loadFromFileSystem();
                    part->disk_cache_mode = DiskCacheMode::SKIP_DISK_CACHE;

                    // See https://xxx
                    part->default_codec = CompressionCodecFactory::instance().getDefaultCodec();
                    {
                        std::unique_lock<std::mutex> guard(lock);
                        parts.push_back(part);
                    }
                },
                exception_handler));
        }
    }

    pool.wait();

    exception_handler.throwIfException();

    auto visible_parts = CnchPartsHelper::calcVisibleParts(parts, true, CnchPartsHelper::LoggingOption::EnableLogging);

    if (parts.size() != visible_parts.size())
        throw Exception("Some of the source parts are not visible. This is a logic error.", ErrorCodes::LOGICAL_ERROR);

    LOG_INFO(log, "Find {} parts to merge.", visible_parts.size());

    /// We return parts here because it's sorted by calcVisibleParts.
    return parts;
}
}
