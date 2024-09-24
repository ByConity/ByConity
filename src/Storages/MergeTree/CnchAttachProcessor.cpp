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

#include <Storages/MergeTree/CnchAttachProcessor.h>
#include <cmath>
#include <memory>
#include <vector>
#include <numeric>
#include <filesystem>
#include <memory>
#include <numeric>
#include <set>
#include <thread>
#include <utility>
#include <vector>
#include <CloudServices/CnchDataWriter.h>
#include <CloudServices/CnchPartsHelper.h>
#include <Databases/DatabasesCommon.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/trySetVirtualWarehouse.h>
#include <Parsers/ASTLiteral.h>
#include <Disks/DiskByteS3.h>
#include <Disks/DiskType.h>
#include <Interpreters/executeQuery.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/PartitionCommands.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/parseQuery.h>
#include <Storages/MergeTree/MergeTreeDataPartCNCH_fwd.h>
#include <Storages/MergeTree/MergeTreeCNCHDataDumper.h>
#include <Storages/MergeTree/S3PartsAttachMeta.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TxnTimestamp.h>
#include <Transaction/Actions/S3AttachMetaFileAction.h>

namespace ProfileEvents
{
    extern const Event PartsToAttach;
    extern const Event NumOfRowsToAttach;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BUCKET_TABLE_ENGINE_MISMATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int BAD_ARGUMENTS;
    extern const int DIRECTORY_ALREADY_EXISTS;
}

IMergeTreeDataPartsVector fromCNCHPartsVec(const MutableMergeTreeDataPartsCNCHVector& parts)
{
    IMergeTreeDataPartsVector converted_parts;
    for (const MutableMergeTreeDataPartCNCHPtr& part : parts)
    {
        converted_parts.push_back(part);
    }
    return converted_parts;
}

MutableMergeTreeDataPartsCNCHVector toCNCHPartsVec(const IMergeTreeDataPartsVector& parts)
{
    MutableMergeTreeDataPartsCNCHVector converted_parts;
    for (const IMergeTreeDataPartPtr& part : parts)
    {
        if (auto cnch_part = std::dynamic_pointer_cast<MergeTreeDataPartCNCH>(
            std::const_pointer_cast<IMergeTreeDataPart>(part)); cnch_part != nullptr)
        {
            converted_parts.push_back(cnch_part);
        }
        else
        {
            throw Exception("Failed to convert parts back to cnch parts", ErrorCodes::LOGICAL_ERROR);
        }
    }
    return converted_parts;
}

AttachFilter AttachFilter::createPartFilter(const String& part_name,
    MergeTreeDataFormatVersion ver)
{
    AttachFilter filter(Mode::PART, part_name);
    if (!MergeTreePartInfo::tryParsePartName(part_name, &filter.part_name_info, ver))
    {
        throw Exception(fmt::format("Can't parse part info from {}", part_name),
            ErrorCodes::BAD_ARGUMENTS);
    }
    return filter;
}

AttachFilter AttachFilter::createPartitionFilter(const String& partition)
{
    return AttachFilter(Mode::PARTITION, partition);
}

AttachFilter AttachFilter::createPartsFilter()
{
    return AttachFilter(Mode::PARTS);
}

bool AttachFilter::filter(const MergeTreePartInfo& part_info) const
{
    switch (mode)
    {
        case PART:
        {
            // Filter out all part with same block info, i.e. all base and delta part
            return part_name_info.sameBlocks(part_info);
        }
        case PARTITION:
        {
            return part_info.partition_id == object_id;
        }
        case PARTS:
        {
            return true;
        }
    }
    __builtin_unreachable();
}

void AttachFilter::checkFilterResult(const std::vector<MutableMergeTreeDataPartsCNCHVector>& parts_from_sources,
    UInt64 attach_limit) const
{
    if (mode == PART)
    {
        // NOTE(wsy) We don't support attach from middle of part chain by now
        size_t total_matched_parts = 0;
        MutableMergeTreeDataPartCNCHPtr founded_part = nullptr;
        for (const auto& parts_from_source : parts_from_sources)
        {
            total_matched_parts += parts_from_source.size();
            if (!parts_from_source.empty())
            {
                founded_part = parts_from_source[0];
            }
        }

        if (total_matched_parts != 1)
        {
            throw Exception(fmt::format("Expect only one visible part, got {}",
                total_matched_parts), ErrorCodes::BAD_ARGUMENTS);
        }

        String founded_part_name = founded_part->info.getPartName();
        if (founded_part_name != object_id)
        {
            throw Exception(fmt::format("Can't attach part {}, maybe you want to attach {}",
                object_id, founded_part_name), ErrorCodes::BAD_ARGUMENTS);
        }
    }

    size_t founded_parts = 0;
    for (const auto& parts_from_source : parts_from_sources)
    {
        for (const auto& part : parts_from_source)
        {
            for (IMergeTreeDataPartPtr current = part; current != nullptr;
                current = current->tryGetPreviousPart())
            {
                ++founded_parts;
            }
        }
    }
    if (founded_parts > attach_limit)
    {
        throw Exception(fmt::format("Parts number {} exceed {}", founded_parts,
            attach_limit), ErrorCodes::BAD_ARGUMENTS);
    }
}

String AttachFilter::toString() const
{
    switch (mode)
    {
        case PART:
        {
            return "{Part: " + object_id + "}";
        }
        case PARTITION:
        {
            return "{Partition: " + object_id + "}";
        }
        case PARTS:
        {
            return "{Parts}";
        }
    }
    __builtin_unreachable();
}

void AttachContext::writeRenameRecord(const DiskPtr & disk, const String & from, const String & to)
{
    LOG_TRACE(logger, fmt::format("Write rename record, disk path {}, relative path {} -> {}", disk->getPath(), from, to));

    std::lock_guard<std::mutex> lock(mu);

    auto & res = resources[disk->getName()];
    res.disk = disk;
    res.rename_map[from] = to;
}

void AttachContext::writeMetaFilesNameRecord(const DB::DiskPtr & disk, const DB::String & meta_file_name)
{
    LOG_TRACE(
        logger,
        fmt::format(
            "Write meta files name to delete record for attaching unique table parts, in disk {}, relative file path {}",
            disk->getPath(),
            meta_file_name));

    std::lock_guard<std::mutex> lock(mu);

    auto & res = meta_files_to_delete[disk->getName()];
    res.disk = disk;
    res.rename_map[meta_file_name] = "";
}

void AttachContext::writeRenameMapToKV(Catalog::Catalog & catalog, const StorageID & storage_id, const TxnTimestamp & txn_id)
{
    UndoResources undo_buffers;
    for (const auto & [disk_name, resource] : resources)
    {
        for (const auto & [from, to] : resource.rename_map)
        {
            undo_buffers.emplace_back(txn_id, UndoResourceType::FileSystem,
                from, to);
            undo_buffers.back().setDiskName(disk_name);
        }
    }
    catalog.writeUndoBuffer(storage_id, txn_id, undo_buffers);
}

void AttachContext::commit()
{
    if (new_txn != nullptr)
    {
        query_ctx.getCnchTransactionCoordinator().finishTransaction(new_txn);
    }

    /// If we're not in the interactive transaction session, at this point it's safe
    /// to remove the directory lock
    if (!src_directory.empty() && !isQueryInInteractiveSession(query_ctx.shared_from_this()) && !query_ctx.getSettingsRef().force_clean_transaction_by_dm)
        query_ctx.getCnchCatalog()->clearFilesysLocks({src_directory}, std::nullopt);

    if (!meta_files_to_delete.empty())
    {
        size_t total_records = 0;
        for (const auto & [_, meta_name_records] : meta_files_to_delete)
            total_records += meta_name_records.rename_map.size();
        ThreadPool & pool = getWorkerPool(total_records);
        for (const auto & [_, meta_name_records] : meta_files_to_delete)
        {
            for (const auto & [file_path, __] : meta_name_records.rename_map)
                pool.scheduleOrThrowOnError([&disk = meta_name_records.disk, path = file_path]() { disk->removeFileIfExists(path); });
        }
        pool.wait();
    }
}

void AttachContext::rollback()
{
    if (new_txn != nullptr)
    {
        query_ctx.getCnchTransactionCoordinator().finishTransaction(new_txn);
    }

    size_t total_records = 0;
    for (const auto& [_, resource] : resources)
    {
        total_records += resource.rename_map.size();
    }

    ThreadPool& pool = getWorkerPool(total_records);
    for (const auto& [_, resource] : resources)
    {
        for (const auto& entry : resource.rename_map)
        {
            pool.scheduleOrThrowOnError([&disk = resource.disk, from=entry.first, to=entry.second]() {
                if (disk->exists(to))
                {
                    disk->moveDirectory(to, from);
                }
            });
        }
    }
    pool.wait();
}

ThreadPool& AttachContext::getWorkerPool(int job_nums)
{
    bool need_create_thread_pool = worker_pool == nullptr || worker_pool->finished();
    if (!need_create_thread_pool)
    {
        // Already have a thread pool
        if ((job_nums - static_cast<int>(worker_pool->getMaxThreads())) \
            > expand_thread_pool_threshold)
        {
            worker_pool->wait();
            worker_pool = nullptr;

            need_create_thread_pool = true;
        }
    }

    if (need_create_thread_pool)
    {
        worker_pool = std::make_unique<ThreadPool>(
            std::max(1, std::min(max_worker_threads, job_nums)));
    }
    return *worker_pool;
}

void CnchAttachProcessor::exec()
{
    if (is_unique_tbl && command.replace)
        throw Exception("Replace partition or part is not supported for unique table", ErrorCodes::NOT_IMPLEMENTED);

    AttachContext attach_ctx(*query_ctx, 8,
        query_ctx->getSettingsRef().cnch_part_attach_max_threads, logger);
    NameSet staged_part_names;
    NameSet partitions_filter;
    std::vector<ASTPtr> attached_partitions;

    AttachFilter filter;
    MutableMergeTreeDataPartsCNCHVector preload_parts;
    try
    {
        // Find all parts which matches filter, these parts will retain it's origin
        // position, then calculate parts chain and return all visible parts
        std::pair<AttachFilter, PartsFromSources> collect_res = collectParts(attach_ctx);
        filter = collect_res.first;
        PartsFromSources& parts_from_sources = collect_res.second;

        // Assign new part name and rename it to target location
        PartsWithHistory prepared_parts = prepareParts(parts_from_sources, attach_ctx);
        preload_parts = prepared_parts.second;

        if (command.replace)
        {
            genPartsDeleteMark(prepared_parts);
        }

        if (!prepared_parts.first.empty())
        {
            for (const auto & part : prepared_parts.second)
                partitions_filter.emplace(part->info.partition_id);

            DiskType::Type disk_type = target_tbl.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk()->getType();
            if(disk_type ==  DiskType::Type::ByteS3)
                commitPartsFromS3(prepared_parts, staged_part_names);
            else
                commitParts(prepared_parts.second, staged_part_names, attached_partitions);
        }
    }
    catch(...)
    {
        tryLogCurrentException(logger);

        attach_ctx.rollback();

        throw;
    }

    try
    {
        // If anything went wrong after this point, we don't know for sure if we
        // should move parts back to source table, since UndoResource is recorded,
        // we let transaction handle rollback
        auto& txn_coordinator = query_ctx->getCnchTransactionCoordinator();
        TransactionCnchPtr txn = query_ctx->getCurrentTransaction();
        txn->setMainTableUUID(target_tbl.getStorageUUID());
        txn_coordinator.commitV2(txn);

        injectFailure(AttachFailurePoint::AFTER_COMMIT_FAIL);

        if (is_unique_tbl && query_ctx->getSettingsRef().enable_wait_attached_staged_parts_to_visible && !staged_part_names.empty())
            waitingForDedup(partitions_filter, staged_part_names);

        refreshView(attached_partitions, attach_ctx);
    }
    catch(...)
    {
        tryLogCurrentException(logger);

        attach_ctx.commit();

        throw;
    }

    attach_ctx.commit();

    tryPreload(preload_parts);
}

std::vector<MutableMergeTreeDataPartsCNCHVector> CnchAttachProcessor::getDetachedParts(const AttachFilter& filter)
{
  AttachContext attach_ctx(*query_ctx, 8, query_ctx->getSettingsRef().cnch_part_attach_max_threads, logger);
    return collectPartsFromTableDetached(target_tbl, filter, attach_ctx);
}

// Return relative path from 'from' to 'to'
String CnchAttachProcessor::relativePathTo(const String& from, const String& to)
{
    Poco::Path from_path = Poco::Path::forDirectory(from);
    Poco::Path to_path = Poco::Path::forDirectory(to);

    if (from_path.isAbsolute() ^ to_path.isAbsolute())
    {
        throw Exception(fmt::format("From {} and to {} have only one absolute path",
            from, to), ErrorCodes::BAD_ARGUMENTS);
    }

    int idx = 0;
    for (int limit = std::min(from_path.depth(), to_path.depth());
        idx < limit; ++idx)
    {
        if (from_path[idx] != to_path[idx])
        {
            break;
        }
    }

    Poco::Path relative_path;
    for (int i = idx; i < from_path.depth(); ++i)
    {
        relative_path.pushDirectory("..");
    }
    for (int i = idx; i < to_path.depth(); ++i)
    {
        relative_path.pushDirectory(to_path[i]);
    }

    LOG_TRACE(&Poco::Logger::get("RelativePath"), fmt::format("From {}, to {}, rel {}", from, to, relative_path.toString()));

    return relative_path.toString();
}

std::pair<AttachFilter, CnchAttachProcessor::PartsFromSources> CnchAttachProcessor::collectParts(AttachContext & attach_ctx)
{
    AttachFilter filter;
    PartsFromSources chained_parts_from_sources;

    injectFailure(AttachFailurePoint::BEFORE_COLLECT_PARTS);

    if (!command.from_table.empty())
    {
        String database = command.from_database.empty() ? query_ctx->getCurrentDatabase() : command.from_database;
        from_storage = DatabaseCatalog::instance().getTable(StorageID(database, command.from_table), query_ctx);
        auto * from_cnch_table = target_tbl.checkStructureAndGetCnchMergeTree(from_storage, query_ctx);

        if (command.attach_from_detached)
        {
            auto partition_id = from_cnch_table->getPartitionIDFromQuery(command.partition, query_ctx);

            filter = AttachFilter::createPartitionFilter(partition_id);
            chained_parts_from_sources = collectPartsFromTableDetached(*from_cnch_table, filter, attach_ctx);
        }
        else
            chained_parts_from_sources = collectPartsFromActivePartition(*from_cnch_table, attach_ctx);
    }
    else
    {
        // Construct filter
        filter = AttachFilter::createPartsFilter();
        if (command.part)
        {
            String part_name = typeid_cast<const ASTLiteral &>(*command.partition)
                .value.safeGet<String>();
            filter = AttachFilter::createPartFilter(part_name, target_tbl.format_version);
        }
        else if (!command.parts)
        {
            String partition_id = target_tbl.getPartitionIDFromQuery(command.partition,
                query_ctx);
            filter = AttachFilter::createPartitionFilter(partition_id);
        }

        if (command.from_zookeeper_path.empty())
        {
            from_storage = target_tbl.shared_from_this();
            chained_parts_from_sources = collectPartsFromTableDetached(target_tbl,
                filter, attach_ctx);
        }
        else
        {
            if (query_ctx->getSettingsRef().cnch_atomic_attach_part)
            {
                /// Attach parts from directory, lock the directory
                int retries = 3;
                while (true)
                {
                    auto hold_txn_id = query_ctx->getCnchCatalog()->writeFilesysLock(
                        query_ctx->getCurrentTransactionID(), command.from_zookeeper_path, target_tbl);

                    if (hold_txn_id == query_ctx->getCurrentTransactionID())
                        break;

                    if (!query_ctx->getSettingsRef().cnch_atomic_attach_part_preemtive_lock_acquire || retries == 1)
                        throw Exception(
                            fmt::format(
                                "Cannot lock directory {} because it's currently locked by transaction {}, retries count: {}", command.from_zookeeper_path, hold_txn_id.toUInt64(), retries),
                            ErrorCodes::LOGICAL_ERROR);

                    /// Try to clean the lock if it's not hold by current transaction
                    String clean_txn_query = fmt::format("SYSTEM CLEAN TRANSACTION {}", hold_txn_id.toString());
                    auto ctx = Context::createCopy(query_ctx);
                    ctx->setCurrentTransaction(nullptr, false);
                    executeQuery(clean_txn_query, ctx, true);
                    std::this_thread::sleep_for(std::chrono::milliseconds(3000));
                    retries--;
                }
                attach_ctx.setSourceDirectory(command.from_zookeeper_path);
            }
            chained_parts_from_sources = collectPartsFromPath(command.from_zookeeper_path, filter, attach_ctx);
        }
    }

    if (command.specify_bucket)
    {
        Int64 bucket_number = static_cast<Int64>(command.bucket_number);
        UInt64 expected_table_definition_hash = query_ctx->getSettingsRef().expected_table_definition_hash;
        for (auto & parts_from_source : chained_parts_from_sources)
        {
            std::erase_if(parts_from_source, [&](const MutableMergeTreeDataPartCNCHPtr & part) {
                if (expected_table_definition_hash > 0 && part->table_definition_hash != expected_table_definition_hash)
                {
                    LOG_DEBUG(
                        logger,
                        "Table definition hash {} of part {} is mismatch with expected_table_definition_hash {}, ignore it.",
                        part->table_definition_hash,
                        part->name,
                        expected_table_definition_hash);
                    return true;
                }
                else if (part->bucket_number != bucket_number)
                {
                    LOG_DEBUG(
                        logger,
                        "Bucket number {} of part {} is mismatch with acquired bucket number {}, ignore it.",
                        part->bucket_number,
                        part->name,
                        bucket_number);
                    return true;
                }
                return false;
            });
        }
    }

    injectFailure(AttachFailurePoint::CHECK_FILTER_RESULT);

    filter.checkFilterResult(chained_parts_from_sources, query_ctx->getSettingsRef().cnch_part_attach_limit);

    for (MutableMergeTreeDataPartsCNCHVector & visible_parts : chained_parts_from_sources)
    {
        for (MutableMergeTreeDataPartCNCHPtr & part : visible_parts)
            part->restoreMvccColumns();
    }

    return {std::move(filter), std::move(chained_parts_from_sources)};
}

CnchAttachProcessor::PartsFromSources CnchAttachProcessor::collectPartsFromTableDetached(
    const StorageCnchMergeTree& tbl, const AttachFilter& filter, AttachContext& attach_ctx)
{
    LOG_DEBUG(logger, fmt::format("Collect parts from table {} with filter {}", tbl.getLogName(), filter.toString()));

    DiskType::Type remote_disk_type = target_tbl.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk()->getType();
    switch (remote_disk_type)
    {
        case DiskType::Type::ByteHDFS:
        {
            std::vector<CollectSource> sources(1);
            CollectSource & source = sources.back();

            // Table's detached directory in every disk form a single source
            // and should calculate visible parts together
            Disks remote_disks = tbl.getStoragePolicy(IStorage::StorageLocation::MAIN)->getDisks();
            for (const DiskPtr & disk : remote_disks)
            {
                String src_rel_path = std::filesystem::path(tbl.getRelativeDataPath(IStorage::StorageLocation::MAIN)) / "detached" / "";
                source.units.emplace_back(disk, src_rel_path);
            }

            return collectPartsFromSources(tbl, sources, filter, query_ctx->getSettingsRef().cnch_part_attach_drill_down, attach_ctx);
        }
        case DiskType::Type::ByteS3:
        {
            // Collect detached parts from catalog
            auto catalog = query_ctx->getCnchCatalog();
            ServerDataPartsVector all_parts = catalog->listDetachedParts(tbl, filter);

            ServerDataPartsVector drop_ranges;
            ServerDataPartsVector visible_parts = CnchPartsHelper::calcVisibleParts(all_parts,
                false, false, &drop_ranges, nullptr, CnchPartsHelper::getLoggingOption(*query_ctx));

            DeleteBitmapMetaPtrVector bitmaps;
            if (is_unique_tbl)
            {
                DeleteBitmapMetaPtrVector all_bitmaps = catalog->listDetachedDeleteBitmaps(tbl, filter);
                CnchPartsHelper::calcVisibleDeleteBitmaps(all_bitmaps, bitmaps);
                LOG_DEBUG(logger, "Collect {} bitmap to attach from catalog for table {}", bitmaps.size(), tbl.getLogName());
            }

            PartsFromSources parts_from_sources(1);
            auto bitmap_it = bitmaps.begin();
            for (auto & part : visible_parts)
            {
                auto cnch_part = part->toCNCHDataPart(tbl);
                while (bitmap_it != bitmaps.end() && (*(*bitmap_it)) <= cnch_part->info)
                {
                    if (!(*bitmap_it)->sameBlock(cnch_part->info))
                        bitmap_it++;
                    else
                    {
                        attach_metas[cnch_part->name] = (*bitmap_it)->getModel();
                        bitmap_it++;
                    }
                }
                parts_from_sources.back().push_back(cnch_part);
            }
            return parts_from_sources;
        }
        default:
            throw Exception(
                fmt::format("Unsupported remote volume type {} when collect parts", DiskType::toString(remote_disk_type)),
                ErrorCodes::BAD_ARGUMENTS);
    }
}

CnchAttachProcessor::PartsFromSources
CnchAttachProcessor::collectPartsFromPath(const String & path, const AttachFilter & filter, AttachContext & attach_ctx)
{
    LOG_DEBUG(logger, fmt::format("Collect parts from path {} with filter {}", path, filter.toString()));

    DiskType::Type remote_disk_type = target_tbl.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk()->getType();
    switch (remote_disk_type)
    {
        case DiskType::Type::ByteHDFS:
        {
            auto [src_path, disk] = findBestDiskForHDFSPath(path);

            int drill_down_level = query_ctx->getSettingsRef().cnch_part_attach_drill_down;
            std::vector<CollectSource> sources = discoverCollectSources(target_tbl, disk, src_path, drill_down_level);

            return collectPartsFromSources(target_tbl, sources, filter, drill_down_level, attach_ctx);
        }
        case DiskType::Type::ByteS3:
        {
            // This is to handle parts generated from part writer. In this way, unique table will not generate bitmap. See more detail in doc: Unique Table Batch Loading Doc
            // Read info from task meta file
            return collectPartsFromS3TaskMeta(target_tbl, path, filter, attach_ctx);
        }
        default:
            throw Exception(
                fmt::format("Unsupported remote volume type {} when attach", DiskType::toString(remote_disk_type)),
                ErrorCodes::BAD_ARGUMENTS);
    }
}

std::vector<CollectSource> CnchAttachProcessor::discoverCollectSources(
    const StorageCnchMergeTree& tbl, const DiskPtr& disk, const String& root_path,
    int& drill_down_level)
{
    std::vector<String> current_level_path{root_path};
    std::vector<String> next_level_path;

    // Walkthrough current level's dir, if we should drill down, return next level's
    // dirs. If current level has some directory which can be parsed as valid part name,
    // return empty vector to indicate we should use this level as source
    auto walkthrough = [this, &tbl, disk](const std::vector<String>& current_level)
            -> std::vector<String> {
        MergeTreePartInfo part_info;
        std::vector<String> next_level;
        for (const String& path : current_level)
        {
            if (!disk->exists(path))
                continue;
            for (auto iter = disk->iterateDirectory(path); iter->isValid(); iter->next())
            {
                String current_path = std::filesystem::path(path) / iter->name();
                if (disk->isDirectory(current_path))
                {
                    if (MergeTreePartInfo::tryParsePartName(iter->name(), &part_info, tbl.format_version))
                    {
                        LOG_TRACE(logger, fmt::format("Stop discover source since "
                            "{} is a valid part name", iter->name()));
                        return {};
                    }
                    else
                    {
                        next_level.push_back(current_path);
                    }
                }
            }
        }
        return next_level;
    };

    int drilled = 0;
    for (; drilled <= drill_down_level; ++drilled)
    {
        next_level_path = walkthrough(current_level_path);
        if (next_level_path.empty())
        {
            break;
        }

        current_level_path.swap(next_level_path);
    }

    injectFailure(AttachFailurePoint::DISCOVER_PATH);

    // Return remained drill down level
    drill_down_level -= drilled;

    // Construct sources from current level's path
    std::vector<CollectSource> sources;
    for (size_t i = 0; i < current_level_path.size(); ++i)
    {
        LOG_TRACE(logger, fmt::format("Construct new source from {}",
            std::string(std::filesystem::path(disk->getPath()) / current_level_path[i])));

        sources.emplace_back();
        sources.back().units.emplace_back(disk, current_level_path[i]);
    }
    return sources;
}

CnchAttachProcessor::PartsFromSources CnchAttachProcessor::collectPartsFromSources(
    const StorageCnchMergeTree& tbl, const std::vector<CollectSource>& sources,
    const AttachFilter& filter, int max_drill_down_level, AttachContext& attach_ctx)
{
    if (max_drill_down_level < 0)
    {
        LOG_INFO(logger, "Skip parts collection since it already encounter drill down level limit, maybe try increase cnch_part_attach_drill_down");
        return {};
    }

    std::atomic<size_t> total_parts_num = 0;
    // Founded parts from different sources, each source will calculate visibility
    // independently
    PartsFromSources parts_from_sources(sources.size());

    auto& worker_pool = attach_ctx.getWorkerPool(sources.size());
    for (size_t i = 0; i < sources.size(); ++i)
    {
        worker_pool.scheduleOrThrowOnError([this, &tbl, &source = sources[i], &founded_parts = parts_from_sources[i], &total_parts_num, &filter, max_drill_down_level]() {
            for (const CollectSource::Unit& unit : source.units)
            {
                LOG_DEBUG(logger, fmt::format("Collect parts from disk {}, path {}",
                    unit.disk->getName(),
                    std::string(std::filesystem::path(unit.disk->getPath()) / unit.rel_path)));

                injectFailure(AttachFailurePoint::COLLECT_PARTS_FROM_UNIT);

                if (!unit.disk->exists(unit.rel_path))
                {
                    LOG_DEBUG(logger, fmt::format("Path {} doesn't exist, skip",
                        std::string(std::filesystem::path(unit.disk->getPath()) / unit.rel_path)));
                }
                else
                {
                    String unit_rel_path = std::filesystem::path(unit.rel_path) / "";
                    collectPartsFromUnit(tbl, unit.disk, unit_rel_path,
                        max_drill_down_level, filter, founded_parts);
                }
            }

            total_parts_num.fetch_add(founded_parts.size());
        });
    }
    worker_pool.wait();

    verifyPartsNum(total_parts_num);

    // Parallel load parts
    auto& load_pool = attach_ctx.getWorkerPool(total_parts_num);
    for (MutableMergeTreeDataPartsCNCHVector& parts : parts_from_sources)
    {
        for (const MutableMergeTreeDataPartCNCHPtr& part : parts)
        {
            load_pool.scheduleOrThrowOnError([this, part]() {
                injectFailure(AttachFailurePoint::LOAD_PART);

                part->loadFromFileSystem(true);
            });
        }
    }
    load_pool.wait();

    // Calculate visible parts
    for (MutableMergeTreeDataPartsCNCHVector& parts : parts_from_sources)
    {
        auto const_parts = fromCNCHPartsVec(parts);
        parts = toCNCHPartsVec(CnchPartsHelper::calcVisibleParts(const_parts, false));
    }

    return parts_from_sources;
}

CnchAttachProcessor::PartsFromSources CnchAttachProcessor::collectPartsFromS3TaskMeta(
    StorageCnchMergeTree & tbl, const String & task_id_prefix, const AttachFilter & filter, AttachContext & attach_ctx)
{
    if (filter.mode != AttachFilter::PARTS)
    {
        throw Exception(fmt::format("Only attach from parts is supported when attach from a path"), ErrorCodes::NOT_IMPLEMENTED);
    }

    VolumePtr volume = tbl.getStoragePolicy(IStorage::StorageLocation::MAIN)->getVolume(0);
    DiskPtr disk = tbl.getStoragePolicy(IStorage::StorageLocation::MAIN)->getVolume(0)->getDefaultDisk();
    std::shared_ptr<DiskByteS3> disk_s3 = std::dynamic_pointer_cast<DiskByteS3>(disk);
    if (disk_s3 == nullptr)
    {
        throw Exception("Failed to cast storage's default disk to Bytes3 when attach", ErrorCodes::LOGICAL_ERROR);
    }

    {
        TransactionCnchPtr txn = query_ctx->getCurrentTransaction();
        auto action = txn->createAction<S3AttachMetaFileAction>(disk_s3, task_id_prefix);
        txn->appendAction(action);

        UndoResource attaching_mark_res(txn->getTransactionID(), UndoResourceType::S3AttachMeta, task_id_prefix);
        attaching_mark_res.setDiskName(disk_s3->getName());
        query_ctx->getCnchCatalog()->writeUndoBuffer(
            target_tbl.getCnchStorageID(), txn->getTransactionID(), {attaching_mark_res});
    }

    S3PartsAttachMeta task_meta(disk_s3->getS3Client(), disk_s3->getS3Bucket(), disk_s3->getPath(), task_id_prefix);

    S3PartsAttachMeta::Reader reader(task_meta, 16);

    MergeTreePartInfo info;
    MutableMergeTreeDataPartsCNCHVector parts;
    for (const auto & part_meta : reader.metas())
    {
        if (MergeTreePartInfo::tryParsePartName(part_meta.first, &info, tbl.format_version))
        {
            if (filter.filter(info))
            {
                parts.push_back(std::make_shared<MergeTreeDataPartCNCH>(
                    tbl, part_meta.first, info, volume, part_meta.second, nullptr, UUIDHelpers::toUUID(part_meta.second)));
            }
        }
        else
        {
            throw Exception("Can't parse part name: " + part_meta.first + " in task: " + task_id_prefix, ErrorCodes::BAD_ARGUMENTS);
        }
    }

    auto & worker_pool = attach_ctx.getWorkerPool(parts.size());
    for (const auto & part : parts)
    {
        worker_pool.scheduleOrThrowOnError([this, part]() {
            injectFailure(AttachFailurePoint::LOAD_PART);
            part->loadFromFileSystem(false);
        });
    }
    worker_pool.wait();

    IMergeTreeDataPartsVector drop_ranges;
    IMergeTreeDataPartsVector all_parts = fromCNCHPartsVec(parts);
    IMergeTreeDataPartsVector visible_parts = CnchPartsHelper::calcVisibleParts(
        all_parts, false, false, &drop_ranges, nullptr,
        CnchPartsHelper::getLoggingOption(*query_ctx));

    PartsFromSources parts_from_sources(1);
    parts_from_sources.back() = toCNCHPartsVec(visible_parts);

    return parts_from_sources;
}

void CnchAttachProcessor::commitParts(MutableMergeTreeDataPartsCNCHVector & prepared_parts,
                     NameSet & staged_parts_name, std::vector<ASTPtr> & attached_partitions)
{
    injectFailure(AttachFailurePoint::BEFORE_COMMIT_FAIL);

    TxnTimestamp commit_time;
    CnchDataWriter cnch_writer(target_tbl, query_ctx, ManipulationType::Insert);

    std::set<String> attached_partition_ids;
    ParserPartition parser;
    FormatSettings format_settings;
    for (const auto & part : prepared_parts)
    {
        if (!attached_partition_ids.contains(part->info.partition_id))
        {
            attached_partition_ids.insert(part->info.partition_id);
            WriteBufferFromOwnString writer;
            part->partition.serializeText(target_tbl, writer, format_settings);
            LOG_TRACE(logger, fmt::format("Attached partition {}", writer.str()));
            String formated_partition = writer.str();
            ASTPtr partition_ast = parseQuery(parser, formated_partition, query_ctx->getSettingsRef().max_query_size,
                 query_ctx->getSettingsRef().max_parser_depth);
            attached_partitions.push_back(partition_ast);
        }
    }
    if (is_unique_tbl)
    {
        DeleteBitmapMetaPtrVector bitmap_metas;
        size_t parts_num = prepared_parts.size();
        bitmap_metas.reserve(parts_num);
        for (const auto & part : prepared_parts)
        {
            std::lock_guard<std::mutex> lock(unique_table_info_mutex);
            auto & attach_meta = attach_metas[part->name];
            if (attach_meta)
            {
                bitmap_metas.emplace_back(std::make_shared<DeleteBitmapMeta>(target_tbl, attach_meta));
                LOG_TRACE(logger, "Delete bitmap of part {} exists, will be attached.", part->name);
            }
        }

        if (query_ctx->getSettingsRef().enable_unique_table_attach_without_dedup)
        {
            MutableMergeTreeDataPartsCNCHVector visible_parts;
            MutableMergeTreeDataPartsCNCHVector staged_parts;
            visible_parts.reserve(parts_num);
            staged_parts.reserve(parts_num);
            for (const auto & part : prepared_parts)
            {
                std::lock_guard<std::mutex> lock(unique_table_info_mutex);
                if (attach_metas[part->name])
                {
                    LOG_TRACE(
                        logger,
                        "Part {} to be attached will be visible directly for unique table {} when enable_unique_table_attach_without_dedup",
                        part->name,
                        target_tbl.getStorageID().getNameForLogs());
                    visible_parts.emplace_back(std::move(part));
                }
                else
                {
                    LOG_TRACE(
                        logger,
                        "Part {} to be attached will be staged for unique table {} when enable_unique_table_attach_without_dedup",
                        part->name,
                        target_tbl.getStorageID().getNameForLogs());
                    staged_parts.emplace_back(std::move(part));
                    staged_parts_name.insert(part->info.getPartName());
                }
            }
            size_t visible_parts_size = visible_parts.size();
            size_t staged_parts_size = staged_parts.size();
            size_t bitmap_metas_size = bitmap_metas.size();
            cnch_writer.commitPreparedCnchParts(DumpedData{
                .parts = std::move(visible_parts),
                .bitmaps = std::move(bitmap_metas),
                .staged_parts = std::move(staged_parts),
            });
            LOG_DEBUG(
                logger,
                "Unique table {} attach {} visible parts, {} staged parts, {} bitmaps.",
                target_tbl.getStorageID().getNameForLogs(),
                visible_parts_size,
                staged_parts_size,
                bitmap_metas_size);
        }
        else
        {
            for (const auto & part : prepared_parts)
                staged_parts_name.insert(part->info.getPartName());

            size_t staged_parts_size = prepared_parts.size();
            size_t bitmap_metas_size = bitmap_metas.size();
            cnch_writer.commitPreparedCnchParts(DumpedData{
                .bitmaps = std::move(bitmap_metas),
                .staged_parts = std::move(prepared_parts),
            });

            LOG_DEBUG(
                logger,
                "Unique table {} attach {} staged parts, {} bitmaps.",
                target_tbl.getStorageID().getNameForLogs(),
                staged_parts_size,
                bitmap_metas_size);
        }
    }
    else
    {
        cnch_writer.commitPreparedCnchParts(DumpedData{
            .parts = std::move(prepared_parts),
        });
    }
    injectFailure(AttachFailurePoint::MID_COMMIT_FAIL);
}

void CnchAttachProcessor::loadUniqueDeleteMeta(IMergeTreeDataPartPtr & part, const MergeTreePartInfo & info)
{
    const auto & disk = part->volume->getDisk();
    String meta_file_relative_path;
    /// Check if delete bitmap meta exists.
    {
        std::lock_guard<std::mutex> lock(unique_table_info_mutex);
        auto & delete_file_relative_path = part_delete_file_relative_paths[part->name];
        if (delete_file_relative_path.empty())
        {
            LOG_DEBUG(logger,
                "Part " + part->name + " delete_file_relative_path is empty, it may happen when unique table attach detach partition from non-unique table, ignore it.");
            return;
        }

        meta_file_relative_path = std::filesystem::path(target_tbl.getRelativeDataPath(IStorage::StorageLocation::MAIN))
            / delete_file_relative_path / (part->name + ".meta");
        if (!disk->exists(meta_file_relative_path))
        {
            LOG_DEBUG(
                logger, "Delete bitmap meta (path: " + meta_file_relative_path + ") of part " + part->name + " doesn't exist. Ignore it.");
            return;
        }
    }

    DataModelDeleteBitmapPtr meta_ptr = std::make_shared<Protos::DataModelDeleteBitmap>();
    meta_ptr->set_partition_id(info.partition_id);
    meta_ptr->set_part_min_block(info.min_block);
    meta_ptr->set_part_max_block(info.max_block);
    meta_ptr->set_type(static_cast<Protos::DataModelDeleteBitmap_Type>(DeleteBitmapMetaType::Base));
    meta_ptr->set_txn_id(query_ctx->getCurrentTransaction()->getPrimaryTransactionID().toUInt64());

    std::unique_ptr<ReadBufferFromFileBase> meta_file = disk->readFile(meta_file_relative_path);

    UInt8 meta_format_version{0};
    readIntBinary(meta_format_version, *meta_file);
    if (meta_format_version != DeleteBitmapMeta::delete_file_meta_format_version)
        throw Exception("Unknown delete meta file version: " + toString(meta_format_version), ErrorCodes::UNKNOWN_FORMAT_VERSION);
    size_t cardinality;
    readIntBinary(cardinality, *meta_file);
    meta_ptr->set_cardinality(cardinality);
    if (cardinality <= DeleteBitmapMeta::kInlineBitmapMaxCardinality)
    {
        String inline_value;
        readStringBinary(inline_value, *meta_file);
        meta_ptr->set_inlined_value(inline_value);
    }
    else
    {
        size_t bitmap_file_size;
        readIntBinary(bitmap_file_size, *meta_file);
        meta_ptr->set_file_size(bitmap_file_size);
    }

    std::lock_guard<std::mutex> lock(unique_table_info_mutex);
    attach_metas[part->name] = std::move(meta_ptr);
}

void CnchAttachProcessor::collectPartsFromUnit(const StorageCnchMergeTree& tbl,
    const DiskPtr& disk, String& path, int max_drill_down_level,
    const AttachFilter& filter, MutableMergeTreeDataPartsCNCHVector& founded_parts)
{
    if (max_drill_down_level < 0)
    {
        LOG_INFO(logger, fmt::format("Terminate collect since reach max drill down level at {}", path));
        return;
    }

    MergeTreePartInfo part_info;
    auto volume = std::make_shared<SingleDiskVolume>("single_disk_vol", disk);
    for (auto iter = disk->iterateDirectory(path); iter->isValid(); iter->next())
    {
        String current_entry_path = std::filesystem::path(path) / iter->name();
        if (disk->isDirectory(current_entry_path))
        {
            if (MergeTreePartInfo::tryParsePartName(iter->name(), &part_info,
                tbl.format_version))
            {
                if (filter.filter(part_info))
                {
                    // HACK here, since part's relative path to disk is related to storage's
                    // so, have a relative path here
                    founded_parts.push_back(std::make_shared<MergeTreeDataPartCNCH>(
                        tbl, iter->name(), volume,
                        relativePathTo(tbl.getRelativeDataPath(IStorage::StorageLocation::MAIN), current_entry_path)));
                    // load delete bitmap for unique table
                    if (is_unique_tbl)
                    {
                        std::lock_guard<std::mutex> lock(unique_table_info_mutex);
                        part_delete_file_relative_paths[iter->name()] = relativePathTo(
                            target_tbl.getRelativeDataPath(IStorage::StorageLocation::MAIN),
                            std::filesystem::path(path) / DeleteBitmapMeta::delete_files_dir);
                    }
                }
            }
            else
            {
                LOG_TRACE(logger, fmt::format("Failed to parse part name from {}, "
                    "drill down with limit {}", std::string(std::filesystem::path(disk->getPath()) / current_entry_path),
                    max_drill_down_level - 1));

                String dir_name = iter->name() + '/';
                path += dir_name;
                collectPartsFromUnit(tbl, disk, path, max_drill_down_level - 1,
                    filter, founded_parts);
                path.resize(path.size() - dir_name.size());
            }
        }
        else
        {
            LOG_TRACE(logger, fmt::format("When collect parts from disk {}, path {} "
                "is a file, skip", disk->getName(), std::string(std::filesystem::path(disk->getPath()) / iter->path())));
        }
    }
}

CnchAttachProcessor::PartsFromSources CnchAttachProcessor::collectPartsFromActivePartition(
    StorageCnchMergeTree& tbl, AttachContext& attach_ctx)
{
    LOG_DEBUG(logger, fmt::format("Collect parts from table {} active parts",
        tbl.getLogName()));

    IMergeTreeDataPartsVector parts;
    PartitionCommand drop_command;
    // Detach this partition
    drop_command.detach = true;
    drop_command.type
        = partitionCommandHasWhere(command) ? PartitionCommand::Type::DROP_PARTITION_WHERE : PartitionCommand::Type::DROP_PARTITION;
    drop_command.partition = command.partition->clone();
    tbl.dropPartitionOrPart(drop_command, query_ctx, &parts);

    injectFailure(AttachFailurePoint::DETACH_PARTITION_FAIL);

    size_t total_parts_num = 0;
    for (const auto& part : parts)
    {
        for (auto curr_part = part; curr_part != nullptr; curr_part = curr_part->tryGetPreviousPart())
        {
            ++total_parts_num;
        }
    }

    verifyPartsNum(total_parts_num);

    // dropPartition will commit old transaction, we need to create a
    // new transaction here
    if (query_ctx->getCurrentTransaction()->getStatus() == CnchTransactionStatus::Finished)
    {
        TransactionCnchPtr txn = query_ctx->getCnchTransactionCoordinator()
            .createTransaction(CreateTransactionOption().setAsyncPostCommit(query_ctx->getSettingsRef().async_post_commit));
        attach_ctx.setAdditionalTxn(txn);
        query_ctx->setCurrentTransaction(txn, false);
    }

    // Convert part
    return PartsFromSources{toCNCHPartsVec(parts)};
}

std::pair<String, DiskPtr> CnchAttachProcessor::findBestDiskForHDFSPath(
    const String& from_path)
{
    // If target is a subdirectory of root, return longest common depth
    auto prefix_match = [](const String& root, const String& target) {
        Poco::Path root_path = Poco::Path::forDirectory(root);
        Poco::Path target_path = Poco::Path::forDirectory(target);

        if (!root_path.isAbsolute() || !target_path.isAbsolute())
        {
            throw Exception(fmt::format("Expect only absolute path, but got root {}, target {}",
                root, target), ErrorCodes::BAD_ARGUMENTS);
        }

        if (root_path.depth() > target_path.depth())
        {
            return std::make_pair<UInt32, String>(0, "");
        }
        for (int i = 0, limit = root_path.depth(); i < limit; ++i)
        {
            if (root_path[i] != target_path[i])
            {
                return std::make_pair<UInt32, String>(0, "");
            }
        }
        std::filesystem::path rel_path;
        for (int i = root_path.depth(), limit = target_path.depth(); i < limit; ++i)
        {
            rel_path /= target_path[i];
        }
        return std::make_pair<UInt32, String>(target_path.depth() - root_path.depth(),
            String(rel_path));
    };

    DiskPtr best_disk = nullptr;
    UInt32 max_match_depth = 0;
    String rel_path_on_disk;

    Disks disks = target_tbl.getStoragePolicy(IStorage::StorageLocation::MAIN)->getDisks();
    for (const DiskPtr& disk : disks)
    {
        std::pair<UInt32, String> res = prefix_match(disk->getPath(), from_path);
        if (res.first > max_match_depth)
        {
            best_disk = disk;
            max_match_depth = res.first;
            rel_path_on_disk = res.second;
        }
    }

    if (best_disk == nullptr)
    {
        best_disk = target_tbl.getStoragePolicy(IStorage::StorageLocation::MAIN)->getVolume(0)->getDefaultDisk();
        // Since currently table will assume it's data in disk_root/{table_uuid},
        // Use a relative path to hack here...
        // Currently we use default disk, maybe use disk with most prefix?
        rel_path_on_disk = relativePathTo(best_disk->getPath(),
            from_path);
        LOG_INFO(logger, fmt::format("Path {} is not contained in any name node, "
            "will use default disk, disk base path: {}, relative path: {}",
            from_path, best_disk->getPath(), rel_path_on_disk));
    }
    return std::pair<String, DiskPtr>(rel_path_on_disk, best_disk);
}

// Return flatten data parts
CnchAttachProcessor::PartsWithHistory  CnchAttachProcessor::prepareParts(
    const PartsFromSources& parts_from_sources, AttachContext& attach_ctx)
{
    // Old part and corresponding new part info
    std::vector<std::vector<std::pair<IMergeTreeDataPartPtr, MergeTreePartInfo>>> parts_and_infos_from_sources;
    // Use multiset to prevent different source have part with same name
    std::multiset<String> visible_part_names;

    size_t total_parts_count = 0;
    size_t total_rows_count = 0;

    UInt64 current_tx_id = query_ctx->getCurrentTransactionID().toUInt64();
    for (const MutableMergeTreeDataPartsCNCHVector & visible_parts : parts_from_sources)
    {
        parts_and_infos_from_sources.emplace_back();
        std::vector<std::pair<IMergeTreeDataPartPtr, MergeTreePartInfo>> & parts_and_infos =
            parts_and_infos_from_sources.back();

        for (const MutableMergeTreeDataPartCNCHPtr & part : visible_parts)
        {
            UInt64 new_block_id = query_ctx->getTimestamp();
            UInt64 new_mutation = current_tx_id;

            visible_part_names.insert(part->name);

            for (IMergeTreeDataPartPtr current_part = part; current_part != nullptr;
                current_part = current_part->tryGetPreviousPart())
            {
                auto prev_part = current_part->tryGetPreviousPart();
                if (current_part->isPartial() &&
                    (prev_part == nullptr || current_part->info.hint_mutation != prev_part->info.mutation))
                {
                    throw Exception("Previous part of partial part is absent", ErrorCodes::LOGICAL_ERROR);
                }

                auto new_part_info = MergeTreePartInfo::fromPartName(
                    current_part->info.getPartNameWithHintMutation(), target_tbl.format_version);
                new_part_info.min_block = new_block_id;
                new_part_info.max_block = new_block_id;
                new_part_info.mutation = new_mutation--;

                if (current_part->isPartial())
                {
                    new_part_info.hint_mutation = new_mutation;
                }

                if (!current_part->deleted)
                {
                    total_rows_count += current_part->rows_count;
                }

                parts_and_infos.emplace_back(current_part, new_part_info);
            }
        }

        total_parts_count += parts_and_infos.size();
    }

    ProfileEvents::increment(ProfileEvents::NumOfRowsToAttach, total_rows_count);

    injectFailure(AttachFailurePoint::ROWS_ASSERT_FAIL);

    if (size_t expected_rows = query_ctx->getSettingsRef().cnch_part_attach_assert_rows_count;
        expected_rows != 0 && expected_rows != total_rows_count)
    {
        throw Exception(fmt::format("Expected rows count {} but got {}", expected_rows, total_rows_count),
            ErrorCodes::BAD_ARGUMENTS);
    }

    // Parallel rename, move parts from source location to target location
    PartsWithHistory parts_with_history;
    parts_with_history.first.resize(total_parts_count);
    parts_with_history.second.resize(total_parts_count);

    DiskType::Type remote_disk_type = target_tbl.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk()->getType();

    switch (remote_disk_type)
    {
        case DiskType::Type::ByteHDFS:
        {
            // Create target directory first
            Disks disks = target_tbl.getStoragePolicy(IStorage::StorageLocation::MAIN)->getDisks();
            for (const DiskPtr & disk : disks)
            {
                disk->createDirectories(target_tbl.getRelativeDataPath(IStorage::StorageLocation::MAIN));
            }

            injectFailure(AttachFailurePoint::PREPARE_WRITE_UNDO_FAIL);

            // Write rename record to kv first
            for (auto & parts_and_infos : parts_and_infos_from_sources)
            {
                for (auto & part_and_info : parts_and_infos)
                {
                    IMergeTreeDataPartPtr part = part_and_info.first;
                    MergeTreePartInfo part_info = part_and_info.second;
                    String part_name = part_info.getPartNameWithHintMutation();
                    String tbl_rel_path = target_tbl.getRelativeDataPath(IStorage::StorageLocation::MAIN);
                    String target_path = std::filesystem::path(tbl_rel_path) / part_name / "";
                    attach_ctx.writeRenameRecord(part->volume->getDefaultDisk(), part->getFullRelativePath(), target_path);
                    if (is_unique_tbl)
                    {
                        loadUniqueDeleteMeta(part, part_info);

                        DataModelDeleteBitmapPtr attach_meta;
                        String part_delete_file_relative_path;
                        {
                            std::lock_guard<std::mutex> lock(unique_table_info_mutex);
                            attach_meta = attach_metas[part->name];
                            part_delete_file_relative_path = part_delete_file_relative_paths[part->name];
                        }
                        if (attach_meta)
                        {
                            String bitmap_rel_path = std::filesystem::path(tbl_rel_path) / part_delete_file_relative_path;
                            String bitmap_target_path = std::filesystem::path(bitmap_rel_path) / (part->name + ".meta");
                            attach_ctx.writeMetaFilesNameRecord(part->volume->getDefaultDisk(), bitmap_target_path);
                            String to_path
                                = std::filesystem::path(tbl_rel_path) / DeleteBitmapMeta::deleteBitmapFileRelativePath(*attach_meta);
                            String from_path = to_path;
                            if (attach_meta->cardinality() > DeleteBitmapMeta::kInlineBitmapMaxCardinality)
                            {
                                // Write delete bitmap rename record
                                from_path = std::filesystem::path(bitmap_rel_path) / (part->name + ".bitmap");
                            }
                            /// It's necessary to write rename record no matter if it has bitmap file, because we need to write undo buffer to KV.
                            attach_ctx.writeRenameRecord(part->volume->getDefaultDisk(), from_path, to_path);
                        }
                    }
                }
            }
            attach_ctx.writeRenameMapToKV(
                *(query_ctx->getCnchCatalog()),
                target_tbl.getCnchStorageID(),
                query_ctx->getCurrentTransaction()->getTransactionID());

            auto table_def_hash = target_tbl.getTableHashForClusterBy().getDeterminHash();
            bool is_user_defined_cluster_by_expression = target_tbl.getInMemoryMetadataPtr()->getIsUserDefinedExpressionFromClusterByKey();
            size_t offset = 0;
            auto & worker_pool = attach_ctx.getWorkerPool(total_parts_count);
            for (auto & parts_and_infos : parts_and_infos_from_sources)
            {
                for (auto & part_and_info : parts_and_infos)
                {
                    worker_pool.scheduleOrThrowOnError(
                        [&parts_with_history, table_def_hash, is_user_defined_cluster_by_expression, offset, part = part_and_info.first, part_info = part_and_info.second, this]() {
                            String part_name = part_info.getPartNameWithHintMutation();
                            String tbl_rel_path = target_tbl.getRelativeDataPath(IStorage::StorageLocation::MAIN);
                            String target_path = std::filesystem::path(tbl_rel_path) / part_name / "";
                            const auto & disk = part->volume->getDisk();
                            disk->moveDirectory(part->getFullRelativePath(), target_path);
                            DataModelDeleteBitmapPtr attach_meta;
                            if (is_unique_tbl)
                            {
                                String bitmap_rel_path;
                                {
                                    std::lock_guard<std::mutex> lock(unique_table_info_mutex);
                                    attach_meta = attach_metas[part->name];
                                    bitmap_rel_path = part_delete_file_relative_paths[part->name];
                                }
                                if (attach_meta && attach_meta->cardinality() > DeleteBitmapMeta::kInlineBitmapMaxCardinality)
                                {
                                    // Move delete files
                                    String dir_rel_path = std::filesystem::path(tbl_rel_path)
                                        / DeleteBitmapMeta::deleteBitmapDirRelativePath(part_info.partition_id);
                                    if (!disk->exists(dir_rel_path))
                                        disk->createDirectories(dir_rel_path);
                                    String from_path = std::filesystem::path(tbl_rel_path) / bitmap_rel_path / (part->name + ".bitmap");
                                    String to_path = std::filesystem::path(tbl_rel_path)
                                        / DeleteBitmapMeta::deleteBitmapFileRelativePath(*attach_meta);
                                    disk->moveFile(from_path, to_path);
                                }
                            }

                            Protos::DataModelPart part_model;
                            fillPartModel(target_tbl, *part, part_model, true);
                            // Assign new part info
                            auto part_info_model = part_model.mutable_part_info();
                            part_info_model->set_partition_id(part_info.partition_id);
                            part_info_model->set_min_block(part_info.min_block);
                            part_info_model->set_max_block(part_info.max_block);
                            part_info_model->set_level(part_info.level);
                            part_info_model->set_mutation(part_info.mutation);
                            part_info_model->set_hint_mutation(part_info.hint_mutation);

                            // Discard part's commit time & end time
                            part_model.set_commit_time(IMergeTreeDataPart::NOT_INITIALIZED_COMMIT_TIME);
                            part_model.clear_end_time();
                            parts_with_history.first[offset] = part;
                            parts_with_history.second[offset] = createPartFromModel(target_tbl, part_model, part_name);
                            if (!query_ctx->getSettingsRef().allow_attach_parts_with_different_table_definition_hash || is_user_defined_cluster_by_expression)
                                parts_with_history.second[offset]->table_definition_hash = table_def_hash;

                            if (is_unique_tbl && attach_meta)
                            {
                                std::lock_guard<std::mutex> lock(unique_table_info_mutex);
                                attach_metas[parts_with_history.second[offset]->name] = std::move(attach_meta);
                            }

                            injectFailure(AttachFailurePoint::MOVE_PART_FAIL);
                        });
                    ++offset;
                }
            }
            worker_pool.wait();
            break;
        }
        case DiskType::Type::ByteS3:
        {
            UndoResources undo_resources;
            TxnTimestamp txn_id = query_ctx->getCurrentTransaction()->getTransactionID();

            size_t offset = 0;
            UInt64 table_def_hash = target_tbl.getTableHashForClusterBy().getDeterminHash();
            bool is_user_defined_cluster_by_expression = target_tbl.getInMemoryMetadataPtr()->getIsUserDefinedExpressionFromClusterByKey();
            String from_storage_uuid = from_storage == nullptr ? "" : UUIDHelpers::UUIDToString(from_storage->getStorageUUID());

            std::unordered_map<String, LocalDeleteBitmapPtr> new_bitmaps;
            for (auto & parts_and_infos : parts_and_infos_from_sources)
            {
                for (std::pair<IMergeTreeDataPartPtr, MergeTreePartInfo>& part_and_info : parts_and_infos)
                {
                    const IMergeTreeDataPartPtr& part = part_and_info.first;
                    const MergeTreePartInfo& part_info = part_and_info.second;
                    String part_name = part->info.getPartName();

                    if (!from_storage_uuid.empty())
                    {
                        // Write part's origin meta into undo buffer, so when we rollback
                        // we can revert changes like part's column commit time
                        Protos::DataModelPart origin_part_model;
                        fillPartModel(*from_storage, *part, origin_part_model);
                        undo_resources.emplace_back(txn_id, UndoResourceType::S3AttachPart,
                            from_storage_uuid, part->info.getPartNameWithHintMutation(), part->info.getPartName(),
                            origin_part_model.SerializeAsString(), part_info.getPartName());
                    }
                    else
                    {
                        undo_resources.emplace_back(txn_id, UndoResourceType::S3VolatilePart,
                            part->info.getPartNameWithHintMutation());
                    }
                    Protos::DataModelPart part_model;
                    fillPartModel(target_tbl, *part, part_model, true);
                    // Assign new part info
                    auto part_info_model = part_model.mutable_part_info();
                    part_info_model->set_partition_id(part_info.partition_id);
                    part_info_model->set_min_block(part_info.min_block);
                    part_info_model->set_max_block(part_info.max_block);
                    part_info_model->set_level(part_info.level);
                    part_info_model->set_mutation(part_info.mutation);
                    part_info_model->set_hint_mutation(part_info.hint_mutation);

                    // Discard part's commit time & end time
                    part_model.set_commit_time(IMergeTreeDataPart::NOT_INITIALIZED_COMMIT_TIME);
                    part_model.clear_end_time();
                    parts_with_history.first[offset] = part;
                    parts_with_history.second[offset] = createPartFromModel(target_tbl, part_model, part_info.getPartNameWithHintMutation());
                    if (!query_ctx->getSettingsRef().allow_attach_parts_with_different_table_definition_hash || is_user_defined_cluster_by_expression)
                        parts_with_history.second[offset]->table_definition_hash = table_def_hash;

                    // Rewrite delete bitmap file
                    if (is_unique_tbl && !from_storage_uuid.empty()) /// attach part from path will not have bitmap
                    {
                        DataModelDeleteBitmapPtr attach_meta;
                        {
                            std::lock_guard<std::mutex> lock(unique_table_info_mutex);
                            attach_meta = attach_metas[part->name];
                        }
                        if (attach_meta)
                        {
                            /// Due to S3 don't support move file, and delete bitmap file is small.
                            /// For convenience, we generate a new bitmap file here.
                            DeleteBitmapPtr bitmap = std::make_shared<Roaring>();
                            deserializeDeleteBitmapInfo(part->storage, attach_meta, bitmap);
                            auto new_delete_bitmap = LocalDeleteBitmap::createBase(part_info, bitmap, txn_id, part->bucket_number);
                            auto & new_bitmap_model = new_delete_bitmap->getModel();
                            UndoResource ub(
                                txn_id,
                                UndoResourceType::S3AttachDeleteBitmap,
                                from_storage_uuid,
                                dataModelName(*attach_meta),
                                attach_meta->SerializeAsString(),
                                dataModelName(*new_bitmap_model),
                                DeleteBitmapMeta::deleteBitmapFileRelativePath(*new_bitmap_model));
                            ub.setDiskName(part->volume->getDisk()->getName());
                            undo_resources.emplace_back(ub);
                            new_bitmaps[parts_with_history.second[offset]->name] = std::move(new_delete_bitmap);
                        }
                    }

                    ++offset;
                }
            }

            /// Write undo buffer first
            query_ctx->getCnchCatalog()->writeUndoBuffer(target_tbl.getCnchStorageID(), txn_id, undo_resources);

            /// Dump new bitmap
            for (const auto & [part_name, new_bitmap] : new_bitmaps)
            {
                auto new_bitmap_meta = new_bitmap->dump(target_tbl);
                std::lock_guard<std::mutex> lock(unique_table_info_mutex);
                attach_metas[part_name] = new_bitmap_meta->getModel();
            }

            if (is_unique_tbl)
                LOG_DEBUG(logger, "Unique table {} generates {} new bitmaps.", target_tbl.getStorageID().getNameForLogs(), new_bitmaps.size());
            break;
        }
        default:
            throw Exception(fmt::format("Unsupported remote volume type {} when attach",
                DiskType::toString(remote_disk_type)), ErrorCodes::BAD_ARGUMENTS);
    }
    return parts_with_history;
}

void CnchAttachProcessor::genPartsDeleteMark(PartsWithHistory & parts_to_write)
{
    injectFailure(AttachFailurePoint::GEN_DELETE_MARK_FAIL);

    auto parts_to_drop = target_tbl.selectPartsByPartitionCommand(query_ctx, command).first;
    if (!parts_to_drop.empty())
    {
        if (command.part)
        {
            throw Exception(fmt::format("Trying to attach part, but {} already exists",
                parts_to_drop.front()->name()), ErrorCodes::BAD_ARGUMENTS);
        }

        if (target_tbl.isBucketTable() && !query_ctx->getSettingsRef().skip_table_definition_hash_check)
        {
            auto table_def_hash = target_tbl.getTableHashForClusterBy();
            for (const auto& part : parts_to_drop)
            {
                if (part->part_model().bucket_number() < 0 || !table_def_hash.match(part->part_model().table_definition_hash()))
                {
                    LOG_ERROR(logger, fmt::format("Part's table_definition_hash [{}] is "
                        "different from target's table_definition_hash [{}]",
                        part->part_model().table_definition_hash(), table_def_hash.toString()));
                    throw Exception("Source parts are not bucket parts or have different CLUSTER BY definition from the target table. ",
                        ErrorCodes::BUCKET_TABLE_ENGINE_MISMATCH);
                }
            }
        }

        S3ObjectMetadata::PartGeneratorID part_generator_id(S3ObjectMetadata::PartGeneratorID::TRANSACTION,
            query_ctx->getCurrentTransactionID().toString());
        MergeTreeCNCHDataDumper dumper(target_tbl, part_generator_id);
        for (auto && temp_part : target_tbl.createDropRangesFromParts(query_ctx, parts_to_drop, query_ctx->getCurrentTransaction()))
        {
            auto dumped_part = dumper.dumpTempPart(temp_part);
            dumped_part->is_temp = false;
            parts_to_write.first.push_back(nullptr);
            parts_to_write.second.push_back(std::move(dumped_part));
        }
    }
}

void CnchAttachProcessor::waitingForDedup(const NameSet & partitions_filter, const NameSet & staged_parts_name)
{
    LOG_INFO(
        logger,
        "Attach partition committed {} staged parts. waiting to dedup in {}",
        staged_parts_name.size(),
        toString(partitions_filter));

    /// Sync the attach process to wait for the dedup to finish before returns.
    UInt64 unique_key_attach_partition_timeout = query_ctx->getSettingsRef().unique_key_attach_partition_timeout.totalMilliseconds();

    Stopwatch timer;
    while (true)
    {
        auto ts = query_ctx->getTimestamp();
        auto curr_staged_parts = target_tbl.getStagedParts(ts, &partitions_filter);
        bool exists = false;
        for (const auto & part : curr_staged_parts)
            exists |= staged_parts_name.count(part->name);

        if (!exists)
            break;
        else if (timer.elapsedMilliseconds() >= unique_key_attach_partition_timeout)
            throw Exception("Attach partition timeout for unique table", ErrorCodes::TIMEOUT_EXCEEDED);

        /// Sleep for a while, not burning cpu cycles.
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    LOG_INFO(logger, "Attach partition dedup {} parts finished, costs {} ms", staged_parts_name.size(), timer.elapsedMilliseconds());
}

void CnchAttachProcessor::refreshView(const std::vector<ASTPtr>& attached_partitions, AttachContext& attach_ctx)
{
   try
   {
       ExceptionHandler except_handler;
       ThreadPool& worker_pool = attach_ctx.getWorkerPool(attached_partitions.size());
       for (const auto& partition_ast : attached_partitions)
       {
           worker_pool.scheduleOrThrowOnError(createExceptionHandledJob([this, ast = partition_ast]() {
               auto refresh_context = Context::createCopy(query_ctx);
               auto worker_group = getWorkerGroupForTable(target_tbl, refresh_context);
               refresh_context->setCurrentWorkerGroup(worker_group);
               std::vector<StoragePtr> views = getViews(target_tbl.getStorageID(), refresh_context);

               for (auto& view : views)
               {
                   if (auto * mv = dynamic_cast<StorageMaterializedView*>(view.get()))
                   {
                       mv->refresh(ast, refresh_context, true);
                   }
               }
           }, except_handler));
       }

       worker_pool.wait();
       except_handler.throwIfException();
   }
   catch(...)
   {
       tryLogCurrentException(logger);
   }
}

void CnchAttachProcessor::verifyPartsNum(size_t parts_num) const
{
    ProfileEvents::increment(ProfileEvents::PartsToAttach, parts_num);

    injectFailure(AttachFailurePoint::PARTS_ASSERT_FAIL);

    if (size_t expected_parts = query_ctx->getSettingsRef().cnch_part_attach_assert_parts_count;
        expected_parts != 0 && parts_num != expected_parts)
    {
        throw Exception(fmt::format("Expected parts count {} but got {}", expected_parts,
            parts_num), ErrorCodes::BAD_ARGUMENTS);
    }
}

void CnchAttachProcessor::commitPartsFromS3(const PartsWithHistory & parts_with_history, NameSet & staged_parts_name)
{
    size_t parts_num = parts_with_history.second.size();
    MutableMergeTreeDataPartsCNCHVector prepared_parts;
    MutableMergeTreeDataPartsCNCHVector staged_parts;
    DeleteBitmapMetaPtrVector detached_bitmaps;
    DeleteBitmapMetaPtrVector new_bitmaps;
    if (is_unique_tbl)
    {
        prepared_parts.reserve(parts_num);
        staged_parts.reserve(parts_num);
        std::lock_guard<std::mutex> lock(unique_table_info_mutex);
        for (size_t i = 0; i < parts_with_history.second.size(); ++i)
        {
            const auto & part = parts_with_history.second[i];
            const auto & attach_meta = attach_metas[part->name];

            /// Handle bitmaps
            if (attach_meta)
            {
                new_bitmaps.emplace_back(std::make_shared<DeleteBitmapMeta>(target_tbl, attach_meta));

                const auto & former_part = parts_with_history.first[i];
                const auto & former_meta = attach_metas[former_part->name];
                if (!former_meta)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR, "Former meta for part {} is not exist while it has new meta", former_part->name);

                auto & merge_tree_storage = dynamic_cast<MergeTreeMetaBase &>(*from_storage);
                detached_bitmaps.push_back(std::make_shared<DeleteBitmapMeta>(merge_tree_storage, former_meta));
            }

            /// Handle parts
            if (attach_meta && query_ctx->getSettingsRef().enable_unique_table_attach_without_dedup)
                prepared_parts.emplace_back(std::move(part));
            else
            {
                staged_parts_name.insert(part->info.getPartName());
                staged_parts.emplace_back(std::move(part));
            }
        }
    }
    else
    {
        prepared_parts = std::move(parts_with_history.second);
    }

    // Commit transaction
    injectFailure(AttachFailurePoint::BEFORE_COMMIT_FAIL);

    if (command.from_table.empty() && !command.from_zookeeper_path.empty())
    {
        CnchDataWriter cnch_writer(target_tbl, query_ctx, ManipulationType::Insert);
        if (is_unique_tbl)
        {
            cnch_writer.commitPreparedCnchParts(DumpedData{
                .staged_parts = std::move(parts_with_history.second),
            });
            LOG_DEBUG(logger, "Unique table {} attach {} staged parts.", target_tbl.getStorageID().getNameForLogs(), staged_parts.size());
        }
        else
        {
            cnch_writer.commitPreparedCnchParts(DumpedData{
                .parts = std::move(parts_with_history.second),
            });
        }

        injectFailure(AttachFailurePoint::MID_COMMIT_FAIL);
        return;
    }

    CnchDataWriter cnch_writer(target_tbl, query_ctx, ManipulationType::Attach);
    std::unique_ptr<S3AttachPartsInfo> s3_parts_info;

    if (!command.from_table.empty())
    {
        s3_parts_info = std::make_unique<S3AttachPartsInfo>(from_storage, parts_with_history.first, prepared_parts, staged_parts, detached_bitmaps, new_bitmaps);
    }
    else
    {
        s3_parts_info = std::make_unique<S3AttachPartsInfo>(target_tbl.shared_from_this(), parts_with_history.first, prepared_parts, staged_parts, detached_bitmaps, new_bitmaps);
    }

    cnch_writer.commitPreparedCnchParts({}, s3_parts_info);

    if (is_unique_tbl)
    {
        LOG_DEBUG(
            logger,
            "Unique table {} attach {} visible parts, {} staged parts, {} bitmaps.",
            target_tbl.getStorageID().getNameForLogs(),
            prepared_parts.size(),
            staged_parts.size(),
            new_bitmaps.size());
    }

    injectFailure(AttachFailurePoint::MID_COMMIT_FAIL);
}


void CnchAttachProcessor::injectFailure(AttachFailurePoint point) const
{
    if (unlikely(failure_injection_knob & static_cast<int>(point)))
    {
        throw Exception("Injected exception", ErrorCodes::NETWORK_ERROR);
    }
}

void CnchAttachProcessor::tryPreload(MutableMergeTreeDataPartsCNCHVector & attached_parts)
{
    const auto & settings = query_ctx->getSettingsRef();
    if (!settings.parts_preload_level || (!target_tbl.getSettings()->parts_preload_level && !target_tbl.getSettings()->enable_preload_parts)
        || !target_tbl.getSettings()->enable_local_disk_cache)
        return;

    try
    {
        if (!attached_parts.empty())
        {
            ServerDataPartsVector preload_parts = createServerPartsFromDataParts(target_tbl, attached_parts);
            target_tbl.sendPreloadTasks(
                query_ctx,
                preload_parts,
                false,
                (target_tbl.getSettings()->enable_preload_parts ? PreloadLevelSettings::AllPreload
                                                                 : target_tbl.getSettings()->parts_preload_level.value),
                time(nullptr));
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Fail to preload");
    }
}

}
