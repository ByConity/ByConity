#include "DumpHelper.h"

#include <Core/Settings.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
// #include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/StorageCloudMergeTree.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <Processors/Executors/PipelineExecutingBlockInputStream.h>
#include <Poco/Logger.h>
#include <Poco/NullChannel.h>
#include <Poco/String.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/Macros.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <rocksdb/db.h>
#include <Common/Coding.h>
#include <Storages/IndexFile/IndexFileWriter.h>
#include <Storages/IndexFile/FilterPolicy.h>
#include <Interpreters/sortBlock.h>
#include <IO/Operators.h>
// #include <IO/WriteBufferFromHDFS.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NO_REPLICA_NAME_GIVEN;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int MANIFEST_OPERATION_ERROR;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int NO_ZOOKEEPER;
}

static String deleteBitmapFileRelativePath(const String & part_name)
{
    std::stringstream ss;
    ss << DeleteBitmapMeta::delete_files_dir << part_name <<  ".bitmap";
    return ss.str();
}

static String deleteMetaFileRelativePath(const String & part_name)
{
    std::stringstream ss;
    ss << DeleteBitmapMeta::delete_files_dir << part_name <<  ".meta";
    return ss.str();
}

static String getDeleteFilesPathForDump(StorageCloudMergeTree & cloud, const DB::String & part_name, bool is_bitmap = true)
{
    DiskPtr disk = cloud.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();
    std::stringstream ss;
    ss << cloud.getRelativeDataPath(IStorage::StorageLocation::MAIN) << "/" << DeleteBitmapMeta::delete_files_dir << part_name;

    if (is_bitmap)
        ss << ".bitmap";
    else
        ss << ".meta";
    return ss.str();
}

void UniqueTableDumpHelper::writeTempUniqueKeyIndex(Block & block, size_t first_rid, rocksdb::DB & temp_index, StorageCloudMergeTree & cloud)
{
    ColumnsWithTypeAndName key_columns;
    for (auto & col_name : cloud.getInMemoryMetadataPtr()->getUniqueKeyColumns())
        key_columns.emplace_back(block.getByName(col_name));

    rocksdb::WriteOptions opts;
    opts.disableWAL = true;

    ColumnPtr version_column;
    if (!unique_version_column.empty())
        version_column = block.getByName(unique_version_column).column;

    SerializationsMap serializations;
    for (auto & col : key_columns)
    {
        if (!serializations.count(col.name))
            serializations.emplace(col.name, col.type->getDefaultSerialization());
    }

    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
    {
        WriteBufferFromOwnString key_buf;
        for (auto & col : key_columns)
            serializations[col.name]->serializeMemComparable(*col.column, i, key_buf);

        String value;
        auto rid = static_cast<UInt32>(first_rid + i);
        PutVarint32(&value, rid);
        if (version_column)
            PutFixed64(&value, version_column->getUInt(i));
        auto status = temp_index.Put(opts, key_buf.str(), value);
        if (!status.ok())
            throw Exception("Failed to add unique key : " + status.ToString(), ErrorCodes::LOGICAL_ERROR);
    }
}

void UniqueTableDumpHelper::generateUniqueIndexFileIfNeed(MergeTreeMetaBase::MutableDataPartPtr & part, StorageCloudMergeTree & cloud, const std::shared_ptr<IDisk> & local_disk)
{
    /// if table is not unique or disk based unique key index, do nothing, otherwise generate unique key index file
    if (!cloud.getInMemoryMetadataPtr()->hasUniqueKey() || part->getChecksums()->files.count(UKI_FILE_NAME))
        return;

    LOG_DEBUG(log, "Part {} do not have {} and start to generate.", part->name, UKI_FILE_NAME);

    String part_relative_path;
    String part_path;
    if (local_disk)
    {
        part_relative_path = "data/" + cloud.getDatabaseName() + '/' + cloud.getTableName() + "/" + part->name + "/" ;
        part_path = local_disk->getPath() + "data/" + cloud.getDatabaseName() + '/' + cloud.getTableName() + '/' + part->name + "/" ;
    }
    else 
        throw Exception("Failed to get local disk, because local disk is null ptr", ErrorCodes::LOGICAL_ERROR);

    LOG_DEBUG(log, "Part part_relative_path = {}, part_path = {}", part_relative_path, part_path);

    String uki_path = part_path + UKI_FILE_NAME;

    /// 1. read unique key index required column from part
    Stopwatch timer;
    ExpressionActionsPtr unique_key_expr = cloud.getInMemoryMetadataPtr()->getUniqueKeyExpression();
    Names read_columns = unique_key_expr->getRequiredColumns();
    if (!unique_version_column.empty())
        read_columns.emplace_back(unique_version_column);

    std::unique_ptr<MergeTreeSequentialSource> source = std::make_unique<MergeTreeSequentialSource>(
        cloud,
        cloud.getStorageSnapshot(cloud.getInMemoryMetadataPtr(), nullptr),
        part,
        read_columns,
        /*direct_io=*/false,
        /*take_column_types_from_storage=*/true,
        /*quite=*/false);
    QueryPipeline pipeline;
    pipeline.init(Pipe(std::move(source)));
    pipeline.setMaxThreads(1);
    BlockInputStreamPtr pipeline_input_stream = std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline));

    auto input = std::make_shared<MaterializingBlockInputStream>(
        std::make_shared<ExpressionBlockInputStream>(
                pipeline_input_stream,
                unique_key_expr));

    input->readPrefix();
    rocksdb::DB * temp_unique_key_index = nullptr;
    Block unique_key_block;
    size_t rows_count = 0;
    while (Block block = input->read())
    {
        size_t rows = block.rows();
        if (temp_unique_key_index != nullptr)
        {
            writeTempUniqueKeyIndex(block, /*first_rid=*/rows_count, *temp_unique_key_index, cloud);
            rows_count += rows;
            continue;
        }
        if (unique_key_block.rows() == 0)
            unique_key_block = std::move(block);
        else
        {
            rocksdb::Options opts;
            opts.create_if_missing = true;
            opts.error_if_exists = true;
            opts.write_buffer_size = 16 << 20; /// 16MB
            String temp_unique_key_index_abs_path = part_path + "TEMP_unique_key_index";
            String temp_unique_key_index_relative_path = part_relative_path + "TEMP_unique_key_index";
            /// avoid files left over from the last execution failure
            if (local_disk->exists(temp_unique_key_index_relative_path))
            {
                LOG_WARNING(log, "local disk {} already unique key index path {}, start to remove it...", local_disk->getPath(), temp_unique_key_index_relative_path);
                local_disk->removeRecursive(temp_unique_key_index_relative_path);
            }

            auto status = rocksdb::DB::Open(opts, temp_unique_key_index_abs_path, &temp_unique_key_index);
            if (!status.ok())
                throw Exception("Can't create temp unique key index at " + temp_unique_key_index_abs_path, ErrorCodes::LOGICAL_ERROR);

            writeTempUniqueKeyIndex(unique_key_block, /*first_rid=*/0, *temp_unique_key_index, cloud);
            rows_count += unique_key_block.rows();
            unique_key_block.clear();
            writeTempUniqueKeyIndex(block, /*first_rid=*/rows_count, *temp_unique_key_index, cloud);
            rows_count += rows;
        }
    }
    input->readSuffix();
    LOG_DEBUG(log, "Read unique key data cost {} ms. ", timer.elapsedMilliseconds());

    /// 2. write unique key index file
    timer.restart();
    IndexFile::Options options;
    options.filter_policy.reset(IndexFile::NewBloomFilterPolicy(10));
    IndexFile::IndexFileWriter index_writer(options);
    size_t keys_count = 0;
    auto status = index_writer.Open(uki_path);
    if (!status.ok())
        throw Exception("Error while opening file " + uki_path + ": " + status.ToString(),
                        ErrorCodes::CANNOT_OPEN_FILE);
    if (temp_unique_key_index)
    {
        std::unique_ptr<rocksdb::Iterator> iter(temp_unique_key_index->NewIterator(rocksdb::ReadOptions()));
        for (iter->SeekToFirst(); iter->Valid(); iter->Next())
        {
            auto key = iter->key();
            auto val = iter->value();
            status = index_writer.Add(Slice(key.data(), key.size()), Slice(val.data(), val.size()));
            if (!status.ok())
                throw Exception("Error while adding key to " + uki_path + ": " + status.ToString(),
                                ErrorCodes::LOGICAL_ERROR);
            keys_count++;
        }
        if (!iter->status().ok())
            throw Exception("Error while scanning temp key index file " + uki_path + ": " + iter->status().ToString(),
                            ErrorCodes::LOGICAL_ERROR);
        iter.reset();
        try
        {
            auto res = temp_unique_key_index->Close();
            if (!res.ok())
                LOG_WARNING(log, "Failed to close temp_unique_key_index {}", status.ToString());
            delete temp_unique_key_index;
            temp_unique_key_index = nullptr;
        }
        catch (...)
        {
            LOG_WARNING(log, getCurrentExceptionMessage(false));
        }
    }
    else
    {
        assert(unique_key_block.rows() > 0);
        rows_count = unique_key_block.rows();
        SortDescription sort_description;
        sort_description.reserve(cloud.getInMemoryMetadataPtr()->getUniqueKeyColumns().size());
        for (auto & name : cloud.getInMemoryMetadataPtr()->getUniqueKeyColumns())
            sort_description.emplace_back(unique_key_block.getPositionByName(name), 1, 1);

        IColumn::Permutation * unique_key_perm_ptr = nullptr;
        IColumn::Permutation unique_key_perm;
        if (!isAlreadySorted(unique_key_block, sort_description))
        {
            stableGetPermutation(unique_key_block, sort_description, unique_key_perm);
            unique_key_perm_ptr = &unique_key_perm;
        }

        ColumnsWithTypeAndName key_columns;
        for (auto & col_name : cloud.getInMemoryMetadataPtr()->getUniqueKeyColumns())
            key_columns.emplace_back(unique_key_block.getByName(col_name));

        ColumnPtr version_column;
        if (!unique_version_column.empty())
            version_column = unique_key_block.getByName(unique_version_column).column;

        SerializationsMap serializations;
        for (auto & col : key_columns)
        {
            if (!serializations.count(col.name))
                serializations.emplace(col.name, col.type->getDefaultSerialization());
        }

        for (UInt32 rid = 0, size = unique_key_block.rows(); rid < size; ++rid)
        {
            size_t idx = unique_key_perm_ptr ? unique_key_perm[rid] : rid;

            WriteBufferFromOwnString key_buf;
            for (auto & col : key_columns)
                serializations[col.name]->serializeMemComparable(*col.column, rid, key_buf);

            String value;
            PutVarint32(&value, static_cast<UInt32>(idx));
            if (version_column)
                PutFixed64(&value, version_column->getUInt(idx));

            status = index_writer.Add(key_buf.str(), value);
            if (!status.ok())
                throw Exception("Error while adding key to " + uki_path + ": " + status.ToString(),
                                ErrorCodes::LOGICAL_ERROR);
        }

        keys_count = unique_key_block.rows();
        unique_key_block.clear();
    }

    if (rows_count != keys_count)
        throw Exception("rows count " + toString(rows_count) + " doesn't match unique keys count " + toString(keys_count), ErrorCodes::LOGICAL_ERROR);

    IndexFile::IndexFileInfo file_info;
    status = index_writer.Finish(&file_info);
    if (!status.ok())
        throw Exception("Error while finishing total file " + uki_path + ": " + status.ToString(), ErrorCodes::LOGICAL_ERROR);

    LOG_DEBUG(log, "Write unique key index file cost {} ms", timer.elapsedMilliseconds());

    /// 3. rewrite checksum
    if (file_info.file_size > 0)
    {
        part->getChecksums()->files[UKI_FILE_NAME].file_size = file_info.file_size;
        part->getChecksums()->files[UKI_FILE_NAME].file_hash = file_info.file_hash;
        part->min_unique_key = file_info.smallest_key;
        part->max_unique_key = file_info.largest_key;
    }
    else
        throw Exception("Write unique key index file size = 0, it's a bug! ", ErrorCodes::LOGICAL_ERROR);


    {
        /// Write file with prepared_checksums
        String name = "checksums.txt";
        WriteBufferFromFile out_temp(part_path + name + ".tmp", 4096);
        part->getChecksums()->writeLocal(out_temp);
        Poco::File file_tmp{part_path + name};
        if (file_tmp.exists())
            file_tmp.renameTo(part_path + name + ".tmp2");

        Poco::File{part_path + name + ".tmp"}.renameTo(part_path + name);

        Poco::File file_remove{part_path + name + ".tmp2"};
        if (file_remove.exists())
            file_remove.remove();
    }
}

void UniqueTableDumpHelper::loadAndDumpDeleteBitmap(StorageCloudMergeTree & cloud, MergeTreeCloudData::DataPartPtr part, const UInt64 & version, const std::shared_ptr<IDisk> & local_disk)
{
    if (!cloud.getInMemoryMetadataPtr()->hasUniqueKey())
        return;

    /// Load delete file
    String part_path;
    if (local_disk)
        part_path = local_disk->getPath() + "data/" + cloud.getDatabaseName() + '/' + cloud.getTableName() + '/' + part->name + "/" ;
    else 
        part_path = cloud.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk()->getPath() + part->name + "/" ;
    String delete_file_path = part_path + "delete." + std::to_string(version);
    ReadBufferFromFile in(delete_file_path, std::min(static_cast<Poco::File::FileSize>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(delete_file_path).getSize()));
    UInt8 format_version;
    readIntBinary(format_version, in);
    if (format_version != 1)
        throw Exception("Unknown delete file version: " + toString(format_version), ErrorCodes::UNKNOWN_FORMAT_VERSION);
    size_t buf_size;
    readIntBinary(buf_size, in);
    PODArray<char> buf(buf_size);
    in.read(buf.data(), buf_size);
    Roaring bitmap = Roaring::read(buf.data());

    /// Dump delete meta file and delete file if necessary 
    DiskPtr disk = cloud.getStoragePolicy(IStorage::StorageLocation::MAIN)->getAnyDisk();
    bool need_bitmap = false;
    {
        const String files_rel_path = cloud.getRelativeDataPath(IStorage::StorageLocation::MAIN) + "/" + DeleteBitmapMeta::delete_files_dir;
        if (!disk->exists(files_rel_path))
            disk->createDirectories(files_rel_path);

        String meta_rel_path = getDeleteFilesPathForDump(cloud, part->name, false);
        std::unique_ptr<WriteBufferFromFileBase> meta_writer = disk->writeFile(meta_rel_path);
        writeIntBinary(DeleteBitmapMeta::delete_file_meta_format_version, *meta_writer);
        writeIntBinary(bitmap.cardinality(), *meta_writer);
        if (bitmap.cardinality() <= DeleteBitmapMeta::kInlineBitmapMaxCardinality)
        {
            // Write inline value
            String value;
            value.reserve(bitmap.cardinality() * sizeof(UInt32));
            for (auto it = bitmap.begin(); it != bitmap.end(); ++it)
                PutFixed32(&value, *it);
            writeStringBinary(value, *meta_writer);
        }
        else
        {
            need_bitmap = true;
            bitmap.runOptimize();
            size_t size = bitmap.getSizeInBytes();
            PODArray<char> write_buf(size);
            size = bitmap.write(write_buf.data());
            writeIntBinary(size, *meta_writer);
            {
                String bitmap_rel_path = getDeleteFilesPathForDump(cloud, part->name, true);
                std::unique_ptr<WriteBufferFromFileBase> bitmap_writer = disk->writeFile(bitmap_rel_path);
                bitmap_writer->write(write_buf.data(), size);
                /// It's necessary to do next() and sync() here, otherwise it will omit the error in WriteBufferFromHDFS::WriteBufferFromHDFSImpl::~WriteBufferFromHDFSImpl() which case file incomplete.
                bitmap_writer->next();
                bitmap_writer->sync();
            }
        }
        meta_writer->next();
        meta_writer->sync();
    }

    auto move_file_to_target = [disk](const String & from_path, const String & to_path)
    {
        disk->removeFileIfExists(to_path);
        disk->moveFile(from_path, to_path);
    };

    const String meta_to_path = cloud.getDatabaseName() + '/' + cloud.getTableName() + '/' + deleteMetaFileRelativePath(part->name) ;
    const String meta_from_path = getDeleteFilesPathForDump(cloud, part->name, false);
    move_file_to_target(meta_from_path, meta_to_path);
    if (need_bitmap)
    {
        const String bitmap_to_path = cloud.getDatabaseName() + '/' + cloud.getTableName() + '/' + deleteBitmapFileRelativePath(part->name) ;
        const String bitmap_from_path = getDeleteFilesPathForDump(cloud, part->name, true);
        move_file_to_target(bitmap_from_path, bitmap_to_path);
    }
}

void UniqueTableDumpHelper::parseZKReplicaPathFromEngineArgs(
    const ASTStorage * storage, const String & /*database*/, const String & /*table*/, const Context & context)
{
    const auto & engine_args = storage->engine->arguments->getChildren();
    String zookeeper_path = "";
    String replica_name = "";
    const auto * ast_zk_path = engine_args[0]->as<ASTLiteral>();
    if (ast_zk_path && ast_zk_path->value.getType() == Field::Types::String)
        zookeeper_path = safeGet<String>(ast_zk_path->value);
    else
        throw Exception(
            "Parse zookeeper replica path failed for unique: Path in ZooKeeper must be a string literal", ErrorCodes::BAD_ARGUMENTS);

    const auto * ast_replica_name = engine_args[1]->as<ASTLiteral>();
    if (ast_replica_name && ast_replica_name->value.getType() == Field::Types::String)
        replica_name = safeGet<String>(ast_replica_name->value);
    else
        throw Exception("Parse zookeeper replica path failed for unique: Replica name must be a string literal", ErrorCodes::BAD_ARGUMENTS);

    if (replica_name.empty())
        throw Exception("Parse zookeeper replica path failed for unique: No replica name in config", ErrorCodes::NO_REPLICA_NAME_GIVEN);

    if (engine_args.size() > 2)
    {
        if (!engine_args.back()->as<ASTIdentifier>() && !engine_args.back()->as<ASTFunction>())
            throw Exception("Version column must be identifier or function expression", ErrorCodes::BAD_ARGUMENTS);

        String partition_key = storage->partition_by ? storage->partition_by->getColumnName() : "";
        if (partition_key == engine_args.back()->getColumnName())
        {
            /// When partition as version, we skip set version column in order to avoiding write version to unique key index
            unique_version_column = "";
        }
        else if (!tryGetIdentifierNameInto(engine_args.back(), unique_version_column))
            throw Exception("Version column name must be an unquoted string", ErrorCodes::BAD_ARGUMENTS);
    }

    /// Expand by marcos
    LOG_DEBUG(log, "Parsed replica zookeeper path before expand: {}, replica_name = {}", zookeeper_path, replica_name);

    zookeeper_path = context.getMacros()->expand(zookeeper_path);
    replica_name = context.getMacros()->expand(replica_name);

    LOG_DEBUG(log, "Parsed replica zookeeper path after expand: {}, replica_name = {}", zookeeper_path, replica_name);

    replica_path = zookeeper_path + "/replicas/" + replica_name;
    auto zookeeper = context.getZooKeeper();
    if (zookeeper->expired())
        throw Exception("ZooKeeper session has expired.", ErrorCodes::NO_ZOOKEEPER);

    zookeeper->createIfNotExists(replica_path + "/dump_lsn", "");
    LOG_DEBUG(log, "Parsed replica zookeeper path: {}", replica_path);
}

Snapshot UniqueTableDumpHelper::getUniqueTableSnapshot(
    const DB::String & database, const DB::String & table, const DB::String & dir, const Context & context)
{
    /// 1. open manifest in read-only mode
    rocksdb::Options options;
    String db_dir = dir + "db";
    rocksdb::DB * db = nullptr;
    auto status = rocksdb::DB::OpenForReadOnly(options, db_dir, &db);
    if (!status.ok())
        throw Exception(" Can't open manifest DB at " + db_dir + " for unique table " + database + "." + table + ": " + status.ToString(), ErrorCodes::DIRECTORY_DOESNT_EXIST);

    /// 2. set dump_lsn on zookeeper
    auto keys = {
        rocksdb::Slice(ManifestStore::commit_version_key),
        rocksdb::Slice(ManifestStore::checkpoint_version_key)
    };
    Strings values(2);
    auto result = db->MultiGet(rocksdb::ReadOptions(), keys, &values);
    for (auto & st : result)
    {
        if (!st.ok())
            throw Exception("Can't read version from manifest DB for dump unique table " + database + "." + table +  ": " + status.ToString(), ErrorCodes::MANIFEST_OPERATION_ERROR);
    }

    UInt64 commit_version = parse<UInt64>(values[0]);
    UInt64 checkpoint_version = parse<UInt64>(values[1]);
    if (commit_version < checkpoint_version)
        throw Exception("Can't get snapshot at " + toString(commit_version) + " because it's older than last checkpoint " + toString(checkpoint_version), ErrorCodes::LOGICAL_ERROR);

    saveDumpVersionToZk(commit_version, context);

    /// 3. get commit_version snapshot
    std::ostringstream ss;
    ss << dir << "/" << "checkpoint." << std::setfill('0') << std::setw(10) << checkpoint_version;
    Snapshot snapshot = manifest_store.readCheckpointFile(checkpoint_version, ss.str());
    String start_key = manifest_store.generateLogKey(checkpoint_version + 1);
    String end_key = manifest_store.generateLogKey(commit_version + 1);
    std::cout << " start key = " << start_key << " end key = " << end_key << std::endl;
    rocksdb::ReadOptions opt;
    rocksdb::Slice upper_bound(end_key);
    opt.iterate_upper_bound = &upper_bound;
    std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(opt));
    for (iter->Seek(start_key); iter->Valid(); iter->Next())
    {
        ReadBufferFromMemory in(iter->value().data(), iter->value().size());
        ManifestLogEntry value;
        manifest_store.readManifest(value, in);
        if (value.type == ManifestLogEntry::MODIFY_SCHEMA)
            continue;
        if (value.type == ManifestLogEntry::SYNC_PARTS)
        {
            for (auto & part : value.added_parts)
                snapshot[part] = value.version;
            for (auto & part : value.updated_parts)
                snapshot[part] = value.version;
            for (auto & part : value.removed_parts)
                snapshot.erase(part);
        }
        if (value.type == ManifestLogEntry::DETACH_PARTS)
        {
            for (auto & part : value.removed_parts)
                snapshot.erase(part);
        }
    }
    if (!iter->status().ok())
        throw Exception("Failed to read manifest in [" + start_key + "," + end_key + ") : " + iter->status().ToString(), ErrorCodes::MANIFEST_OPERATION_ERROR);

    return snapshot;
}

void UniqueTableDumpHelper::saveDumpVersionToZk(const UInt64 & dump_version, const Context & context)
{
    auto zookeeper = context.getZooKeeper();
    if (zookeeper->expired())
        throw Exception("ZooKeeper session has expired.", ErrorCodes::NO_ZOOKEEPER);

    String dump_lsn_path_prefix = replica_path + "/dump_lsn/dump_lsn-";
    dump_lsn_path = zookeeper->create(dump_lsn_path_prefix, toString(dump_version), zkutil::CreateMode::EphemeralSequential);
    LOG_DEBUG(log, "created dump lsn path: {}, value: {}", dump_lsn_path, dump_version);
}

void UniqueTableDumpHelper::removeDumpVersionFromZk(const Context & context)
{
    auto zookeeper = context.getZooKeeper();
    if (zookeeper->expired())
        throw Exception("ZooKeeper session has expired.", ErrorCodes::NO_ZOOKEEPER);

    if (zookeeper->exists(dump_lsn_path))
    {
        zookeeper->remove(dump_lsn_path);
        LOG_DEBUG(log, "Dump lsn path: {} has been removed.", dump_lsn_path);
    }
    /// Try to remove base dump lsn path
    zookeeper->tryRemove(replica_path + "/dump_lsn");
}

void ManifestStore::readLabeledNames(ReadBuffer & in, const char * label, Strings & names)
{
    size_t num;
    in >> label >> num >> "\n";
    names.resize(num);
    for (size_t i = 0; i < num; ++i)
        in >> names[i] >> "\n";
}

void ManifestStore::readManifest(DB::ManifestLogEntry & log_entry, DB::ReadBuffer & in) const
{
    UInt8 format;
    in >> "format:" >> format >> "\n";
    if (format != log_entry.format_version)
        throw Exception("unknown manifest format version : " + toString(format), ErrorCodes::LOGICAL_ERROR);

    LocalDateTime create_time_dt;
    in >> "version:" >> log_entry.version >> "\n";
    in >> "prev_version:" >> log_entry.prev_version >> "\n";
    in >> "create_time:" >> create_time_dt >> "\n";
    log_entry.create_time = create_time_dt;
    in >> "source_replica:" >> log_entry.source_replica >> "\n";

    String type_str;
    in >> type_str >> "\n";
    if (type_str == "sync_parts")
    {
        log_entry.type = ManifestLogEntry::Type::SYNC_PARTS;
        readLabeledNames(in, "added:", log_entry.added_parts);
        readLabeledNames(in, "updated:", log_entry.updated_parts);
        readLabeledNames(in, "removed:", log_entry.removed_parts);
    }
    else if (type_str == "detach_parts")
    {
        log_entry.type = ManifestLogEntry::Type::DETACH_PARTS;
        readLabeledNames(in, "removed:", log_entry.removed_parts);
    }
    else if (type_str == "reset_manifest")
    {
        log_entry.type = ManifestLogEntry::Type::RESET_MANIFEST;
        in >> "target_version:" >> log_entry.target_version >> "\n";
    }
    else if (type_str == "checkpoint_manifest")
    {
        log_entry.type = ManifestLogEntry::Type::CHECKPOINT_MANIFEST;
        in >> "target_version:" >> log_entry.target_version >> "\n";
    }
    else if (type_str == "modify_schema")
    {
        log_entry.type = ManifestLogEntry::Type::MODIFY_SCHEMA;
    }
    else
    {
        throw Exception("Unknown manifest log_entry type: " + type_str, ErrorCodes::LOGICAL_ERROR);
    }
}

Snapshot ManifestStore::readCheckpointFile(DB::UInt64 version, const String & path)
{
    Snapshot result;
    if (version == 0)
        return result;

    ReadBufferFromFile buf(path, std::min(static_cast<Poco::File::FileSize>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
    while (!buf.eof())
    {
        String partName;
        UInt64 delete_version;
        readString(partName, buf);
        assertChar('\t', buf);
        readIntText(delete_version, buf);
        assertChar('\n', buf);
        result[partName] = delete_version;
    }

    return result;
}

}
