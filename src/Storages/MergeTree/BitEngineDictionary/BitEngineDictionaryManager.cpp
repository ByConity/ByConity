
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDictionaryManager.h>
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDataExchanger.h>
#include <Storages/StorageHaMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>

#include <Processors/QueryPipeline.h>
#include <Processors/Executors/PipelineExecutingBlockInputStream.h>
#include <Parsers/queryToString.h>

#include <DataTypes/DataTypeBitMap64.h>
#include <Disks/DiskLocal.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>

#include <mutex>

namespace DB
{
class BitEngineDataExchanger;
class BitEngineDataService;

////////////////////////////    StartOf BitEngineDictionaryManager
BitEngineDictionaryManager::BitEngineDictionaryManager(const String & db_tbl_, const String & disk_name_, const String & dict_path_, ContextPtr context_)
    : BitEngineDictionaryManagerBase<BitEngineDictionaryPtr>(db_tbl_, disk_name_, dict_path_, context_)
    , version_path(dict_path_ + "/bitengine_version")
    , log(&Poco::Logger::get("BitEngineDictionaryManager (" + db_tbl + ")"))
{
    init();
    //std::cout<<" ########  initialize bitengine manager with version " << version << " in shard " << std::to_string(shard_id) << std::endl;
}

void BitEngineDictionaryManager::init()
{
    try{
        loadVersion();
    }catch(...){
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        // TODO: check version
    }
}

BitEngineDictionaryManager::~BitEngineDictionaryManager()
{
    try{
        flushVersion();
    }catch(...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void BitEngineDictionaryManager::setVersion(const size_t version_)
{
    auto lock = getWriteLock();
    version = version_;
}

void BitEngineDictionaryManager::loadVersion()
{
    if (!context->getDisk(disk_name)->exists(version_path))
        return;

    ReadBufferFromFile in(context->getDisk(disk_name)->getPath() + version_path);

    if (in.eof())
        return;

    size_t version_tmp = 0;
    readVarUInt(version_tmp, in);
    if (version_tmp > version)
        version = version_tmp;
}

void BitEngineDictionaryManager::flushVersion()
{
    if (dropped)
        return;

    String version_path_tmp = version_path + ".tmp";
    auto disk = context->getDisk(disk_name);
    try
    {
        auto lock = getWriteLock();
        if (!disk->exists(version_path_tmp))
        {
            // LOG_DEBUG(log, "there is no {}, will create one", version_path_tmp);
            disk->createFile(version_path_tmp);
        }
        WriteBufferFromFile out(fs::path(disk->getPath()) / version_path_tmp);
        writeVarUInt(version, out);
        out.close();

        if (disk->exists(version_path_tmp))
            disk->moveFile(version_path_tmp, version_path);
    }
    catch(...)
    {
        if (disk->exists(version_path_tmp))
            disk->removeFile(version_path_tmp);
        throw;
    }
}

void BitEngineDictionaryManager::flushDict()
{
    auto lock = getWriteLock();

    for (auto & item : dict_containers)
    {
        if (item.second)
            item.second->flushDict();
    }
}

void BitEngineDictionaryManager::reload(const String & column_name)
{
    auto it = dict_containers.find(column_name);
    if (it == dict_containers.end())
    {
        LOG_TRACE(log, "Reload BitEngine dictionary: not find dictionary {}, will create a new one", column_name);
        dict_containers.emplace(column_name, std::make_shared<BitEngineDictionary>(disk_name, path, column_name, context, shard_id, 0, version))
                 .first;
    }
    else
        LOG_TRACE(log, "Reload BitEngine dictionary: find dictionary {} locally", column_name);
}

BitEngineDictionaryPtr BitEngineDictionaryManager::getBitEngineDictPtr(const String & name)
{
    auto lock = getWriteLock();
    auto it = dict_containers.find(name);
    if (it == dict_containers.end())
        it = dict_containers.emplace(name, std::make_shared<BitEngineDictionary>(disk_name, path, name, context, shard_id, 1, version)).first;

    return it->second;
}

void BitEngineDictionaryManager::updateVersionTo(const size_t version_)
{
    {
        auto lock = getWriteLock();
        if (version_ <= version)
            return;

        version = version_;
        LOG_TRACE(log, " Recursive Update version of bitengine dictionary to {}", std::to_string(version));
        for (auto & item : dict_containers)
        {
            if (item.second)
                item.second->updateVersionTo(version);
        }
    }
    // To flush version after version changed to avoid the case the engine is down in an expected way.
    flushVersion();
}

void BitEngineDictionaryManager::updateVersion()
{
    {
        auto lock = getWriteLock();
        version++;
        LOG_TRACE(log, "Update version of bitengine dictionary to {}", std::to_string(version));
        for (auto & item : dict_containers)
        {
            if (item.second)
                item.second->updateVersionTo(version);
        }
    }

    // To flush version after version changed to avoid the case the engine is down in an expected way.
    flushVersion();
}

void BitEngineDictionaryManager::updateSnapshots()
{
    auto write_lock = writeLockForSnapshot();
    for (auto & dict_it : dict_containers)
    {
        auto snapshot_it = dict_snapshots.find(dict_it.first);
        if (snapshot_it == dict_snapshots.end())
            dict_snapshots.emplace(dict_it.first,
                                   std::make_shared<BitEngineDictionarySnapshot>(*(dict_it.second)));
        else if (dict_it.second->needUpdateSnapshot())
        {
            snapshot_it->second->tryUpdateSnapshot<BitEngineDictionary>(*(dict_it.second));
            dict_it.second->resetUpdateSnapshot();
        }
    }
}

void BitEngineDictionaryManager::resetDictImpl()
{
    version = 0;
    flushVersion();

    for (auto & item : dict_containers)
    {
        if (item.second)
            item.second->resetDict();
    }
}

IncrementOffset BitEngineDictionaryManager::getIncrementOffset()
{
    IncrementOffset increment_offset;
    for (auto & item: dict_containers)
    {
        if (item.second)
            increment_offset.increment_offset.emplace(item.first, item.second->getIncrementDictOffset());
    }
    return increment_offset;
}

IncrementData BitEngineDictionaryManager::getIncrementData(const IncrementOffset & increment_offset)
{
    //std::cout<<" bitengine manager will get increment data" << std::endl;
    IncrementData increment_data;
    for (const auto & item: dict_containers)
    {
        auto it = increment_offset.increment_offset.find(item.first);
        if (it == increment_offset.increment_offset.end())
        {
            // empty offset means the increament data starts from offset 0
            IncrementDictOffset empty_offset = item.second->getEmptyIncrementDictOffset();
            increment_data.increment_data.emplace(item.first, item.second->getIncrementDictData(empty_offset));
        }
        else
            increment_data.increment_data.emplace(it->first, item.second->getIncrementDictData(it->second));
    }
    return increment_data;
}

void BitEngineDictionaryManager::insertIncrementData(const IncrementData & increment_data)
{
    for (const auto & item: increment_data.increment_data)
    {
        auto dict_ptr = getBitEngineDictPtr(item.first);
        dict_ptr->insertIncrementDictData(item.second);
    }
}

void BitEngineDictionaryManager::readDataFromReadBuffer(ReadBuffer & in)
{
    size_t dict_size;
    readVarUInt(dict_size, in);

    for (size_t i = 0; i < dict_size; ++i)
    {
        String dict_name;
        readStringBinary(dict_name, in);
        //std::cout<<" manager read column: " << column_name << std::endl;
        auto dict_ptr = getBitEngineDictPtr(dict_name);
        dict_ptr->readDataFromReadBuffer(in);
    }
}

void BitEngineDictionaryManager::writeDataToWriteBuffer(WriteBuffer & out)
{
    size_t dict_size = dict_containers.size();
    writeVarUInt(dict_size, out);

    for (auto & entry : dict_containers)
    {
        writeStringBinary(entry.first, out);
        //std::cout<<" manager write column: " << column_name << std::endl;
        entry.second->writeDataToWriteBuffer(out);
    }
}

std::map<String, UInt64> BitEngineDictionaryManager::getAllDictColumnSize()
{
    std::map<String, UInt64> dict_size;

    for (const auto & entry : dict_containers)
    {
        UInt64 rows{0};
        if (entry.second)
            rows += entry.second->getColumnSize();
        dict_size[entry.first] = rows;
    }
    return dict_size;
}

BitEngineDictionaryManager::Status BitEngineDictionaryManager::getStatus()
{
    auto dict_size = getAllDictColumnSize();
    Strings encoded_columns;
    std::vector<UInt64> encoded_columns_size;
    for (auto name_size : dict_size)
    {
        encoded_columns.push_back(name_size.first);
        encoded_columns_size.push_back(name_size.second);
    }

    BitEngineDictionaryManager::Status status;
    status.version = version;
    status.encoded_columns = std::move(encoded_columns);
    status.encoded_columns_size = std::move(encoded_columns_size);
    status.is_valid = isValid();
    status.shard_id = shard_id;
    status.shard_base_offset = dict_containers.begin()->second->getShardBaseOffset();
    return status;
}

BitEngineDictionarySnapshotPtr BitEngineDictionaryManager::tryGetUpdatedSnapshot(const String & column_name)
{
    auto manager_lock = getWriteLock();
    auto dict_it = dict_containers.find(column_name);
    if (dict_it == dict_containers.end())
    {
        throw Exception("Cannot find dict name: " + column_name + " in all dicts: " + allDictNamesToString(), ErrorCodes::LOGICAL_ERROR);
    }

    auto snapshot_lock_ = writeLockForSnapshot();
    auto snapshot_it = dict_snapshots.find(column_name);
    if (snapshot_it == dict_snapshots.end())
    {
        snapshot_it = dict_snapshots.emplace(column_name,
                                             std::make_shared<BitEngineDictionarySnapshot>(*(dict_it->second))).first;
    }
    else if (dict_it->second->needUpdateSnapshot())
    {
        snapshot_it->second->tryUpdateSnapshot<BitEngineDictionary>(*(dict_it->second));
        dict_it->second->resetUpdateSnapshot();
    }

    return snapshot_it->second;
}

BitEngineDictionarySnapshotPtr BitEngineDictionaryManager::getDictSnapshotPtr(const String & column_name)
{
    return tryGetUpdatedSnapshot(column_name);
}

BitEngineDictionarySnapshot BitEngineDictionaryManager::getDictSnapshot(const String & column_name)
{
    return *tryGetUpdatedSnapshot(column_name);
}

ColumnPtr BitEngineDictionaryManager::decodeColumn(const IColumn & column, const String & dict_name)
{
    auto dict_snapshot = getDictSnapshotPtr(dict_name);
    if (dict_snapshot->empty())
        throw Exception("Got an empty dictionary for decoding bitmap, dict_name: " + dict_name, ErrorCodes::LOGICAL_ERROR);

    return dict_snapshot->decodeColumn(column);
}


ColumnPtr BitEngineDictionaryManager::decodeNonBitEngineColumn(const IColumn & column, String & dict_name)
{
    auto dict_snapshot = getDictSnapshotPtr(dict_name);
    if (dict_snapshot->empty())
        throw Exception("Got an empty dictionary for decoding bitmap", ErrorCodes::LOGICAL_ERROR);

    return dict_snapshot->decodeNonBitEngineColumn(column);
}


bool BitEngineDictionaryManager::checkEncodedPart(
    const MergeTreeData::DataPartPtr & part,
    const MergeTreeData & merge_tree_data,
    std::unordered_map<String, MergeTreeData::DataPartPtr> & res_abnormal_parts,
    [[maybe_unused]] bool without_lock)
{
// TODO (liuhaoqiang)
return false;
}

MergeTreeData::DataPartsVector BitEngineDictionaryManager::checkEncodedParts(
    const MergeTreeData::DataPartsVector & parts, const MergeTreeData & merge_tree_data, ContextPtr query_context, bool without_lock)
{
    // TODO (liuhaoqiang)
    return DB::MergeTreeData::DataPartsVector();
}

Strings BitEngineDictionaryManager::getDictKeysVector()
{
    Strings keys;
    {
        auto lock = getWriteLock();
        transform(dict_containers.begin(), dict_containers.end(), back_inserter(keys), RetrieveKey());
    }
    return keys;
}

String BitEngineDictionaryManager::allDictNamesToString() {
    Strings dicts = getDictKeysVector();
    String res("[");
    for (const auto & str :dicts)
    {
        res += str;
        res += ", ";
    }
    res.resize(res.size()-2);
    res += "]";
    return res;
}

static bool needSyncPart(size_t input_rows, size_t input_bytes, const MergeTreeSettings & settings)
{
    return ((settings.min_rows_to_fsync_after_merge && input_rows >= settings.min_rows_to_fsync_after_merge)
            || (settings.min_compressed_bytes_to_fsync_after_merge && input_bytes >= settings.min_compressed_bytes_to_fsync_after_merge));
}

MergeTreeData::MutableDataPartPtr
BitEngineDictionaryManager::encodePartToTemporaryPart(
    const FutureMergedMutatedPart & future_part,
    const NamesAndTypesList & encode_columns,
    const MergeTreeData & merge_tree_data,
    const ReservationPtr & space_reservation,
    bool can_skip,
    bool part_in_detach,
    bool without_lock)
{
    const auto & source_part = future_part.parts[0];
    auto storage_from_source_part = StorageFromMergeTreeDataPart::create(source_part);
    auto context_for_reading = Context::createCopy(context);
    context_for_reading->setSetting("max_streams_to_max_threads_ratio", 1);
    context_for_reading->setSetting("max_threads", 1);

    size_t skipped_cnt{0};
    for (const auto & column : encode_columns)
    {
        String original_column_name = column.name;
        if (can_skip)
        {
            auto disk = source_part->volume->getDisk();
            if (disk->exists(source_part->getFullRelativePath() + original_column_name + BITENGINE_DATA_FILE_EXTENSION)
                && disk->exists(source_part->getFullRelativePath() + original_column_name + BITENGINE_DATA_MARKS_EXTENSION))
            {
                LOG_DEBUG(log, "BitEngine skips encoding column {} of part {}", original_column_name, source_part->name);
                ++skipped_cnt;
            }
        }
    }

    if (skipped_cnt == encode_columns.size())
        return nullptr;

    LOG_DEBUG(log, "BitEngine encoding part {} to mutation version {}", source_part->name, future_part.part_info.mutation);

    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + future_part.name, space_reservation->getDisk(), 0);

    String path = String("tmp_enc_") + future_part.name;
    if (part_in_detach)
        path.insert(0, String(MergeTreeData::DETACHED_DIR_NAME).append("/"));
    auto new_data_part = merge_tree_data.createPart(
        future_part.name, future_part.type, future_part.part_info, single_disk_volume, path);

    new_data_part->uuid = future_part.uuid;
    new_data_part->is_temp = true;
    new_data_part->ttl_infos = source_part->ttl_infos;
    new_data_part->versions = source_part->versions;

    /// It shouldn't be changed by mutation.
    const StorageMetadataPtr & metadata_snapshot = merge_tree_data.getInMemoryMetadataPtr();
    NamesAndTypesList storage_columns = metadata_snapshot->getColumns().getAllPhysical();
    /// In compact parts we read all columns, because they all stored in a single file

    new_data_part->index_granularity_info = source_part->index_granularity_info;
    new_data_part->setColumns(storage_columns);
    new_data_part->partition.assign(source_part->partition);

    auto disk = new_data_part->volume->getDisk();
    String new_part_tmp_path = new_data_part->getFullRelativePath();

    SyncGuardPtr sync_guard;
    if (merge_tree_data.getSettings()->fsync_part_directory)
        sync_guard = disk->getDirectorySyncGuard(new_part_tmp_path);

    /// Don't change granularity type while mutating subset of columns
    auto mrk_extension = source_part->index_granularity_info.is_adaptive ? getAdaptiveMrkExtension(new_data_part->getType())
                                                                         : getNonAdaptiveMrkExtension();
    bool need_sync = needSyncPart(source_part->rows_count, source_part->getBytesOnDisk(), *merge_tree_data.getSettings());
    bool need_remove_expired_values = false;

    if (!isWidePart(source_part))
    {
        /// TODO (liuhaoqiang) finish this
//        disk->createDirectories(new_part_tmp_path);

        /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
        /// (which is locked in data.getTotalActiveSizeInBytes())
        /// (which is locked in shared mode when input streams are created) and when inserting new data
        /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
        /// deadlock is impossible.
//        auto compression_codec = merge_tree_data.getCompressionCodecForPart(source_part->getBytesOnDisk(), source_part->ttl_infos, time(nullptr));

//        auto part_indices = getIndicesForNewDataPart(metadata_snapshot->getSecondaryIndices(), for_file_renames);
//        auto part_projections = getProjectionsForNewDataPart(metadata_snapshot->getProjections(), for_file_renames);

//        mutateAllPartColumns();
    }
    else
    {
        /// We count total amount of bytes in parts
        /// and use direct_io + aio if there is more than min_merge_bytes_to_use_direct_io
        bool read_with_direct_io = false;
        if (merge_tree_data.getSettings()->min_merge_bytes_to_use_direct_io != 0)
        {
            size_t total_size = source_part->getBytesOnDisk();
            if (total_size >= merge_tree_data.getSettings()->min_merge_bytes_to_use_direct_io)
            {
//                LOG_DEBUG(log, "Will encode part reading files in O_DIRECT");
                read_with_direct_io = true;
            }
        }

        /// calculate which columns can be skipped in encoding
        NameSet files_to_skip = source_part->getFileNamesWithoutChecksums();
        disk->createDirectories(new_part_tmp_path);

        // Create hardlinks for unchanged files
        for (auto it = disk->iterateDirectory(source_part->getFullRelativePath()); it->isValid(); it->next())
        {
            if (files_to_skip.count(it->name()))
                continue;

            String file_name = it->name();
            String destination = new_part_tmp_path + file_name;

            if (!disk->isDirectory(it->path()))
                disk->createHardLink(it->path(), destination);
            else if (!startsWith(it->name(), "tmp_"))  // ignore projection tmp merge dir
            {
                // it's a projection part directory
                disk->createDirectories(destination);
                for (auto p_it = disk->iterateDirectory(it->path()); p_it->isValid(); p_it->next())
                {
                    String p_destination = destination + "/";
                    String p_file_name = p_it->name();
                    p_destination += p_it->name();
                    disk->createHardLink(p_it->path(), p_destination);
                }
            }
        }

        new_data_part->checksums = source_part->checksums;

        auto input_source = std::make_unique<MergeTreeSequentialSource>(
            merge_tree_data, metadata_snapshot, source_part, encode_columns.getNames(), read_with_direct_io, false);

        QueryPipeline pipeline;
        pipeline.init(Pipe(std::move(input_source)));
        pipeline.setMaxThreads(1);
        BlockInputStreamPtr input_stream = std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline));

        IMergedBlockOutputStream::WrittenOffsetColumns unused_written_offsets;
        const auto & index_factory = MergeTreeIndexFactory::instance();
        MergeTreeWriterSettings writer_settings(
            new_data_part->storage.getContext()->getSettings(),
            new_data_part->storage.getSettings(),
            /*can_use_adaptive_granularity = */ source_part->index_granularity_info.is_adaptive,
            /* rewrite_primary_key = */false);
        writer_settings.bitengine_settings = BitEngineEncodeSettings().bitengineOnlyRecode(true).bitengineEncodeWithoutLock(without_lock);

        MergedColumnOnlyOutputStream out_stream(
            new_data_part,
            metadata_snapshot,
            writer_settings,
            input_stream->getHeader(),
            source_part->default_codec,
            index_factory.getMany(metadata_snapshot->getSecondaryIndices()),
            nullptr,
            source_part->index_granularity
        );

        input_stream->readPrefix();
        out_stream.writePrefix();

        while (auto block = input_stream->read())
        {
            out_stream.write(block);
        }

        input_stream->readSuffix();
        // Get the checksums that only contains recoded files.
        auto changed_checksums = out_stream.writeSuffixAndGetChecksums(new_data_part, new_data_part->checksums);
        new_data_part->checksums.add(std::move(changed_checksums));
    }

    finalizeEncodedPart(source_part, new_data_part, false, source_part->default_codec);
    return new_data_part;
}

void BitEngineDictionaryManager::finalizeEncodedPart(
    const MergeTreeDataPartPtr & source_part,
    MergeTreeData::MutableDataPartPtr new_data_part,
    [[maybe_unused]] bool need_remove_expired_values,
    const CompressionCodecPtr & codec)
{
    auto disk = new_data_part->volume->getDisk();

    if (new_data_part->uuid != UUIDHelpers::Nil)
    {
        auto out = disk->writeFile(new_data_part->getFullRelativePath() + IMergeTreeDataPart::UUID_FILE_NAME, 4096);
        HashingWriteBuffer out_hashing(*out);
        writeUUIDText(new_data_part->uuid, out_hashing);
        new_data_part->checksums.files[IMergeTreeDataPart::UUID_FILE_NAME].file_size = out_hashing.count();
        new_data_part->checksums.files[IMergeTreeDataPart::UUID_FILE_NAME].file_hash = out_hashing.getHash();
    }

//    if (need_remove_expired_values)
//    {
//        /// Write a file with ttl infos in json format.
//        LOG_DEBUG(log, "Now write ttl.txt");
//        auto out_ttl = disk->writeFile(fs::path(new_data_part->getFullRelativePath()) / "ttl.txt", 4096);
//        HashingWriteBuffer out_hashing(*out_ttl);
//        new_data_part->ttl_infos.write(out_hashing);
//        new_data_part->checksums.files["ttl.txt"].file_size = out_hashing.count();
//        new_data_part->checksums.files["ttl.txt"].file_hash = out_hashing.getHash();
//    }

    {
        /// Write file with checksums.
//        LOG_DEBUG(log, "Now write checksums.txt");
        auto out_checksums = disk->writeFile(fs::path(new_data_part->getFullRelativePath()) / "checksums.txt", 4096);
        new_data_part->checksums.versions = new_data_part->versions;
        new_data_part->checksums.write(*out_checksums);
    } /// close fd

    {
//        LOG_DEBUG(log, "Now write codec file name");
        auto out = disk->writeFile(new_data_part->getFullRelativePath() + IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME, 4096);
        DB::writeText(queryToString(codec->getFullCodecDesc()), *out);
    }

    {
        /// Write a file with a description of columns.
//        LOG_DEBUG(log, "Now write columns.txt");
        auto out_columns = disk->writeFile(fs::path(new_data_part->getFullRelativePath()) / "columns.txt", 4096);
        new_data_part->getColumns().writeText(*out_columns);
    } /// close fd

    new_data_part->rows_count = source_part->rows_count;
    new_data_part->index_granularity = source_part->index_granularity;
    new_data_part->index = source_part->index;
    new_data_part->minmax_idx = source_part->minmax_idx;
    new_data_part->modification_time = time(nullptr);
//    new_data_part->loadProjections(false, false);
    new_data_part->setBytesOnDisk(
        MergeTreeData::DataPart::calculateTotalSizeOnDisk(new_data_part->volume->getDisk(), new_data_part->getFullRelativePath()));
    new_data_part->default_codec = codec;
    new_data_part->calculateColumnsSizesOnDisk();
    new_data_part->storage.lockSharedData(*new_data_part);
}







/////////////   StartOf BitEngineDictionaryHaManager
BitEngineDictionaryHaManager::BitEngineDictionaryHaManager(StorageHaMergeTree & storage_, BitEngineDictionaryManager * bitengine_manager_, const String & zookeeper_path_
                                                           ,const String & replica_name_)
    : storage(storage_), bitengine_manager(bitengine_manager_), zookeeper_path(zookeeper_path_), replica_name(replica_name_)
    , log(&Poco::Logger::get("BitEngineDictioanryHaManager (" + getDatabaseAndTable() + ")" ))
{
    replica_path = zookeeper_path + "/replicas/" + replica_name;
    version_path = replica_path + "/bitengine_version";
    lock_path = zookeeper_path + "/bitengine_lock";

    bitengine_dict_exchanger = std::make_shared<BitEngineDataExchanger>(*this);

    InterserverIOEndpointPtr bitengine_dict_endpoint_ptr = std::make_shared<BitEngineDataService>(*this);
    [[maybe_unused]] auto prev_ptr = std::atomic_exchange(&bitengine_dict_endpoint, bitengine_dict_endpoint_ptr);
    assert(prev_ptr == nullptr);
    storage.getContext()->getInterserverIOHandler().addEndpoint(bitengine_dict_endpoint_ptr->getId(getReplicaPath()),
                                                                bitengine_dict_endpoint_ptr);
    prepareZookeeper();

    if (bitengine_manager)
    {
        version = bitengine_manager->getVersion();
        size_t version_in_zk = version;
        try
        {
            version_in_zk = getVersionOnZooKeeper();
        } catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            LOG_ERROR(log, "Cannot get version from zookeeper when initialize bitengine ha manager");
        }

        // For the case the bitengine manger failed to update version, we choose to trust version in zookeeper.
        if (version < version_in_zk)
        {
            version = version_in_zk;
            bitengine_manager->updateVersionTo(version);
        }
        else if (version > version_in_zk)
            updateVersionOnZookeeper();
    }
}

String BitEngineDictionaryHaManager::getDatabaseAndTable()
{
    return storage.getStorageID().getFullNameNotQuoted();
}

void BitEngineDictionaryHaManager::stop()
{
    stopped = true;
    if (event)
        event->set();

    auto bitengine_dict_endpoint_ptr = std::atomic_exchange(&bitengine_dict_endpoint, InterserverIOEndpointPtr{});
    if (bitengine_dict_endpoint_ptr)
    {
        storage.getContext()->getInterserverIOHandler().removeEndpointIfExists((bitengine_dict_endpoint_ptr->getId(getReplicaPath())));
        // Ask all bitengine dictionary data exchange handlers to finish asap. New ones will fail to start
        bitengine_dict_endpoint_ptr->blocker.cancelForever();
        // Wait for all of them
        std::unique_lock lock(bitengine_dict_endpoint_ptr->rwlock);
    }
}

BitEngineDictionaryHaManager::~BitEngineDictionaryHaManager()
{
    try{
        if (!stopped)
            stop();
        if (event)
            event->set();
    }catch(...){
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}


void BitEngineDictionaryHaManager::prepareZookeeper()
{
    auto zookeeper = getZooKeeper();
    zookeeper->tryCreate(version_path, "", zkutil::CreateMode::Persistent);
    zookeeper->tryCreate(lock_path, "", zkutil::CreateMode::Persistent);
}

zkutil::ZooKeeperPtr BitEngineDictionaryHaManager::getZooKeeper()
{
    current_zookeeper = storage.getZooKeeper();
    return current_zookeeper;
}

size_t BitEngineDictionaryHaManager::getVersionOnZooKeeper()
{
    auto zookeeper = getZooKeeper();
    String version_in_zk;
    zookeeper->tryGet(version_path, version_in_zk);

    if (version_in_zk.empty())
    {
        zookeeper->createOrUpdate(version_path, std::to_string(version), zkutil::CreateMode::Persistent);
        return version;
    }

    return std::stoll(version_in_zk);
}

std::tuple<size_t, String> BitEngineDictionaryHaManager::getMaxVersionAndReplica()
{
    auto zookeeper = getZooKeeper();
    size_t max_version = 0;
    String res_replica;
    Strings replicas = getReplicas();
    for (const auto & replica : replicas)
    {
        if (replica == replica_name)
            continue;

        size_t version_tmp = getVersionOfReplica(replica, zookeeper);
        if (version_tmp > max_version)
        {
            max_version = version_tmp;
            res_replica = replica;
        }
    }
    return {max_version, res_replica};
}

size_t BitEngineDictionaryHaManager::getMinVersion()
{
    auto zookeeper = getZooKeeper();
    size_t min_version = version;
    Strings replicas = getActiveReplicas();
    for (const auto & replica : replicas)
    {
        if (replica == replica_name)
            continue;

        size_t version_tmp = getVersionOfReplica(replica, zookeeper);
        if (version_tmp < min_version)
            min_version = version_tmp;
    }
    return min_version;
}

void BitEngineDictionaryHaManager::setVersionOnZookeeper()
{
    if (!is_valid)
        return;
    auto zookeeper = getZooKeeper();
    zookeeper->createOrUpdate(version_path, std::to_string(version), zkutil::CreateMode::Persistent);
}

size_t BitEngineDictionaryHaManager::getVersionOfReplica(const String & replica, zkutil::ZooKeeperPtr & zookeeper)
{
    String version_in_zk;
    String version_path_of_replica = zookeeper_path + "/replicas/" + replica + "/bitengine_version";
    zookeeper->tryGet(version_path_of_replica, version_in_zk);

    if (version_in_zk.empty())
        return 0;

    return std::stoll(version_in_zk);
}

void BitEngineDictionaryHaManager::updateVersion()
{
    if (!bitengine_manager)
        return;

    size_t version_local = bitengine_manager->getVersion();

    if (version > version_local)
        throw Exception("Cannot update version since ha version is larger than local version, " + std::to_string(version) + " > " + std::to_string(version_local), ErrorCodes::LOGICAL_ERROR);

    if (version == version_local)
        return;

    version = version_local;

    size_t version_in_zk = getVersionOnZooKeeper();
    if (version > version_in_zk && is_valid)
    {
        String version_string = std::to_string(version);
        LOG_TRACE(log, "Will update version in zookeeper to {}", version_string);
        current_zookeeper->set(version_path, version_string);
    }
}

void BitEngineDictionaryHaManager::updateVersionOnZookeeper()
{
    String version_string = std::to_string(version);
    current_zookeeper->set(version_path, version_string);
}


HaMergeTreeAddress BitEngineDictionaryHaManager::getReplicaAddress(const String & replica_name_)
{
    String replica_host_path = zookeeper_path + "/replicas/" + replica_name_ + "/host";
    return HaMergeTreeAddress(getZooKeeper()->get(replica_host_path));
}

Strings BitEngineDictionaryHaManager::getReplicas()
{
    auto zookeeper = getZooKeeper();
    return zookeeper->getChildren(zookeeper_path + "/replicas");
}

Strings BitEngineDictionaryHaManager::getActiveReplicas()
{
    auto zookeeper = getZooKeeper();
    Strings candidates = zookeeper->getChildren(zookeeper_path + "/replicas");
    Strings replicas;
    for (const auto & replica : candidates)
    {
        if (replica == replica_name)
            continue;
        String active_path = zookeeper_path + "/replicas/" + replica + "/is_active";
        if (!zookeeper->exists(active_path))
            continue;
        replicas.push_back(replica);
    }
    return replicas;
}

bool BitEngineDictionaryHaManager::isActiveReplica(const String & replica, zkutil::ZooKeeperPtr & zookeeper)
{
    return zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/is_active");
}

bool BitEngineDictionaryHaManager::isActiveReplica(const String & replica)
{
    auto zookeeper = getZooKeeper();
    return zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/is_active");
}

void BitEngineDictionaryHaManager::tryUpdateVersionAndDict()
{
    if (!bitengine_manager)
        return;

    size_t version_local = bitengine_manager->getVersion();

    if (version > version_local)
        throw Exception("Cannot update version since ha version is larger than local version, " + std::to_string(version) + " > " + std::to_string(version_local), ErrorCodes::LOGICAL_ERROR);

    version = version_local;

    size_t version_in_zk = getVersionOnZooKeeper();
    if (version > version_in_zk)
    {
        String version_string = std::to_string(version);
        current_zookeeper->set(version_path, version_string);
    }
    else
    {
        tryUpdateDict();
    }
}

void BitEngineDictionaryHaManager::tryUpdateDictFromReplica(const String & replica)
{
    if (!isValid() || replica.empty() || replica == replica_name)
        return;


    // only one can update dict at a time, to avoid race condition
    auto lock = std::lock_guard<std::mutex>(ha_mutex);

    auto zookeeper = getZooKeeper();
    size_t version_in_zk = getVersionOfReplica(replica, zookeeper);

    LOG_TRACE(log, "Try to get lock before updating dict from replica {} of version {} (local version: {})",
              replica, version_in_zk, version);
//    LOG_TRACE(log, "$$$$$$ version in zk $$$$$$: {}, version in local: {}", version_in_zk, version);

    if (version < version_in_zk && bitengine_dict_exchanger)
    {
        LOG_DEBUG(log, "Will fetch bitengine dictionary from {} with version {}", replica, version_in_zk);
        bitengine_dict_exchanger->fetchIncrementData(replica);
        version = version_in_zk;
        bitengine_manager->updateVersionTo(version);
        bitengine_manager->updated();
        setVersionOnZookeeper();
        LOG_DEBUG(log, "Updated bitengine dictionary to version {}", std::to_string(version));
    }
}

void BitEngineDictionaryHaManager::tryUpdateDictFromReplicaPath(const String & src_replica_path)
{
    if (!isValid() || src_replica_path.empty() || src_replica_path == replica_path)
        return;
    Strings replicas = getReplicas();

    String src_replica;

    for (const auto & replica : replicas)
    {
        if (replica == replica_name)
            continue;

        String path_of_replica = zookeeper_path + "/replicas/" + replica;
        if (path_of_replica == src_replica_path)
        {
            src_replica = replica;
            break;
        }
    }

    //LOG_TRACE(log, "Get src replica from replica_path " << src_replica);

    try
    {
        tryUpdateDictFromReplica(src_replica);
    }catch(...)
    {
        //tryLogCurrentException(log, __PRETTY_FUNCTION__);
        LOG_DEBUG(log, "Cannot update bitengine dict from replica {}", src_replica);
        throw;
    }
}


void BitEngineDictionaryHaManager::tryUpdateDict()
{
    LOG_TRACE(log, "Try to get lock before updating dict");
    // only one can update dict at a time, to avoid race condition
    auto lock = std::lock_guard<std::mutex>(ha_mutex);

    size_t max_version;
    String replica;
    tie(max_version, replica) = getMaxVersionAndReplica();
    LOG_DEBUG(log, "Max version in zk is: {}, current version is {}", std::to_string(max_version), std::to_string(version));
    if (max_version <= version || replica.empty())
        return;

    // Too dangerous.
    // There exists the case the node with larger version is down, and the others cannot update its dicts.
    // So it cannot decode data anymore.
    // However, it can insert data with a invalid dictionary. This is reasonable since we have store the original
    // data, not just encoded data. It left a chance to recode this data in the future, but before that time,
    // these data will not be decode.
    // It is acceptable in most case.
    // TODO: backup dictionary in third party storage like hdfs.
    if (!isActiveReplica(replica))
    {
        //LOG_ERROR(log, "Cannot update bitengine dictionary from replica " + replica + " with version " + std::to_string(max_version)
        //          + ", the current version is " + std::to_string(version));
        throw Exception("Cannot update bitengine dictionary from replica " + replica + " with version " + std::to_string(max_version)
                            + ", the current version is " + std::to_string(version), ErrorCodes::LOGICAL_ERROR);
        //setInvalid();
        //return;
    }

    if (bitengine_dict_exchanger)
    {
        bitengine_dict_exchanger->fetchIncrementData(replica);
        bitengine_manager->updateVersionTo(max_version);
        version = bitengine_manager->getVersion();
        bitengine_manager->updated();
        setVersionOnZookeeper();
        LOG_DEBUG(log, "Updated bitengine dict to version {}", std::to_string(version));
    }
}

void BitEngineDictionaryHaManager::resetDictImpl()
{
    version = 0;
    setVersionOnZookeeper();
    is_valid = false;
    stopped = true;
    bitengine_dict_exchanger = nullptr;
    auto bitengine_dict_endpoint_ptr = std::atomic_exchange(&bitengine_dict_endpoint, InterserverIOEndpointPtr{});
    if (bitengine_dict_endpoint_ptr)
    {
        storage.getContext()->getInterserverIOHandler().removeEndpointIfExists((bitengine_dict_endpoint_ptr->getId(getReplicaPath())));
        // Ask all bitengine dictionary data exchange handlers to finish asap. New ones will fail to start
        bitengine_dict_endpoint->blocker.cancelForever();
        // Wait for all of them
        std::unique_lock lock(bitengine_dict_endpoint_ptr->rwlock);
    }

    bitengine_manager->resetDict();
}

void BitEngineDictionaryHaManager::setInvalid()
{
    if (!is_valid)
        return;

    if (bitengine_manager)
        bitengine_manager->setInvalid();
    is_valid = false;
}

void BitEngineDictionaryHaManager::setValid()
{
    if (is_valid)
        return;

    if (bitengine_manager)
        bitengine_manager->setValid();

    is_valid = true;
}

void BitEngineDictionaryHaManager::flush()
{
    if (bitengine_manager)
        bitengine_manager->flushDict();
}

void BitEngineDictionaryHaManager::readData(ReadBuffer & in)
{
    if (bitengine_manager)
    {
        try
        {
            bitengine_manager->readDataFromReadBuffer(in);
        }catch(...){
            LOG_DEBUG(log, "Cannot read data in BitEngineDiciotnaryHaManager, the dictionary may be corrupt");
            setInvalid();
            throw;
        }
        setValid();
    }
}

void BitEngineDictionaryHaManager::writeData(WriteBuffer & out)
{
    if (bitengine_manager)
    {
        try
        {
            bitengine_manager->writeDataToWriteBuffer(out);
        }catch(...){
            LOG_DEBUG(log, "Cannot write data in BitEngineDiciotnaryHaManager, the dictionary may be corrupt");
            throw;
        }
    }
}

void BitEngineDictionaryHaManager::readIncrementData(ReadBuffer & in)
{
    if (bitengine_manager)
    {
        try
        {
            IncrementData increment_data;
            increment_data.readIncrementData(in);
            bitengine_manager->insertIncrementData(increment_data);
        }catch(...){
            LOG_DEBUG(log, "Cannot read increment data in BitEngineDiciotnaryHaManager, the dictionary may be corrupt");
            // If the increment data corrupts the dictionary, the dictionary will set itself as invalid so that we should not
            // set invalid here. For most case, the dictionary has not been corrupted, it is just a network disconnect and we have
            // not write it into dictionary. Thus, the current dictionary can still works.
            //setInvalid();
            throw;
        }
        setValid();
    }
}

void BitEngineDictionaryHaManager::writeIncrementData(WriteBuffer & out, const IncrementOffset & increment_offset)
{
    if (bitengine_manager)
    {
        try
        {
            IncrementData increment_data = bitengine_manager->getIncrementData(increment_offset);
            increment_data.writeIncrementData(out);
        }catch(...){
            LOG_DEBUG(log, "Cannot write increment data in BitEngineDiciotnaryHaManager, the dictionary may be corrupt");
            throw;
        }
    }
}

IncrementOffset BitEngineDictionaryHaManager::readIncrementOffset(ReadBuffer & in)
{
    IncrementOffset increment_offset;
    if (bitengine_manager)
    {
        try
        {
            size_t received_version;
            readVarUInt(received_version, in);
            if (received_version > version)
                throw Exception("Read a increment offset which has a higher version "
                                    + std::to_string(received_version) + ", the current version is "
                                    + std::to_string(version), ErrorCodes::LOGICAL_ERROR);

            increment_offset.readIncrementOffset(in);
            return increment_offset;
        }catch(...){
            LOG_DEBUG(log, "Cannot read increment offset in BitEngineDiciotnaryHaManager");
            //setInvalid();
            throw;
        }
    }
    return increment_offset;
}

void BitEngineDictionaryHaManager::writeIncrementOffset(WriteBuffer & out)
{
    if (bitengine_manager)
    {
        try
        {
            writeVarUInt(version, out);

            IncrementOffset increment_offset = bitengine_manager->getIncrementOffset();
            increment_offset.writeIncrementOffset(out);
        }catch(...){
            LOG_DEBUG(log, "Cannot write incrementOffset in BitEngineDiciotnaryHaManager, the dictionary may be corrupt");
            throw;
        }
    }
}

BitEngineDictionaryHaManager::BitEngineLockPtr BitEngineDictionaryHaManager::tryGetLock()
{
    /// Only one thread can try to get lock in one manager,
    /// in case of multiple threads register mutiple same watcher on one zookeeper node,
    /// but only one client can be waked up.
    auto lock_lock = std::lock_guard<std::mutex>(lock_mutex);

    auto zookeeper = getZooKeeper();
    String lock_prefix = lock_path + "/lock-";
    String current_lock_path = zookeeper->create(lock_prefix, "", zkutil::CreateMode::EphemeralSequential);
    String current_lock = current_lock_path.substr(lock_path.length() + 1);


    // get locks before the current lock
    Strings locks = zookeeper->getChildren(lock_path);
    Strings before_locks;

    std::sort(locks.begin(), locks.end());

    for (const auto & lock : locks)
    {
        if (lock == current_lock)
            break;
        before_locks.push_back(lock);
    }

    LOG_TRACE(log, "the current lock is: {}", current_lock);

    if (before_locks.empty())
        return std::make_shared<BitEngineLock>(*this, current_lock_path);

    for (const auto & lock : before_locks)
    {
        if (!zookeeper->exists(lock_path + "/" + lock, nullptr, event))
            continue;

        LOG_TRACE(log, "Try wait for lock {}", lock);
        if (stopped)
            break;
        event->wait();
    }

    LOG_DEBUG(log, "GETTED LOCK FROM ZOOKEEPER!");

    return std::make_shared<BitEngineLock>(*this, current_lock_path);
}

bool BitEngineDictionaryHaManager::recodeBitEnginePart(
    const FutureMergedMutatedPart & part,
    ContextPtr query_context,
    bool can_skip,
    bool part_in_detach)
{
    bool without_lock = query_context->getSettingsRef().bitengine_encode_without_lock;
    BitEngineDictionaryHaManager::BitEngineLockPtr bitengine_lock;
    if (!without_lock)
        bitengine_lock = tryGetLock();

    tryUpdateDict();
    // double check the status of bitengine manager if the storage is shutdown when it was waitting for the lock
    if (isStopped())
        return false;

    try
    {
       if (bitengine_manager)
           bitengine_manager->recodeBitEnginePart(part, storage, query_context, can_skip, part_in_detach);
    }catch(...){
        // updateVersion(); // version in zk is updated when releasing the lock
        throw;
    }

    // updateVersion();  // version in zk is updated when releasing the lock

    return true;
}

bool BitEngineDictionaryHaManager::recodeBitEngineParts(
    const std::vector<FutureMergedMutatedPart> & future_parts,
    ContextPtr query_context,
    bool can_skip,
    bool part_in_detach)
{
    bool without_lock = query_context->getSettingsRef().bitengine_encode_without_lock;
    BitEngineDictionaryHaManager::BitEngineLockPtr bitengine_lock;
    if (!without_lock)
        bitengine_lock = tryGetLock();

    LOG_DEBUG(log, "Now encode {} BitEngine parts in one thread with {} zk lock", future_parts.size(), (without_lock ? "no" : "a"));
    tryUpdateDict();
    // double check the status of bitengine manager if the storage is shutdown when it was waitting for the lock
    if (isStopped())
        return false;

    if (!bitengine_manager)
        return false;

    try
    {
        for (const auto & part : future_parts)
        {
            bitengine_manager->recodeBitEnginePart(part, storage, query_context, can_skip, part_in_detach);
            // updateVersion(); // version in zk is updated when releasing the lock
        }
    }catch(...){
        // updateVersion();  // version in zk is updated when releasing the lock
        throw;
    }

    // only flush at the end of bitengine recode
    bitengine_manager->flushDict();

    return true;
}

bool BitEngineDictionaryHaManager::recodeBitEnginePartsParallel(
    const std::vector<FutureMergedMutatedPart> & parts,
    ContextPtr query_context,
    bool can_skip,
    bool part_in_detach)
{
    bool without_lock = query_context->getSettingsRef().bitengine_encode_without_lock;
    BitEngineDictionaryHaManager::BitEngineLockPtr bitengine_lock;
    if (!without_lock)
        bitengine_lock = tryGetLock();

    tryUpdateDict();
    // double check the status of bitengine manager if the storage is shutdown when it was waitting for the lock
    if (isStopped())
        return false;

    if (!bitengine_manager)
        return false;

    std::mutex recode_mutex;

    if (parts.empty())
        return true;

    auto data_parts = parts;

    ThreadGroupStatusPtr thread_group = CurrentThread::getGroup();

    auto runRecodeBitEnginePart = [&]()
    {
        setThreadName("ParaEncBtEngPt");
        CurrentThread::attachToIfDetached(thread_group);
        while (true)
        {
            FutureMergedMutatedPart * part{nullptr};
            {
                std::lock_guard<std::mutex> lock(recode_mutex);
                if (!data_parts.empty())
                {
                    part = &(data_parts.back());
                    data_parts.pop_back();
                }
                else
                {
                    break;
                }
            }

            if (!part)
                return;

            try{
                bitengine_manager->recodeBitEnginePart(*part, storage, query_context, can_skip, part_in_detach);
                // updateVersion();  // version in zk is updated when releasing the lock
            }
            catch(...){
                // updateVersion();  // version in zk is updated when releasing the lock
                throw;
            }
        }
    };

    size_t max_threads = query_context->getSettingsRef().max_parallel_threads_for_bitengine_recode;
    size_t num_threads = std::min(max_threads, data_parts.size());
    std::unique_ptr<ThreadPool> thread_pool = std::make_unique<ThreadPool>(num_threads);
    LOG_DEBUG(log, "BitEngine will encode {} parts In Parallel in {} threads with {} zk lock",
              parts.size(), num_threads, (bitengine_lock ? "a" : "no"));

    for (size_t i = 0; i<num_threads; i++)
    {
        thread_pool->scheduleOrThrowOnError(runRecodeBitEnginePart);
    }

    thread_pool->wait();

    //only flush at the end of bitengine recode
    bitengine_manager->flushDict();

    return true;
}



void BitEngineDictionaryHaManager::checkBitEnginePart(const MergeTreeData::DataPartPtr & part)
{
    if (bitengine_manager)
        bitengine_manager->checkBitEnginePart(part);
}

MergeTreeData::DataPartsVector
BitEngineDictionaryHaManager::checkEncodedParts(const MergeTreeData::DataPartsVector & parts,
                                                ContextPtr query_context,
                                                [[maybe_unused]]bool without_lock)
{
    if (parts.empty())
        return {};
    if (!bitengine_manager)
        throw Exception("Failed to get dict_manager", ErrorCodes::LOGICAL_ERROR);

    std::mutex parts_mutex;
    MergeTreeData::DataPartsVector data_parts = parts;

    std::unordered_map<String, MergeTreeData::DataPartPtr> abnormal_parts_maps;
    for (const auto & part: parts)
    {
        abnormal_parts_maps.emplace(part->name, nullptr);
    }

    ThreadGroupStatusPtr thread_group = CurrentThread::getGroup();

    auto runCheckEncodedPart = [&]()
    {
        setThreadName("checkEncodedPart");
        CurrentThread::attachToIfDetached(thread_group);

        while (true)
        {
            MergeTreeData::DataPartPtr part;
            {
                std::lock_guard<std::mutex> lock(parts_mutex);
                if (!data_parts.empty())
                {
                    part = data_parts.back();
                    data_parts.pop_back();
                }
                else
                    break;
            }

            if (!part)
                return;

            try{
                bitengine_manager->checkEncodedPart(part, storage, abnormal_parts_maps, true);
            }
            catch(...){
                throw;
            }
        }
    };

    size_t max_threads = query_context->getSettingsRef().max_parallel_threads_for_bitengine_recode;
    size_t num_threads = std::min(max_threads, data_parts.size());

    std::unique_ptr<ThreadPool> thread_pool = std::make_unique<ThreadPool>(num_threads);
    for (size_t i = 0; i < num_threads; i++)
    {
        thread_pool->scheduleOrThrowOnError(runCheckEncodedPart);
    }

    thread_pool->wait();

    MergeTreeData::DataPartsVector abnormal_parts;
    for (auto & part : abnormal_parts_maps)
    {
        if (part.second)
            abnormal_parts.emplace_back(part.second);
    }

    return abnormal_parts;
}

BitEngineDictionaryHaManager::BitEngineLock::BitEngineLock(BitEngineDictionaryHaManager & ha_manager_, const String & lock_path_)
    : ha_manager(ha_manager_), lock_path(lock_path_)
{
    LOG_TRACE(&Poco::Logger::get("BitEngineLock"), "Create a lock [ {} ]", lock_path);
}

BitEngineDictionaryHaManager::BitEngineLock::~BitEngineLock()
{
    try{
        auto zookeeper = ha_manager.getZooKeeper();
        // update version in zk if needed
        ha_manager.updateVersion();
        // remove the lock in zk
        zookeeper->tryRemove(lock_path);
        LOG_TRACE(&Poco::Logger::get("BitEngineLock"), "Removed the lock [ {} ]", lock_path);
    }catch(...)
    {
        LOG_DEBUG(&Poco::Logger::get("BitEngineLock"), "Fail to remove the lock [ {} ]", lock_path);
        tryLogCurrentException(&Poco::Logger::get("BitEngineLock"), __PRETTY_FUNCTION__);
        // do nothing
    }
}

}
