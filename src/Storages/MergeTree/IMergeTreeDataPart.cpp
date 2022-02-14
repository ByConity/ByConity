#include "IMergeTreeDataPart.h"

#include <optional>
#include <Core/Defines.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/localBackup.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/ChecksumsCache.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/CurrentMetrics.h>
#include <common/JSON.h>
#include <common/logger_useful.h>
#include <Compression/getCompressionCodecForFile.h>
#include <Parsers/queryToString.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeByteMap.h>
#include <DataTypes/MapHelpers.h>

#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/DiskUniqueIndexFileCache.h>
#include <Storages/MergeTree/DeleteBitmapCache.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <Processors/Executors/PipelineExecutingBlockInputStream.h>
#include <Poco/DirectoryIterator.h>

namespace CurrentMetrics
{
    extern const Metric PartsTemporary;
    extern const Metric PartsPreCommitted;
    extern const Metric PartsCommitted;
    extern const Metric PartsOutdated;
    extern const Metric PartsDeleting;
    extern const Metric PartsDeleteOnDestroy;

    extern const Metric PartsWide;
    extern const Metric PartsCompact;
    extern const Metric PartsInMemory;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
    extern const int FILE_DOESNT_EXIST;
    extern const int NO_FILE_IN_DATA_PART;
    extern const int EXPECTED_END_OF_FILE;
    extern const int CORRUPTED_DATA;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int BAD_TTL_FILE;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_FORMAT_VERSION;
}

static std::unique_ptr<ReadBufferFromFileBase> openForReading(const DiskPtr & disk, const String & path)
{
    return disk->readFile(path, std::min(size_t(DBMS_DEFAULT_BUFFER_SIZE), disk->getFileSize(path)));
}

void IMergeTreeDataPart::MinMaxIndex::load(const MergeTreeData & data, const DiskPtr & disk_, const String & part_path)
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    const auto & partition_key = metadata_snapshot->getPartitionKey();

    auto minmax_column_names = data.getMinMaxColumnsNames(partition_key);
    auto minmax_column_types = data.getMinMaxColumnsTypes(partition_key);
    size_t minmax_idx_size = minmax_column_types.size();
    hyperrectangle.reserve(minmax_idx_size);
    for (size_t i = 0; i < minmax_idx_size; ++i)
    {
        String file_name = fs::path(part_path) / ("minmax_" + escapeForFileName(minmax_column_names[i]) + ".idx");
        auto file = openForReading(disk_, file_name);
        auto serialization = minmax_column_types[i]->getDefaultSerialization();

        Field min_val;
        serialization->deserializeBinary(min_val, *file);
        Field max_val;
        serialization->deserializeBinary(max_val, *file);

        hyperrectangle.emplace_back(min_val, true, max_val, true);
    }
    initialized = true;
}

void IMergeTreeDataPart::MinMaxIndex::load(const MergeTreeData & data, ReadBuffer & buf)
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    const auto & partition_key = metadata_snapshot->getPartitionKey();

    auto minmax_column_names = data.getMinMaxColumnsNames(partition_key);
    auto minmax_column_types = data.getMinMaxColumnsTypes(partition_key);
    size_t minmax_idx_size = minmax_column_types.size();
    hyperrectangle.reserve(minmax_idx_size);

    for (size_t i = 0; i < minmax_idx_size; ++i)
    {
        auto serialization = minmax_column_types[i]->getDefaultSerialization();
        Field min_val;
        serialization->deserializeBinary(min_val, buf);
        Field max_val;
        serialization->deserializeBinary(max_val, buf);

        hyperrectangle.emplace_back(min_val, true, max_val, true);
    }
    initialized = true;
}

void IMergeTreeDataPart::MinMaxIndex::store(
    const MergeTreeData & data, const DiskPtr & disk_, const String & part_path, Checksums & out_checksums) const
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    const auto & partition_key = metadata_snapshot->getPartitionKey();

    auto minmax_column_names = data.getMinMaxColumnsNames(partition_key);
    auto minmax_column_types = data.getMinMaxColumnsTypes(partition_key);

    store(minmax_column_names, minmax_column_types, disk_, part_path, out_checksums);
}

void IMergeTreeDataPart::MinMaxIndex::store(
    const Names & column_names,
    const DataTypes & data_types,
    const DiskPtr & disk_,
    const String & part_path,
    Checksums & out_checksums) const
{
    if (!initialized)
        throw Exception("Attempt to store uninitialized MinMax index for part " + part_path + ". This is a bug.",
            ErrorCodes::LOGICAL_ERROR);

    for (size_t i = 0; i < column_names.size(); ++i)
    {
        String file_name = "minmax_" + escapeForFileName(column_names[i]) + ".idx";
        auto serialization = data_types.at(i)->getDefaultSerialization();

        auto out = disk_->writeFile(fs::path(part_path) / file_name);
        HashingWriteBuffer out_hashing(*out);
        serialization->serializeBinary(hyperrectangle[i].left, out_hashing);
        serialization->serializeBinary(hyperrectangle[i].right, out_hashing);
        out_hashing.next();
        out_checksums.files[file_name].file_size = out_hashing.count();
        out_checksums.files[file_name].file_hash = out_hashing.getHash();
        out->finalize();
    }
}

void IMergeTreeDataPart::MinMaxIndex::store(const MergeTreeData & data, const String & part_path, WriteBuffer & buf) const
{
    if (!initialized)
        throw Exception("Attempt to store uninitialized MinMax index for part " + part_path + ". This is a bug.",
            ErrorCodes::LOGICAL_ERROR);
    
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    const auto & partition_key = metadata_snapshot->getPartitionKey();

    auto minmax_column_names = data.getMinMaxColumnsNames(partition_key);
    auto minmax_column_types = data.getMinMaxColumnsTypes(partition_key);

    for (size_t i = 0; i < minmax_column_names.size(); ++i)
    {
        auto serialization = minmax_column_types.at(i)->getDefaultSerialization();

        serialization->serializeBinary(hyperrectangle[i].left, buf);
        serialization->serializeBinary(hyperrectangle[i].right, buf);
    }
}

void IMergeTreeDataPart::MinMaxIndex::update(const Block & block, const Names & column_names)
{
    if (!initialized)
        hyperrectangle.reserve(column_names.size());

    for (size_t i = 0; i < column_names.size(); ++i)
    {
        FieldRef min_value;
        FieldRef max_value;
        const ColumnWithTypeAndName & column = block.getByName(column_names[i]);
        column.column->getExtremes(min_value, max_value);

        if (!initialized)
            hyperrectangle.emplace_back(min_value, true, max_value, true);
        else
        {
            hyperrectangle[i].left = std::min(hyperrectangle[i].left, min_value);
            hyperrectangle[i].right = std::max(hyperrectangle[i].right, max_value);
        }
    }

    initialized = true;
}

void IMergeTreeDataPart::MinMaxIndex::merge(const MinMaxIndex & other)
{
    if (!other.initialized)
        return;

    if (!initialized)
    {
        hyperrectangle = other.hyperrectangle;
        initialized = true;
    }
    else
    {
        for (size_t i = 0; i < hyperrectangle.size(); ++i)
        {
            hyperrectangle[i].left = std::min(hyperrectangle[i].left, other.hyperrectangle[i].left);
            hyperrectangle[i].right = std::max(hyperrectangle[i].right, other.hyperrectangle[i].right);
        }
    }
}

void IMergeTreeDataPart::loadVersions()
{
    String path = fs::path(getFullRelativePath()) / "versions.txt";
    try {
        if (volume->getDisk()->exists(path))
        {
            auto file = openForReading(volume->getDisk(), path);
            if (versions->read(*file))
            {
                assertEOF(*file);
            }
        }
        else
        {
            versions->enable_compact_map_data = false;
        }
    }
    catch(...)
    {
        LOG_DEBUG(storage.log, "load versions fail in part " + name);

        throw;
    }
}

static void incrementStateMetric(IMergeTreeDataPart::State state)
{
    switch (state)
    {
        case IMergeTreeDataPart::State::Temporary:
            CurrentMetrics::add(CurrentMetrics::PartsTemporary);
            return;
        case IMergeTreeDataPart::State::PreCommitted:
            CurrentMetrics::add(CurrentMetrics::PartsPreCommitted);
            return;
        case IMergeTreeDataPart::State::Committed:
            CurrentMetrics::add(CurrentMetrics::PartsCommitted);
            return;
        case IMergeTreeDataPart::State::Outdated:
            CurrentMetrics::add(CurrentMetrics::PartsOutdated);
            return;
        case IMergeTreeDataPart::State::Deleting:
            CurrentMetrics::add(CurrentMetrics::PartsDeleting);
            return;
        case IMergeTreeDataPart::State::DeleteOnDestroy:
            CurrentMetrics::add(CurrentMetrics::PartsDeleteOnDestroy);
            return;
    }
}

static void decrementStateMetric(IMergeTreeDataPart::State state)
{
    switch (state)
    {
        case IMergeTreeDataPart::State::Temporary:
            CurrentMetrics::sub(CurrentMetrics::PartsTemporary);
            return;
        case IMergeTreeDataPart::State::PreCommitted:
            CurrentMetrics::sub(CurrentMetrics::PartsPreCommitted);
            return;
        case IMergeTreeDataPart::State::Committed:
            CurrentMetrics::sub(CurrentMetrics::PartsCommitted);
            return;
        case IMergeTreeDataPart::State::Outdated:
            CurrentMetrics::sub(CurrentMetrics::PartsOutdated);
            return;
        case IMergeTreeDataPart::State::Deleting:
            CurrentMetrics::sub(CurrentMetrics::PartsDeleting);
            return;
        case IMergeTreeDataPart::State::DeleteOnDestroy:
            CurrentMetrics::sub(CurrentMetrics::PartsDeleteOnDestroy);
            return;
    }
}

static void incrementTypeMetric(MergeTreeDataPartType type)
{
    switch (type.getValue())
    {
        case MergeTreeDataPartType::WIDE:
            CurrentMetrics::add(CurrentMetrics::PartsWide);
            return;
        case MergeTreeDataPartType::COMPACT:
            CurrentMetrics::add(CurrentMetrics::PartsCompact);
            return;
        case MergeTreeDataPartType::IN_MEMORY:
            CurrentMetrics::add(CurrentMetrics::PartsInMemory);
            return;
        case MergeTreeDataPartType::UNKNOWN:
            return;
    }
}

static void decrementTypeMetric(MergeTreeDataPartType type)
{
    switch (type.getValue())
    {
        case MergeTreeDataPartType::WIDE:
            CurrentMetrics::sub(CurrentMetrics::PartsWide);
            return;
        case MergeTreeDataPartType::COMPACT:
            CurrentMetrics::sub(CurrentMetrics::PartsCompact);
            return;
        case MergeTreeDataPartType::IN_MEMORY:
            CurrentMetrics::sub(CurrentMetrics::PartsInMemory);
            return;
        case MergeTreeDataPartType::UNKNOWN:
            return;
    }
}


IMergeTreeDataPart::IMergeTreeDataPart(
    MergeTreeData & storage_,
    const String & name_,
    const VolumePtr & volume_,
    const std::optional<String> & relative_path_,
    Type part_type_,
    const IMergeTreeDataPart * parent_part_)
    : storage(storage_)
    , name(name_)
    , info(MergeTreePartInfo::fromPartName(name_, storage.format_version))
    , volume(parent_part_ ? parent_part_->volume : volume_)
    , relative_path(relative_path_.value_or(name_))
    , index_granularity_info(storage_, part_type_)
    , versions(std::make_shared<MergeTreeDataPartVersions>(storage.getSettings()))
    , part_type(part_type_)
    , parent_part(parent_part_)
{
    if (parent_part)
        state = State::Committed;
    incrementStateMetric(state);
    incrementTypeMetric(part_type);
}

IMergeTreeDataPart::IMergeTreeDataPart(
    const MergeTreeData & storage_,
    const String & name_,
    const MergeTreePartInfo & info_,
    const VolumePtr & volume_,
    const std::optional<String> & relative_path_,
    Type part_type_,
    const IMergeTreeDataPart * parent_part_)
    : storage(storage_)
    , name(name_)
    , info(info_)
    , volume(parent_part_ ? parent_part_->volume : volume_)
    , relative_path(relative_path_.value_or(name_))
    , index_granularity_info(storage_, part_type_)
    , versions(std::make_shared<MergeTreeDataPartVersions>(storage.getSettings()))
    , part_type(part_type_)
    , parent_part(parent_part_)
{
    if (parent_part)
        state = State::Committed;
    incrementStateMetric(state);
    incrementTypeMetric(part_type);
}

IMergeTreeDataPart::~IMergeTreeDataPart()
{
    decrementStateMetric(state);
    decrementTypeMetric(part_type);

    /// clear cache
    /// TODO(lta): find out why it will lead to crash
    // if (storage.merging_params.mode == MergeTreeData::MergingParams::Unique && storage.unique_row_store_cache)
    //     storage.unique_row_store_cache->remove(getMemoryAddress());
}

String IMergeTreeDataPart::getNewName(const MergeTreePartInfo & new_part_info) const
{
    if (storage.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        /// NOTE: getting min and max dates from the part name (instead of part data) because we want
        /// the merged part name be determined only by source part names.
        /// It is simpler this way when the real min and max dates for the block range can change
        /// (e.g. after an ALTER DELETE command).
        DayNum min_date;
        DayNum max_date;
        MergeTreePartInfo::parseMinMaxDatesFromPartName(name, min_date, max_date);
        return new_part_info.getPartNameV0(min_date, max_date);
    }
    else
        return new_part_info.getPartName();
}

std::optional<size_t> IMergeTreeDataPart::getColumnPosition(const String & column_name) const
{
    auto it = column_name_to_position.find(column_name);
    if (it == column_name_to_position.end())
        return {};
    return it->second;
}


void IMergeTreeDataPart::setState(IMergeTreeDataPart::State new_state) const
{
    decrementStateMetric(state);
    state = new_state;
    incrementStateMetric(state);
}

IMergeTreeDataPart::State IMergeTreeDataPart::getState() const
{
    return state;
}


std::pair<DayNum, DayNum> IMergeTreeDataPart::getMinMaxDate() const
{
    if (storage.minmax_idx_date_column_pos != -1 && minmax_idx.initialized)
    {
        const auto & hyperrectangle = minmax_idx.hyperrectangle[storage.minmax_idx_date_column_pos];
        return {DayNum(hyperrectangle.left.get<UInt64>()), DayNum(hyperrectangle.right.get<UInt64>())};
    }
    else
        return {};
}

std::pair<time_t, time_t> IMergeTreeDataPart::getMinMaxTime() const
{
    if (storage.minmax_idx_time_column_pos != -1 && minmax_idx.initialized)
    {
        const auto & hyperrectangle = minmax_idx.hyperrectangle[storage.minmax_idx_time_column_pos];

        /// The case of DateTime
        if (hyperrectangle.left.getType() == Field::Types::UInt64)
        {
            assert(hyperrectangle.right.getType() == Field::Types::UInt64);
            return {hyperrectangle.left.get<UInt64>(), hyperrectangle.right.get<UInt64>()};
        }
        /// The case of DateTime64
        else if (hyperrectangle.left.getType() == Field::Types::Decimal64)
        {
            assert(hyperrectangle.right.getType() == Field::Types::Decimal64);

            auto left = hyperrectangle.left.get<DecimalField<Decimal64>>();
            auto right = hyperrectangle.right.get<DecimalField<Decimal64>>();

            assert(left.getScale() == right.getScale());

            return { left.getValue() / left.getScaleMultiplier(), right.getValue() / right.getScaleMultiplier() };
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part minmax index by time is neither DateTime or DateTime64");
    }
    else
        return {};
}


void IMergeTreeDataPart::setColumns(const NamesAndTypesList & new_columns)
{
    columns = new_columns;
    column_name_to_position.clear();
    column_name_to_position.reserve(new_columns.size());
    size_t pos = 0;
    for (const auto & column : columns)
    {
        column_name_to_position.emplace(column.name, pos);
        for (const auto & subcolumn : column.type->getSubcolumnNames())
            column_name_to_position.emplace(Nested::concatenateName(column.name, subcolumn), pos);
        ++pos;
    }
}

void IMergeTreeDataPart::removeIfNeeded()
{
    if (state == State::DeleteOnDestroy || is_temp)
    {
        try
        {
            auto path = getFullRelativePath();

            if (!volume->getDisk()->exists(path))
                return;

            if (is_temp)
            {
                String file_name = fileName(relative_path);

                if (file_name.empty())
                    throw Exception("relative_path " + relative_path + " of part " + name + " is invalid or not set", ErrorCodes::LOGICAL_ERROR);

                if (!startsWith(file_name, "tmp"))
                {
                    LOG_ERROR(storage.log, "~DataPart() should remove part {} but its name doesn't start with tmp. Too suspicious, keeping the part.", path);
                    return;
                }
            }

            if (parent_part)
            {
                std::optional<bool> keep_shared_data = keepSharedDataInDecoupledStorage();
                if (!keep_shared_data.has_value())
                    return;
                projectionRemove(parent_part->getFullRelativePath(), *keep_shared_data);
            }
            else
                remove();

            if (state == State::DeleteOnDestroy)
            {
                LOG_TRACE(storage.log, "Removed part from old location {}", path);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}


IMergeTreeDataPart::ChecksumsPtr IMergeTreeDataPart::getChecksums() const
{
    {
        std::lock_guard lock(checksums_mutex);
        if (checksums_ptr)
            return checksums_ptr;
    }

    auto cache = storage.getContext()->getChecksumsCache();
    if (cache && !is_temp && !isProjectionPart())
    {
        const String storage_unique_id = storage.getStorageUniqueID();
        auto load_func = [this] { return const_cast<IMergeTreeDataPart *>(this)->loadChecksums(true); };
        auto res = cache->getOrSet(storage_unique_id, getChecksumsCacheKey(storage_unique_id, *this), std::move(load_func)).first;
        return res;
    }
    else
    {
        return const_cast<IMergeTreeDataPart *>(this)->loadChecksums(true);
    }
}

IMergeTreeDataPart::IndexPtr IMergeTreeDataPart::getIndex() const
{
    /// lock is required here to prevent from multi-thread problems when the part is shared between more than one task.
    std::lock_guard<std::mutex> lock(index_mutex);

    if (index->empty())
        const_cast<IMergeTreeDataPart *>(this)->loadIndex();

    return index;
}

UInt64 IMergeTreeDataPart::getIndexSizeInBytes() const
{
    UInt64 res = 0;
    for (const ColumnPtr & column : *index)
        res += column->byteSize();
    return res;
}

UInt64 IMergeTreeDataPart::getIndexSizeInAllocatedBytes() const
{
    UInt64 res = 0;
    for (const ColumnPtr & column : *index)
        res += column->allocatedBytes();
    return res;
}

String IMergeTreeDataPart::stateToString(IMergeTreeDataPart::State state)
{
    switch (state)
    {
        case State::Temporary:
            return "Temporary";
        case State::PreCommitted:
            return "PreCommitted";
        case State::Committed:
            return "Committed";
        case State::Outdated:
            return "Outdated";
        case State::Deleting:
            return "Deleting";
        case State::DeleteOnDestroy:
            return "DeleteOnDestroy";
    }

    __builtin_unreachable();
}

String IMergeTreeDataPart::stateString() const
{
    return stateToString(state);
}

void IMergeTreeDataPart::assertState(const std::initializer_list<IMergeTreeDataPart::State> & affordable_states) const
{
    if (!checkState(affordable_states))
    {
        String states_str;
        for (auto affordable_state : affordable_states)
            states_str += stateToString(affordable_state) + " ";

        throw Exception("Unexpected state of part " + getNameWithState() + ". Expected: " + states_str, ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART);
    }
}

void IMergeTreeDataPart::assertOnDisk() const
{
    if (!isStoredOnDisk())
        throw Exception("Data part '" + name + "' with type '"
            + getType().toString() + "' is not stored on disk", ErrorCodes::LOGICAL_ERROR);
}


UInt64 IMergeTreeDataPart::getMarksCount() const
{
    return index_granularity.getMarksCount();
}

size_t IMergeTreeDataPart::getFileSizeOrZero(const String & file_name) const
{
    auto checksums = getChecksums();
    auto checksum = checksums->files.find(file_name);
    if (checksum == checksums->files.end())
        return 0;
    return checksum->second.file_size;
}

off_t IMergeTreeDataPart::getFileOffsetOrZero(const String & file_name) const
{
    auto checksums = getChecksums();
    auto checksum = checksums->files.find(file_name);
    if (checksum == checksums->files.end())
        return 0;
    return checksum->second.file_offset;
}

String IMergeTreeDataPart::getColumnNameWithMinimumCompressedSize(const StorageMetadataPtr & metadata_snapshot) const
{
    const auto & storage_columns = metadata_snapshot->getColumns().getAllPhysical();
    MergeTreeData::AlterConversions alter_conversions;
    if (!parent_part)
        alter_conversions = storage.getAlterConversionsForPart(shared_from_this());

    std::optional<std::string> minimum_size_column;
    UInt64 minimum_size = std::numeric_limits<UInt64>::max();

    for (const auto & column : storage_columns)
    {
        auto column_name = column.name;
        auto column_type = column.type;
        if (alter_conversions.isColumnRenamed(column.name))
            column_name = alter_conversions.getColumnOldName(column.name);

        if (!hasColumnFiles(column))
            continue;

        const auto size = getColumnSize(column_name, *column_type).data_compressed;
        if (size < minimum_size)
        {
            minimum_size = size;
            minimum_size_column = column_name;
        }
    }

    if (!minimum_size_column)
        throw Exception("Could not find a column of minimum size in MergeTree, part " + getFullPath(), ErrorCodes::LOGICAL_ERROR);

    return *minimum_size_column;
}

String IMergeTreeDataPart::getFullPath() const
{
    if (relative_path.empty())
        throw Exception("Part relative_path cannot be empty. It's bug.", ErrorCodes::LOGICAL_ERROR);

    return fs::path(storage.getFullPathOnDisk(volume->getDisk())) / (parent_part ? parent_part->relative_path : "") / relative_path / "";
}

String IMergeTreeDataPart::getFullRelativePath() const
{
    if (relative_path.empty())
        throw Exception("Part relative_path cannot be empty. It's bug.", ErrorCodes::LOGICAL_ERROR);

    return fs::path(storage.relative_data_path) / (parent_part ? parent_part->relative_path : "") / relative_path / "";
}

void IMergeTreeDataPart::loadColumnsChecksumsIndexes(bool require_columns_checksums, bool check_consistency)
{
    assertOnDisk();

    /// Memory should not be limited during ATTACH TABLE query.
    /// This is already true at the server startup but must be also ensured for manual table ATTACH.
    /// Motivation: memory for index is shared between queries - not belong to the query itself.
    MemoryTracker::BlockerInThread temporarily_disable_memory_tracker(VariableContext::Global);

 	// load versions before load checksum because loading checksum needs it to judge reading format.
    loadVersions();

    loadUUID();
    loadColumns(require_columns_checksums);
    loadChecksums(require_columns_checksums);
    loadIndexGranularity();
    calculateColumnsSizesOnDisk();
    loadIndex();     /// Must be called after loadIndexGranularity as it uses the value of `index_granularity`
    loadRowsCount(); /// Must be called after loadIndexGranularity() as it uses the value of `index_granularity`.
    loadPartitionAndMinMaxIndex();
    if (!parent_part)
    {
        loadTTLInfos();
        loadProjections(require_columns_checksums, check_consistency);
    }

    if (check_consistency)
        checkConsistency(require_columns_checksums);
    loadDefaultCompressionCodec();

}

void IMergeTreeDataPart::loadProjections(bool require_columns_checksums, bool check_consistency)
{
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    for (const auto & projection : metadata_snapshot->projections)
    {
        String path = getFullRelativePath() + projection.name + ".proj";
        if (volume->getDisk()->exists(path))
        {
            auto part = storage.createPart(projection.name, {"all", 0, 0, 0}, volume, projection.name + ".proj", this);
            part->loadColumnsChecksumsIndexes(require_columns_checksums, check_consistency);
            projection_parts.emplace(projection.name, std::move(part));
        }
    }
}

void IMergeTreeDataPart::loadIndexGranularity()
{
    throw Exception("Method 'loadIndexGranularity' is not implemented for part with type " + getType().toString(), ErrorCodes::NOT_IMPLEMENTED);
}

void IMergeTreeDataPart::loadIndexGranularity(const size_t , const std::vector<size_t> & )
{
    throw Exception("Method 'loadIndexGranularity' is not implemented for part with type " + getType().toString(), ErrorCodes::NOT_IMPLEMENTED);
}

void IMergeTreeDataPart::loadIndex()
{
    /// It can be empty in case of mutations
    if (!index_granularity.isInitialized())
        throw Exception("Index granularity is not loaded before index loading", ErrorCodes::LOGICAL_ERROR);

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    if (parent_part)
        metadata_snapshot = metadata_snapshot->projections.get(name).metadata;
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    size_t key_size = primary_key.column_names.size();

    if (key_size)
    {
        MutableColumns loaded_index;
        loaded_index.resize(key_size);

        for (size_t i = 0; i < key_size; ++i)
        {
            loaded_index[i] = primary_key.data_types[i]->createColumn();
            loaded_index[i]->reserve(index_granularity.getMarksCount());
        }

        String index_path = fs::path(getFullRelativePath()) / "primary.idx";
        auto index_file = openForReading(volume->getDisk(), index_path);

        size_t marks_count = index_granularity.getMarksCount();

        Serializations serializations(key_size);
        for (size_t j = 0; j < key_size; ++j)
            serializations[j] = primary_key.data_types[j]->getDefaultSerialization();

        for (size_t i = 0; i < marks_count; ++i) //-V756
            for (size_t j = 0; j < key_size; ++j)
                serializations[j]->deserializeBinary(*loaded_index[j], *index_file);

        for (size_t i = 0; i < key_size; ++i)
        {
            loaded_index[i]->protect();
            if (loaded_index[i]->size() != marks_count)
                throw Exception("Cannot read all data from index file " + index_path
                    + "(expected size: " + toString(marks_count) + ", read: " + toString(loaded_index[i]->size()) + ")",
                    ErrorCodes::CANNOT_READ_ALL_DATA);
        }

        if (!index_file->eof())
            throw Exception("Index file " + fullPath(volume->getDisk(), index_path) + " is unexpectedly long", ErrorCodes::EXPECTED_END_OF_FILE);

        index->assign(std::make_move_iterator(loaded_index.begin()), std::make_move_iterator(loaded_index.end()));
    }
}

NameSet IMergeTreeDataPart::getFileNamesWithoutChecksums() const
{
    if (!isStoredOnDisk())
        return {};

    NameSet result = {"checksums.txt", "columns.txt"};
    String default_codec_path = fs::path(getFullRelativePath()) / DEFAULT_COMPRESSION_CODEC_FILE_NAME;

    if (volume->getDisk()->exists(default_codec_path))
        result.emplace(DEFAULT_COMPRESSION_CODEC_FILE_NAME);

    return result;
}

void IMergeTreeDataPart::loadDefaultCompressionCodec()
{
    /// In memory parts doesn't have any compression
    if (!isStoredOnDisk())
    {
        default_codec = CompressionCodecFactory::instance().get("NONE", {});
        return;
    }

    String path = fs::path(getFullRelativePath()) / DEFAULT_COMPRESSION_CODEC_FILE_NAME;
    if (!volume->getDisk()->exists(path))
    {
        default_codec = detectDefaultCompressionCodec();
    }
    else
    {
        auto file_buf = openForReading(volume->getDisk(), path);
        String codec_line;
        readEscapedStringUntilEOL(codec_line, *file_buf);

        loadDefaultCompressionCodec(codec_line);
    }
}

CompressionCodecPtr IMergeTreeDataPart::detectDefaultCompressionCodec() const
{
    /// In memory parts doesn't have any compression
    if (!isStoredOnDisk())
        return CompressionCodecFactory::instance().get("NONE", {});

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();

    const auto & storage_columns = metadata_snapshot->getColumns();
    CompressionCodecPtr result = nullptr;
    for (const auto & part_column : columns)
    {
        /// It was compressed with default codec and it's not empty
        auto column_size = getColumnSize(part_column.name, *part_column.type);
        if (column_size.data_compressed != 0 && !storage_columns.hasCompressionCodec(part_column.name))
        {
            auto serialization = IDataType::getSerialization(part_column,
                [&](const String & stream_name)
                {
                    return volume->getDisk()->exists(stream_name + /*IMergeTreeDataPart::*/DATA_FILE_EXTENSION);
                });

            String path_to_data_file;
            serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
            {
                if (path_to_data_file.empty())
                {
                    String candidate_path = fs::path(getFullRelativePath()) / (ISerialization::getFileNameForStream(part_column, substream_path) + ".bin");

                    /// We can have existing, but empty .bin files. Example: LowCardinality(Nullable(...)) columns and column_name.dict.null.bin file.
                    if (volume->getDisk()->exists(candidate_path) && volume->getDisk()->getFileSize(candidate_path) != 0)
                        path_to_data_file = candidate_path;
                }
            });

            if (path_to_data_file.empty())
            {
                LOG_WARNING(storage.log, "Part's {} column {} has non zero data compressed size, but all data files don't exist or empty", name, backQuoteIfNeed(part_column.name));
                continue;
            }

            result = getCompressionCodecForFile(volume->getDisk(), path_to_data_file);
            break;
        }
    }

    if (!result)
        result = CompressionCodecFactory::instance().getDefaultCodec();

    return result;
}

void IMergeTreeDataPart::loadPartitionAndMinMaxIndex()
{
    if (storage.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING && !parent_part)
    {
        DayNum min_date;
        DayNum max_date;
        MergeTreePartInfo::parseMinMaxDatesFromPartName(name, min_date, max_date);

        const auto & date_lut = DateLUT::instance();
        partition = MergeTreePartition(date_lut.toNumYYYYMM(min_date));
        minmax_idx = MinMaxIndex(min_date, max_date);
    }
    else
    {
        String path = getFullRelativePath();
        if (!parent_part)
            partition.load(storage, volume->getDisk(), path);

        if (!isEmpty())
        {
            if (parent_part)
                // projection parts don't have minmax_idx, and it's always initialized
                minmax_idx.initialized = true;
            else
                minmax_idx.load(storage, volume->getDisk(), path);
        }
        if (parent_part)
            return;
    }

    if (info.isFakeDropRangePart()) /// Skip check if drop_range_part
        return;

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    String calculated_partition_id = partition.getID(metadata_snapshot->getPartitionKey().sample_block);
    if (calculated_partition_id != info.partition_id)
        throw Exception(
            "While loading part " + getFullPath() + ": calculated partition ID: " + calculated_partition_id
            + " differs from partition ID in part name: " + info.partition_id,
            ErrorCodes::CORRUPTED_DATA);
}

IMergeTreeDataPart::ChecksumsPtr IMergeTreeDataPart::loadChecksums(bool require)
{
    ChecksumsPtr checksums = std::make_shared<Checksums>();

    const String path = fs::path(getFullRelativePath()) / "checksums.txt";

    if (volume->getDisk()->exists(path))
    {
        checksums->versions = versions;
        auto buf = openForReading(volume->getDisk(), path);
        if (checksums->read(*buf))
        {
            assertEOF(*buf);
            /// bytes_on_disk may already loaded from metastore.
            if (!bytes_on_disk)
                bytes_on_disk = checksums->getTotalSizeOnDisk();
        }
        else if (!bytes_on_disk)
            bytes_on_disk = calculateTotalSizeOnDisk(volume->getDisk(), getFullRelativePath());
    }
    else
    {
        if (require)
            throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "No checksums.txt in part {} under path {}", name, path);

        /// If the checksums file is not present, calculate the checksums and write them to disk.
        /// Check the data while we are at it.
        LOG_WARNING(storage.log, "Checksums for part {} not found. Will calculate them from data on disk.", name);

        *checksums = checkDataPart(shared_from_this(), false);

        {
            auto out = volume->getDisk()->writeFile(fs::path(getFullRelativePath()) / "checksums.txt.tmp", 4096);
            checksums->write(*out);
        }

        volume->getDisk()->moveFile(fs::path(getFullRelativePath()) / "checksums.txt.tmp", fs::path(getFullRelativePath()) / "checksums.txt");

        bytes_on_disk = checksums->getTotalSizeOnDisk();
    }

    if (storage.getSettings()->enable_persistent_checksum || is_temp || isProjectionPart())
    {
        std::lock_guard lock(checksums_mutex);
        checksums_ptr = checksums;
    }

    return checksums;
}

void IMergeTreeDataPart::loadRowsCount()
{
    String path = fs::path(getFullRelativePath()) / "count.txt";
    if (index_granularity.empty())
    {
        rows_count = 0;
    }
    else if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING || part_type == Type::COMPACT || parent_part)
    {
        if (!volume->getDisk()->exists(path))
            throw Exception("No count.txt in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

        auto buf = openForReading(volume->getDisk(), path);
        readIntText(rows_count, *buf);
        assertEOF(*buf);

#ifndef NDEBUG
        /// columns have to be loaded
        for (const auto & column : getColumns())
        {
            /// Most trivial types
            if (column.type->isValueRepresentedByNumber() && !column.type->haveSubtypes())
            {
                auto size = getColumnSize(column.name, *column.type);

                if (size.data_uncompressed == 0)
                    continue;

                size_t rows_in_column = size.data_uncompressed / column.type->getSizeOfValueInMemory();
                if (rows_in_column != rows_count)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Column {} has rows count {} according to size in memory "
                        "and size of single value, but data part {} has {} rows", backQuote(column.name), rows_in_column, name, rows_count);
                }

                size_t last_possibly_incomplete_mark_rows = index_granularity.getLastNonFinalMarkRows();
                /// All this rows have to be written in column
                size_t index_granularity_without_last_mark = index_granularity.getTotalRows() - last_possibly_incomplete_mark_rows;
                /// We have more rows in column than in index granularity without last possibly incomplete mark
                if (rows_in_column < index_granularity_without_last_mark)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Column {} has rows count {} according to size in memory "
                        "and size of single value, but index granularity in part {} without last mark has {} rows, which is more than in column",
                        backQuote(column.name), rows_in_column, name, index_granularity.getTotalRows());
                }

                /// In last mark we actually written less or equal rows than stored in last mark of index granularity
                if (rows_in_column - index_granularity_without_last_mark > last_possibly_incomplete_mark_rows)
                {
                     throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Column {} has rows count {} in last mark according to size in memory "
                        "and size of single value, but index granularity in part {} in last mark has {} rows which is less than in column",
                        backQuote(column.name), rows_in_column - index_granularity_without_last_mark, name, last_possibly_incomplete_mark_rows);
                }
            }
        }
#endif
    }
    else
    {
        for (const NameAndTypePair & column : columns)
        {
            ColumnPtr column_col = column.type->createColumn();
            if (!column_col->isFixedAndContiguous() || column_col->lowCardinality())
                continue;

            size_t column_size = getColumnSize(column.name, *column.type).data_uncompressed;
            if (!column_size)
                continue;

            size_t sizeof_field = column_col->sizeOfValueIfFixed();
            rows_count = column_size / sizeof_field;

            if (column_size % sizeof_field != 0)
            {
                throw Exception(
                    "Uncompressed size of column " + column.name + "(" + toString(column_size)
                    + ") is not divisible by the size of value (" + toString(sizeof_field) + ")",
                    ErrorCodes::LOGICAL_ERROR);
            }

            size_t last_mark_index_granularity = index_granularity.getLastNonFinalMarkRows();
            size_t rows_approx = index_granularity.getTotalRows();
            if (!(rows_count <= rows_approx && rows_approx < rows_count + last_mark_index_granularity))
                throw Exception(
                    "Unexpected size of column " + column.name + ": " + toString(rows_count) + " rows, expected "
                    + toString(rows_approx) + "+-" + toString(last_mark_index_granularity) + " rows according to the index",
                    ErrorCodes::LOGICAL_ERROR);

            return;
        }

        throw Exception("Data part doesn't contain fixed size column (even Date column)", ErrorCodes::LOGICAL_ERROR);
    }
}

void IMergeTreeDataPart::loadTTLInfos()
{
    String path = fs::path(getFullRelativePath()) / "ttl.txt";
    if (volume->getDisk()->exists(path))
    {
        auto in = openForReading(volume->getDisk(), path);
        loadTTLInfos(*in);
    }
}

void IMergeTreeDataPart::loadUUID()
{
    String path = fs::path(getFullRelativePath()) / UUID_FILE_NAME;

    if (volume->getDisk()->exists(path))
    {
        auto in = openForReading(volume->getDisk(), path);
        readText(uuid, *in);
        if (uuid == UUIDHelpers::Nil)
            throw Exception("Unexpected empty " + String(UUID_FILE_NAME) + " in part: " + name, ErrorCodes::LOGICAL_ERROR);
    }
}

void IMergeTreeDataPart::loadColumns(bool require)
{
    String path = fs::path(getFullRelativePath()) / "columns.txt";
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    if (parent_part)
        metadata_snapshot = metadata_snapshot->projections.get(name).metadata;
    NamesAndTypesList loaded_columns;

    if (!volume->getDisk()->exists(path))
    {
        /// We can get list of columns only from columns.txt in compact parts.
        if (require || part_type == Type::COMPACT)
            throw Exception("No columns.txt in part " + name + ", expected path " + path + " on drive " + volume->getDisk()->getName(),
                ErrorCodes::NO_FILE_IN_DATA_PART);

        /// If there is no file with a list of columns, write it down.
        for (const NameAndTypePair & column : metadata_snapshot->getColumns().getAllPhysical())
            if (volume->getDisk()->exists(fs::path(getFullRelativePath()) / (getFileNameForColumn(column) + ".bin")))
                loaded_columns.push_back(column);

        if (columns.empty())
            throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

        {
            auto buf = volume->getDisk()->writeFile(path + ".tmp", 4096);
            loaded_columns.writeText(*buf);
        }
        volume->getDisk()->moveFile(path + ".tmp", path);
    }
    else
    {
        loaded_columns.readText(*volume->getDisk()->readFile(path));
    }

    setColumns(loaded_columns);
}

bool IMergeTreeDataPart::shallParticipateInMerges(const StoragePolicyPtr & storage_policy) const
{
    /// `IMergeTreeDataPart::volume` describes space where current part belongs, and holds
    /// `SingleDiskVolume` object which does not contain up-to-date settings of corresponding volume.
    /// Therefore we shall obtain volume from storage policy.
    auto volume_ptr = storage_policy->getVolume(storage_policy->getVolumeIndexByDisk(volume->getDisk()));

    return !volume_ptr->areMergesAvoided();
}

UInt64 IMergeTreeDataPart::calculateTotalSizeOnDisk(const DiskPtr & disk_, const String & from)
{
    if (disk_->isFile(from))
        return disk_->getFileSize(from);
    std::vector<std::string> files;
    disk_->listFiles(from, files);
    UInt64 res = 0;
    for (const auto & file : files)
        res += calculateTotalSizeOnDisk(disk_, fs::path(from) / file);
    return res;
}


void IMergeTreeDataPart::renameTo(const String & new_relative_path, bool remove_new_dir_if_exists) const
{
    assertOnDisk();

    String from = getFullRelativePath();
    String to = fs::path(storage.relative_data_path) / (parent_part ? parent_part->relative_path : "") / new_relative_path / "";

    if (!volume->getDisk()->exists(from))
        throw Exception("Part directory " + fullPath(volume->getDisk(), from) + " doesn't exist. Most likely it is a logical error.", ErrorCodes::FILE_DOESNT_EXIST);

    if (volume->getDisk()->exists(to))
    {
        if (remove_new_dir_if_exists)
        {
            Names files;
            volume->getDisk()->listFiles(to, files);

            LOG_WARNING(storage.log, "Part directory {} already exists and contains {} files. Removing it.", fullPath(volume->getDisk(), to), files.size());

            volume->getDisk()->removeRecursive(to);
        }
        else
        {
            throw Exception("Part directory " + fullPath(volume->getDisk(), to) + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);
        }
    }

    volume->getDisk()->setLastModified(from, Poco::Timestamp::fromEpochTime(time(nullptr)));
    volume->getDisk()->moveDirectory(from, to);
    relative_path = new_relative_path;

    SyncGuardPtr sync_guard;
    if (storage.getSettings()->fsync_part_directory)
        sync_guard = volume->getDisk()->getDirectorySyncGuard(to);

    storage.lockSharedData(*this);
}

std::optional<bool> IMergeTreeDataPart::keepSharedDataInDecoupledStorage() const
{
    /// NOTE: It's needed for S3 zero-copy replication
    if (force_keep_shared_data)
        return true;

    /// TODO Unlocking in try-catch and ignoring exception look ugly
    try
    {
        return !storage.unlockSharedData(*this);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "There is a problem with deleting part " + name + " from filesystem");
    }
    return {};
}

void IMergeTreeDataPart::remove() const
{
    std::optional<bool> keep_shared_data = keepSharedDataInDecoupledStorage();
    if (!keep_shared_data.has_value())
        return;

    if (!isStoredOnDisk())
        return;

    if (relative_path.empty())
        throw Exception("Part relative_path cannot be empty. This is bug.", ErrorCodes::LOGICAL_ERROR);

    if (isProjectionPart())
    {
        LOG_WARNING(storage.log, "Projection part {} should be removed by its parent {}.", name, parent_part->name);
        projectionRemove(parent_part->getFullRelativePath(), *keep_shared_data);
        return;
    }

    /// load checksums before move any part files
    auto checksums = getChecksums();

    /// load checksums for projections parts before removing parent part.
    for (const auto & [ _, projection_part] : projection_parts)
        projection_part->getChecksums();

    /** Atomic directory removal:
      * - rename directory to temporary name;
      * - remove it recursive.
      *
      * For temporary name we use "delete_tmp_" prefix.
      *
      * NOTE: We cannot use "tmp_delete_" prefix, because there is a second thread,
      *  that calls "clearOldTemporaryDirectories" and removes all directories, that begin with "tmp_" and are old enough.
      * But when we removing data part, it can be old enough. And rename doesn't change mtime.
      * And a race condition can happen that will lead to "File not found" error here.
      */

    fs::path from = fs::path(storage.relative_data_path) / relative_path;
    fs::path to = fs::path(storage.relative_data_path) / ("delete_tmp_" + name);
    // TODO directory delete_tmp_<name> is never removed if server crashes before returning from this function

    auto disk = volume->getDisk();
    if (disk->exists(to))
    {
        LOG_WARNING(storage.log, "Directory {} (to which part must be renamed before removing) already exists. Most likely this is due to unclean restart. Removing it.", fullPath(disk, to));
        try
        {
            disk->removeSharedRecursive(fs::path(to) / "", *keep_shared_data);
        }
        catch (...)
        {
            LOG_ERROR(storage.log, "Cannot recursively remove directory {}. Exception: {}", fullPath(disk, to), getCurrentExceptionMessage(false));
            throw;
        }
    }

    try
    {
        disk->moveDirectory(from, to);
    }
    catch (const fs::filesystem_error & e)
    {
        if (e.code() == std::errc::no_such_file_or_directory)
        {
            LOG_ERROR(storage.log, "Directory {} (part to remove) doesn't exist or one of nested files has gone. Most likely this is due to manual removing. This should be discouraged. Ignoring.", fullPath(disk, to));
            return;
        }
        throw;
    }

    // Record existing projection directories so we don't remove them twice
    std::unordered_set<String> projection_directories;
    for (const auto & [p_name, projection_part] : projection_parts)
    {
        projection_part->projectionRemove(to, *keep_shared_data);
        projection_directories.emplace(p_name + ".proj");
    }

    if (checksums->empty())
    {
        /// If the part is not completely written, we cannot use fast path by listing files.
        disk->removeSharedRecursive(fs::path(to) / "", *keep_shared_data);
    }
    else
    {
        try
        {
            /// Remove each expected file in directory, then remove directory itself.

    #if !defined(__clang__)
    #    pragma GCC diagnostic push
    #    pragma GCC diagnostic ignored "-Wunused-variable"
    #endif
			NameSet file_set;
            for (const auto & [file, _] : checksums->files)
            {
                // @ByteMap
                if (versions->enable_compact_map_data && isMapImplicitKey(file))
                {
                    /// When enable compact map data, all implicit column data of the same map column store in a file.
                    /// Thus, we only need to remove the file once.
                    String file_name = getColFileNameFromImplicitColFileName(file);
                    if (file_set.count(file_name))
                    {
                        continue;
                    }
                    file_set.insert(file_name);
                    if (projection_directories.find(file_name) == projection_directories.end())
                        disk->removeSharedFile(fs::path(to) / file_name, *keep_shared_data);
                }
				else
				{
					if (projection_directories.find(file) == projection_directories.end())
						disk->removeSharedFile(fs::path(to) / file, *keep_shared_data);
				}
            }
    #if !defined(__clang__)
    #    pragma GCC diagnostic pop
    #endif

            for (const auto & file : {"checksums.txt", "columns.txt"})
                disk->removeSharedFile(fs::path(to) / file, *keep_shared_data);

            disk->removeSharedFileIfExists(fs::path(to) / DEFAULT_COMPRESSION_CODEC_FILE_NAME, *keep_shared_data);
            disk->removeSharedFileIfExists(fs::path(to) / DELETE_ON_DESTROY_MARKER_FILE_NAME, *keep_shared_data);

            disk->removeDirectory(to);
        }
        catch (...)
        {
            /// Recursive directory removal does many excessive "stat" syscalls under the hood.

            LOG_ERROR(storage.log, "Cannot quickly remove directory {} by removing files; fallback to recursive removal. Reason: {}", fullPath(disk, to), getCurrentExceptionMessage(false));

            disk->removeSharedRecursive(fs::path(to) / "", *keep_shared_data);
        }
    }
}


void IMergeTreeDataPart::projectionRemove(const String & parent_to, bool keep_shared_data) const
{
    auto checksums = getChecksums();
    String to = parent_to + "/" + relative_path;
    auto disk = volume->getDisk();
    if (checksums->empty())
    {
        LOG_ERROR(
            storage.log,
            "Cannot quickly remove directory {} by removing files; fallback to recursive removal. Reason: checksums.txt is missing",
            fullPath(disk, to));
        /// If the part is not completely written, we cannot use fast path by listing files.
        disk->removeSharedRecursive(to + "/", keep_shared_data);
    }
    else
    {
        try
        {
            /// Remove each expected file in directory, then remove directory itself.

    #if !defined(__clang__)
    #    pragma GCC diagnostic push
    #    pragma GCC diagnostic ignored "-Wunused-variable"
    #endif
			NameSet file_set;
            for (const auto & [file, _] : checksums->files)
            {
                // @ByteMap
                if (versions->enable_compact_map_data && isMapImplicitKey(file))
                {
                    /// When enable compact map data, all implicit column data of the same map column store in a file.
                    /// Thus, we only need to remove the file once.
                    String file_name = getColFileNameFromImplicitColFileName(file);
                    if (file_set.count(file_name))
                    {
                        continue;
                    }
                    file_set.insert(file_name);
                    disk->removeSharedFile(to + "/" + file_name, keep_shared_data);
                }
				else
				{
                    disk->removeSharedFile(to + "/" + file, keep_shared_data);
                }
            }
    #if !defined(__clang__)
    #    pragma GCC diagnostic pop
    #endif

            for (const auto & file : {"checksums.txt", "columns.txt"})
                disk->removeSharedFile(to + "/" + file, keep_shared_data);
            disk->removeSharedFileIfExists(to + "/" + DEFAULT_COMPRESSION_CODEC_FILE_NAME, keep_shared_data);
            disk->removeSharedFileIfExists(to + "/" + DELETE_ON_DESTROY_MARKER_FILE_NAME, keep_shared_data);

            disk->removeSharedRecursive(to, keep_shared_data);
        }
        catch (...)
        {
            /// Recursive directory removal does many excessive "stat" syscalls under the hood.

            LOG_ERROR(storage.log, "Cannot quickly remove directory {} by removing files; fallback to recursive removal. Reason: {}", fullPath(disk, to), getCurrentExceptionMessage(false));

            disk->removeSharedRecursive(to + "/", keep_shared_data);
         }
     }
 }

String IMergeTreeDataPart::getRelativePathForPrefix(const String & prefix) const
{
    String res;

    /** If you need to detach a part, and directory into which we want to rename it already exists,
        *  we will rename to the directory with the name to which the suffix is added in the form of "_tryN".
        * This is done only in the case of `to_detached`, because it is assumed that in this case the exact name does not matter.
        * No more than 10 attempts are made so that there are not too many junk directories left.
        */
    for (int try_no = 0; try_no < 10; try_no++)
    {
        res = (prefix.empty() ? "" : prefix + "_") + name + (try_no ? "_try" + DB::toString(try_no) : "");

        if (!volume->getDisk()->exists(fs::path(getFullRelativePath()) / res))
            return res;

        LOG_WARNING(storage.log, "Directory {} (to detach to) already exists. Will detach to directory with '_tryN' suffix.", res);
    }

    return res;
}

String IMergeTreeDataPart::getRelativePathForDetachedPart(const String & prefix) const
{
    /// Do not allow underscores in the prefix because they are used as separators.
    assert(prefix.find_first_of('_') == String::npos);
    assert(prefix.empty() || std::find(DetachedPartInfo::DETACH_REASONS.begin(),
                                       DetachedPartInfo::DETACH_REASONS.end(),
                                       prefix) != DetachedPartInfo::DETACH_REASONS.end());
    return "detached/" + getRelativePathForPrefix(prefix);
}

void IMergeTreeDataPart::renameToDetached(const String & prefix) const
{
    renameTo(getRelativePathForDetachedPart(prefix), true);
}

void IMergeTreeDataPart::makeCloneInDetached(const String & prefix, const StorageMetadataPtr & /*metadata_snapshot*/) const
{
    String destination_path = fs::path(storage.relative_data_path) / getRelativePathForDetachedPart(prefix);

    /// Backup is not recursive (max_level is 0), so do not copy inner directories
    localBackup(volume->getDisk(), getFullRelativePath(), destination_path, 0);
    volume->getDisk()->removeFileIfExists(fs::path(destination_path) / DELETE_ON_DESTROY_MARKER_FILE_NAME);
}

void IMergeTreeDataPart::makeCloneOnDisk(const DiskPtr & disk, const String & directory_name) const
{
    assertOnDisk();

    if (disk->getName() == volume->getDisk()->getName())
        throw Exception("Can not clone data part " + name + " to same disk " + volume->getDisk()->getName(), ErrorCodes::LOGICAL_ERROR);
    if (directory_name.empty())
        throw Exception("Can not clone data part " + name + " to empty directory.", ErrorCodes::LOGICAL_ERROR);

    String path_to_clone = fs::path(storage.relative_data_path) / directory_name / "";

    if (disk->exists(fs::path(path_to_clone) / relative_path))
    {
        LOG_WARNING(storage.log, "Path " + fullPath(disk, path_to_clone + relative_path) + " already exists. Will remove it and clone again.");
    }
    disk->createDirectories(path_to_clone);
    volume->getDisk()->copy(getFullRelativePath(), disk, path_to_clone);
    volume->getDisk()->removeFileIfExists(fs::path(path_to_clone) / DELETE_ON_DESTROY_MARKER_FILE_NAME);
}

void IMergeTreeDataPart::checkConsistencyBase() const
{
    String path = getFullRelativePath();

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    if (parent_part)
        metadata_snapshot = metadata_snapshot->projections.get(name).metadata;
    else
    {
        // No need to check projections here because we already did consistent checking when loading projections if necessary.
    }

    const auto & pk = metadata_snapshot->getPrimaryKey();
    const auto & partition_key = metadata_snapshot->getPartitionKey();
    auto checksums = getChecksums();
    if (!checksums->empty())
    {
        if (!pk.column_names.empty() && !checksums->files.count("primary.idx"))
            throw Exception("No checksum for primary.idx", ErrorCodes::NO_FILE_IN_DATA_PART);

        if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            if (!checksums->files.count("count.txt"))
                throw Exception("No checksum for count.txt", ErrorCodes::NO_FILE_IN_DATA_PART);

            if (metadata_snapshot->hasPartitionKey() && !checksums->files.count("partition.dat"))
                throw Exception("No checksum for partition.dat", ErrorCodes::NO_FILE_IN_DATA_PART);

            if (!isEmpty() && !parent_part)
            {
                for (const String & col_name : storage.getMinMaxColumnsNames(partition_key))
                {
                    if (!checksums->files.count("minmax_" + escapeForFileName(col_name) + ".idx"))
                        throw Exception("No minmax idx file checksum for column " + col_name, ErrorCodes::NO_FILE_IN_DATA_PART);
                }
            }
        }

        checksums->checkSizes(volume->getDisk(), path);
    }
    else
    {
        auto check_file_not_empty = [&path](const DiskPtr & disk_, const String & file_path)
        {
            UInt64 file_size;
            if (!disk_->exists(file_path) || (file_size = disk_->getFileSize(file_path)) == 0)
                throw Exception("Part " + fullPath(disk_, path) + " is broken: " + fullPath(disk_, file_path) + " is empty", ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
            return file_size;
        };

        /// Check that the primary key index is not empty.
        if (!pk.column_names.empty())
            check_file_not_empty(volume->getDisk(), fs::path(path) / "primary.idx");

        if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            check_file_not_empty(volume->getDisk(), fs::path(path) / "count.txt");

            if (metadata_snapshot->hasPartitionKey())
                check_file_not_empty(volume->getDisk(), fs::path(path) / "partition.dat");

            if (!parent_part)
            {
                for (const String & col_name : storage.getMinMaxColumnsNames(partition_key))
                    check_file_not_empty(volume->getDisk(), fs::path(path) / ("minmax_" + escapeForFileName(col_name) + ".idx"));
            }
        }
    }
}

void IMergeTreeDataPart::checkConsistency(bool /* require_part_metadata */) const
{
    throw Exception("Method 'checkConsistency' is not implemented for part with type " + getType().toString(), ErrorCodes::NOT_IMPLEMENTED);
}


void IMergeTreeDataPart::calculateColumnsSizesOnDisk()
{
    if (getColumns().empty())
        throw Exception("Cannot calculate columns sizes when columns are not initialized", ErrorCodes::LOGICAL_ERROR);

    calculateEachColumnSizes(columns_sizes, total_columns_size);
}

ColumnSize IMergeTreeDataPart::getColumnSize(const String & column_name, const IDataType & /* type */) const
{
    /// For some types of parts columns_size maybe not calculated
    auto it = columns_sizes.find(column_name);
    if (it != columns_sizes.end())
        return it->second;

    return ColumnSize{};
}

void IMergeTreeDataPart::accumulateColumnSizes(ColumnToSize & column_to_size) const
{
    for (const auto & [column_name, size] : columns_sizes)
        column_to_size[column_name] = size.data_compressed;
}


bool IMergeTreeDataPart::checkAllTTLCalculated(const StorageMetadataPtr & metadata_snapshot) const
{
    if (!metadata_snapshot->hasAnyTTL())
        return false;

    if (metadata_snapshot->hasRowsTTL())
    {
        if (isEmpty()) /// All rows were finally deleted and we don't store TTL
            return true;
        else if (ttl_infos.table_ttl.min == 0)
            return false;
    }

    for (const auto & [column, desc] : metadata_snapshot->getColumnTTLs())
    {
        /// Part has this column, but we don't calculated TTL for it
        if (!ttl_infos.columns_ttl.count(column) && getColumns().contains(column))
            return false;
    }

    for (const auto & move_desc : metadata_snapshot->getMoveTTLs())
    {
        /// Move TTL is not calculated
        if (!ttl_infos.moves_ttl.count(move_desc.result_column))
            return false;
    }

    for (const auto & group_by_desc : metadata_snapshot->getGroupByTTLs())
    {
        if (!ttl_infos.group_by_ttl.count(group_by_desc.result_column))
            return false;
    }

    for (const auto & rows_where_desc : metadata_snapshot->getRowsWhereTTLs())
    {
        if (!ttl_infos.rows_where_ttl.count(rows_where_desc.result_column))
            return false;
    }

    return true;
}

SerializationPtr IMergeTreeDataPart::getSerializationForColumn(const NameAndTypePair & column) const
{
    auto checksums = getChecksums();
    return IDataType::getSerialization(column,
        [&](const String & stream_name)
        {
            return checksums->files.count(stream_name + DATA_FILE_EXTENSION) != 0;
        });
}

String IMergeTreeDataPart::getUniqueId() const
{
    String id;

    auto disk = volume->getDisk();

    if (disk->getType() == DB::DiskType::Type::S3)
        id = disk->getUniqueId(fs::path(getFullRelativePath()) / "checksums.txt");

    if (id.empty())
        throw Exception("Can't get unique S3 object", ErrorCodes::LOGICAL_ERROR);

    return id;
}


String IMergeTreeDataPart::getZeroLevelPartBlockID() const
{
    auto checksums = getChecksums();
    if (info.level != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to get block id for non zero level part {}", name);

    SipHash hash;
    checksums->computeTotalChecksumDataOnly(hash);
    union
    {
        char bytes[16];
        UInt64 words[2];
    } hash_value;
    hash.get128(hash_value.bytes);

    return info.partition_id + "_" + toString(hash_value.words[0]) + "_" + toString(hash_value.words[1]);
}

void IMergeTreeDataPart::storePartitionAndMinMaxIndex(WriteBuffer & buf) const
{
    if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        if (!parent_part)
            partition.store(storage, buf);
        
        if (!isEmpty())
        {
            if (!parent_part)
               minmax_idx.store(storage, name, buf); 
        }
    }
}

void IMergeTreeDataPart::loadColumns(ReadBuffer & buf)
{
    NamesAndTypesList loaded_columns;
    loaded_columns.readText(buf);
    setColumns(loaded_columns);
}

void IMergeTreeDataPart::loadPartitionAndMinMaxIndex(ReadBuffer & buf)
{
    if (storage.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        DayNum min_date;
        DayNum max_date;
        MergeTreePartInfo::parseMinMaxDatesFromPartName(name, min_date, max_date);

        const auto & date_lut = DateLUT::instance();
        partition = MergeTreePartition(date_lut.toNumYYYYMM(min_date));
        minmax_idx = MinMaxIndex(min_date, max_date);
    }
    else
    {
        if (!parent_part)
            partition.load(storage, buf);
        
        if (!isEmpty())
        {
            if (parent_part)
                // projection parts don't have minmax_idx, and it's always initialized
                minmax_idx.initialized = true;
            else
                minmax_idx.load(storage, buf);
        }
        if (parent_part)
            return;
    }

    if (info.isFakeDropRangePart()) /// Skip check if drop_range_part
        return;

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    String calculated_partition_id = partition.getID(metadata_snapshot->getPartitionKey().sample_block);
    if (calculated_partition_id != info.partition_id)
        throw Exception(
            "While loading part " + getFullPath() + ": calculated partition ID: " + calculated_partition_id
            + " differs from partition ID in part name: " + info.partition_id,
            ErrorCodes::CORRUPTED_DATA);
}

void IMergeTreeDataPart::loadTTLInfos(ReadBuffer & buf, bool old_meta_format)
{   
    assertString("ttl format version: ", buf);
    size_t format_version;
    readText(format_version, buf);
    assertChar('\n', buf);

    if (format_version == 1)
    {
        try
        {
            ttl_infos.read(buf, old_meta_format);
        }
        catch (const JSONException &)
        {
            throw Exception("Error while parsing file ttl.txt in part: " + name, ErrorCodes::BAD_TTL_FILE);
        }
    }
    else
        throw Exception("Unknown ttl format version: " + toString(format_version), ErrorCodes::BAD_TTL_FILE);
}

void IMergeTreeDataPart::loadDefaultCompressionCodec(const String & codec_str)
{
    ReadBufferFromString buf(codec_str);
    if (!checkString("CODEC", buf))
    {
        LOG_WARNING(storage.log, "Cannot parse default codec for part {} from codec string '{}'. Default compression codec will be deduced automatically.", name, codec_str);
        default_codec = detectDefaultCompressionCodec();
    }

    try
    {
        ParserCodec codec_parser;
        auto codec_ast = parseQuery(codec_parser, codec_str.data() + buf.getPosition(), codec_str.data() + codec_str.length(), "codec parser", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        default_codec = CompressionCodecFactory::instance().get(codec_ast, {});
    }
    catch (const DB::Exception & ex)
    {
        LOG_WARNING(storage.log, "Cannot parse default codec for part {} from codec string {}. Default compression codec will be deduced automatically. (Error : {})", name, codec_str, ex.what());
        default_codec = detectDefaultCompressionCodec();
    }
}

bool isCompactPart(const MergeTreeDataPartPtr & data_part)
{
    return (data_part && data_part->getType() == MergeTreeDataPartType::COMPACT);
}

bool isWidePart(const MergeTreeDataPartPtr & data_part)
{
    return (data_part && data_part->getType() == MergeTreeDataPartType::WIDE);
}

bool isInMemoryPart(const MergeTreeDataPartPtr & data_part)
{
    return (data_part && data_part->getType() == MergeTreeDataPartType::IN_MEMORY);
}

UInt32 IMergeTreeDataPart::numDeletedRows() const
{
    if (storage.merging_params.mode == MergeTreeData::MergingParams::Unique)
    {
        return delete_rows;
    }
    else if (delete_rows > 0 || is_delete_bitmap_cached)
    {
        /// we have loaded the delete bitmap before and cached its cardinality
        return delete_rows;
    }
    else
    {
        DeleteBitmapPtr part_delete_bitmap = getDeleteBitmap();
        return part_delete_bitmap ? delete_rows.load(std::memory_order_relaxed) : 0;
    }
}

UInt64 IMergeTreeDataPart::getVersionFromPartition() const
{
    assert(storage.merging_params.partitionValueAsVersion());
    if (!version_from_partition)
        version_from_partition.emplace(partition.value[0].safeGet<UInt64>()); /// lazy init
    return version_from_partition.value();
}

static MemoryUniqueKeyIndexPtr newUniqueArenaAndIndex(size_t num_elements, size_t extra_column_size)
{
    /// Choose arena initial size from 16~4096 based on estimated index size, reducing memory consumption for small part.
    size_t arena_initial_size = 4096;
    /// for an rough estimate, assume key to be 8 bytes
    size_t estimate_arena_size = num_elements * (8 + extra_column_size);
    if (estimate_arena_size < arena_initial_size)
        arena_initial_size = std::max(static_cast<size_t>(16), roundUpToPowerOfTwoOrZero(estimate_arena_size));
    return std::make_shared<MemoryUniqueKeyIndex>(num_elements, arena_initial_size);
}

void IMergeTreeDataPart::loadMemoryUniqueIndex([[maybe_unused]] const std::unique_lock<std::mutex> & unique_index_lock)
{
    assert(storage.merging_params.mode == MergeTreeData::MergingParams::Unique);
    assert(unique_index_lock.owns_lock());

    Stopwatch watch;

    /// load unique index from unique columns + optional version or is_offline column
    // Names read_columns = storage.unique_key_expr->getRequiredColumns();
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    Names read_columns = metadata_snapshot->getColumnsRequiredForUniqueKey();
    String extra_column_name = metadata_snapshot->extra_column_name;
    size_t extra_column_size = metadata_snapshot->extra_column_size;

    if (!extra_column_name.empty() && std::find(read_columns.begin(), read_columns.end(), extra_column_name) == read_columns.end())
        read_columns.push_back(extra_column_name);

    /// FIXME (UNIQUE KEY): Use delete bitmap to filter deleted rows, need to change
    ///     read logic inside MergeTreeSequentialSource
    Pipes pipes;
    auto source = std::make_unique<MergeTreeSequentialSource>(
            storage, metadata_snapshot, shared_from_this(),
            /*delete_bitmap=*/nullptr,
            read_columns,
            /*direct_io=*/ false,
            /*take_column_types_from_storage=*/true,
            /*quite=*/false,
            /*include_rowid_column=*/true);
    pipes.emplace_back(Pipe(std::move(source)));

    QueryPipeline pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes)));
    pipeline.setMaxThreads(1);

    BlockInputStreamPtr input = std::make_shared<MaterializingBlockInputStream>(
        std::make_shared<ExpressionBlockInputStream>(
            std::make_shared<PipelineExecutingBlockInputStream>(std::move(pipeline)),
             metadata_snapshot->getUniqueKeyExpression()));

    MemoryUniqueKeyIndexPtr new_unique_index = newUniqueArenaAndIndex(numRowsRemovingDeletes(), extra_column_size);
    ArenaPtr new_unique_index_arena = new_unique_index->getArena();

    input->readPrefix();
    while (const Block block = input->read())
    {
        const ColumnWithTypeAndName * extra_column = nullptr;
        if (!extra_column_name.empty())
            extra_column = &block.getByName(extra_column_name);
        auto & rowid_column = block.getByName(StorageInMemoryMetadata::rowid_column_name);

        size_t rows = block.rows();
        for (size_t i = 0; i < rows; ++i)
        {
            WriteBufferFromOwnString buf;
            for (auto & column_name : metadata_snapshot->getUniqueKeyColumns())
            {
                auto & col = block.getByName(column_name);
                auto serialization = col.type->getDefaultSerialization();
                serialization->serializeMemComparable(*col.column, i, buf);
            }
            String & key = buf.str();

            const char * arena_key_start = nullptr;
            if (extra_column)
            {
                /// write key's data and extra column
                char * res = new_unique_index_arena->alloc(key.size() + extra_column_size);
                memcpy(res, key.data(), key.size());
                if (metadata_snapshot->version_type & metadata_snapshot->flag_explicit_version)
                {
                    UInt64 version = extra_column->column->getUInt(i);
                    memcpy(res + key.size(), &version, extra_column_size);
                }
                else if (metadata_snapshot->version_type & metadata_snapshot->flag_is_offline)
                {
                    UInt8 is_offline_value = extra_column->column->getBool(i);
                    memcpy(res + key.size(), &is_offline_value, extra_column_size);
                }
                arena_key_start = res;
            }
            else
            {
                arena_key_start = new_unique_index_arena->insert(key.data(), key.size());
            }
            (*new_unique_index)[StringRef(arena_key_start, key.size())] = static_cast<UInt32>(rowid_column.column->getUInt(i));
        }
    }
    input->readSuffix();
    memory_unique_index = std::move(new_unique_index);
    auto memory_bytes = new_unique_index_arena->size() + memory_unique_index->getBufferSizeInBytes();
    memory_unique_index->setMemoryBytes(memory_bytes);
    memory_unique_index->setAccessTime(time(nullptr));
    LOG_DEBUG(
        storage.log,
        "loaded unique index ({}): size={}, memory={}, duration={}ms",
        name,
        memory_unique_index->size(),
        formatReadableSizeWithBinarySuffix(memory_bytes),
        watch.elapsedMilliseconds());
}

DiskUniqueKeyIndexPtr IMergeTreeDataPart::loadDiskUniqueIndex()
{
    assert(storage.merging_params.mode == MergeTreeData::MergingParams::Unique);
    return std::make_shared<DiskUniqueKeyIndex>(getFullPath() + UKI_FILE_NAME, storage.getContext()->getDiskUniqueKeyIndexBlockCache());
}

UniqueRowStorePtr IMergeTreeDataPart::loadUniqueRowStore()
{
    assert(storage.merging_params.mode == MergeTreeData::MergingParams::Unique);
    return std::make_shared<UniqueRowStore>(getFullPath() + UNIQUE_ROW_STORE_DATA_NAME, storage.getContext()->getDiskUniqueRowStoreBlockCache());
}

UInt64 IMergeTreeDataPart::gcUniqueIndexIfNeeded(const time_t * now, bool force_unload)
{
    if (storage.merging_params.mode != MergeTreeData::MergingParams::Unique)
        return 0;

    if (!memory_unique_index)
        return 0; /// index is not loaded, no need to GC

    UInt64 cur_time = now ? *now  : time(nullptr);
    std::unique_lock lock(unique_index_mutex);
    if (!memory_unique_index)
        return 0; /// index is not loaded, no need to GC

    auto index_size_before = memory_unique_index->size();
    auto memory_bytes_before = memory_unique_index->getMemorySize();
    auto num_rows = numRowsRemovingDeletes();

    auto settings = storage.getSettings();
    /// note that unique_index_access_time can't be 0 because unique index has been loaded
    bool idle_timeout = memory_unique_index->getAccessTime() + settings->unique_index_max_idle_seconds < cur_time;
    if (force_unload || num_rows == 0 || idle_timeout)
    {
        memory_unique_index.reset();
        String reason = force_unload ? "force" : (num_rows == 0 ? "all rows have been deleted" : "idle timeout");
        LOG_DEBUG(storage.log, "unload unique index of {}, reason: {}", name, reason);
        return memory_bytes_before;
    }

    /// gc when more than half of elements in index have been deleted
    if (static_cast<double>(num_rows) / memory_unique_index->size() >= 0.5)
        return 0;

    Stopwatch watch;
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    size_t extra_column_size = metadata_snapshot->extra_column_size;
    MemoryUniqueKeyIndexPtr new_unique_index = newUniqueArenaAndIndex(num_rows, extra_column_size);
    ArenaPtr new_unique_index_arena = new_unique_index->getArena();
    /// copy all existing elements from old index to new
    for (auto it = memory_unique_index->begin(), end = memory_unique_index->end(); it != end; ++it)
    {
        if (delete_bitmap && delete_bitmap->contains(it->getMapped()))
            continue;
        size_t value_size = it->getKey().size + extra_column_size;
        char * dst = new_unique_index_arena->alloc(value_size);
        memcpy(dst, it->getKey().data, value_size);
        (*new_unique_index)[StringRef(dst, it->getKey().size)] = it->getMapped();
    }
    /// free memory of old index
    memory_unique_index = std::move(new_unique_index);
    auto memory_bytes_after = new_unique_index_arena->size() + memory_unique_index->getBufferSizeInBytes();
    memory_unique_index->setMemoryBytes(memory_bytes_after);
    auto duration = watch.elapsedMilliseconds();
    if (duration >= 100)
        LOG_DEBUG(
            storage.log,
            "unique index gc ({}):size {}->{}, memory {}->{}, duration {} ms",
            name,
            index_size_before,
            memory_unique_index->size(),
            formatReadableSizeWithBinarySuffix(memory_bytes_before),
            formatReadableSizeWithBinarySuffix(memory_bytes_after),
            duration);
    else
        LOG_TRACE(
            storage.log,
            "unique index gc ({}):size {}->{}, memory {}->{}, duration {} ms",
            name,
            index_size_before,
            memory_unique_index->size(),
            formatReadableSizeWithBinarySuffix(memory_bytes_before),
            formatReadableSizeWithBinarySuffix(memory_bytes_after),
            duration);
    return memory_bytes_before - memory_bytes_after;
}

void IMergeTreeDataPart::writeDeleteFileWithVersion(const DeleteBitmapPtr & bitmap, UInt64 version) const
{
    /// serialize bitmap to buffer
    size_t size = bitmap->getSizeInBytes();
    PODArray<char> buffer(size);
    size = bitmap->write(buffer.data());

    /// write to file
    String path = getDeleteFilePathWithVersion(version);
    WriteBufferFromFile out(path, 4096);
    writeIntBinary(delete_file_format_version, out);
    writeIntBinary(size, out);
    out.write(buffer.data(), size);
}

String IMergeTreeDataPart::getDeleteFilePath() const
{
    return getFullPath() + delete_file_prefix + ".del";
}

void IMergeTreeDataPart::writeDeleteFile(const DeleteBitmapPtr & bitmap, bool sync)
{
    WriteBufferFromFile file_out(getDeleteFilePath(), 4096);
    HashingWriteBuffer out(file_out);

    /// serialize bitmap to buffer
    size_t size = bitmap->getSizeInBytes();
    PODArray<char> buffer(size);
    size = bitmap->write(buffer.data());
    /// write to file
    writeIntBinary(size, out);
    out.write(buffer.data(), size);
    out.next();
    if (sync)
        file_out.sync();

    getChecksums()->files[DELETE_BITMAP_FILE_NAME].file_size = out.count();
    getChecksums()->files[DELETE_BITMAP_FILE_NAME].file_hash = out.getHash();
}

void IMergeTreeDataPart::renameDeleteFileToVersion(UInt64 version) const
{
    auto existing_version = listDeleteFiles(/*min_version*/ 0);
    if (existing_version.empty())
    {
        throw Exception("Expect delete file exists for renaming", ErrorCodes::LOGICAL_ERROR);
    }
    UInt64 max_version = existing_version.back();
    String from_path = getDeleteFilePathWithVersion(max_version);
    String to_path = getDeleteFilePathWithVersion(version);
    LOG_TRACE(storage.log, "Renaming part delete file from: {} to {}", from_path, to_path);
    Poco::File(from_path).renameTo(to_path);
}

std::vector<UInt64> IMergeTreeDataPart::listDeleteFiles(UInt64 min_version) const
{
    std::vector<UInt64> result;
    Poco::DirectoryIterator end;
    String prefix = String(delete_file_prefix) + ".";
    for (auto it = Poco::DirectoryIterator(getFullPath()); it != end; ++it)
    {
        auto & filename = it.name();
        if (startsWith(filename, prefix))
        {
            auto suffix = filename.substr(prefix.size());
            ReadBufferFromString buf(suffix);
            UInt64 ver;
            if (!tryReadIntText(ver, buf))
                LOG_WARNING(storage.log, "Unrecognized file {} in part {}", filename, name);
            else if (ver >= min_version)
                result.push_back(ver);
        }
    }
    std::sort(result.begin(), result.end());
    return result;
}

UInt32 IMergeTreeDataPart::clearOldDeleteFilesIfNeeded(UInt64 commit_version, bool & retry_next_time) const
{
    if (storage.merging_params.mode != MergeTreeData::MergingParams::Unique)
        throw Exception("No need to clear delete files for Non-Unique table " + storage.log_name, ErrorCodes::LOGICAL_ERROR);

    auto settings = storage.getSettings();
    auto & stat = delete_files_stat;
    auto now = time(nullptr);
    bool has_updates = stat.last_add_time.load(std::memory_order_relaxed) >= stat.last_scan_dir_time;
    bool can_cache_more_versions = stat.versions.size() < settings->max_cached_delete_versions_per_part;
    bool reach_scan_interval = stat.last_scan_dir_time + settings->delete_file_gc_part_scan_interval <= static_cast<UInt64>(now);
    bool scanned = false;
    if (has_updates && can_cache_more_versions && reach_scan_interval)
    {
        /// scan dir for delete files
        scanned = true;
        stat.last_scan_dir_time = now;
        UInt64 min_version = stat.versions.empty() ? 0 : stat.versions.front();
        auto new_versions = listDeleteFiles(min_version);
        LOG_TRACE(storage.log, "Scanned directory for part {} and found {} delete files >= {}", name, new_versions.size(), min_version);
        std::swap(stat.versions, new_versions);
    }

    /// all versions (excluding current version) <= commit version can be deleted
    UInt32 num_removed = 0;
    auto remove_before = std::min(delete_version.load(std::memory_order_acquire), commit_version + 1);
    auto new_begin = std::lower_bound(stat.versions.begin(), stat.versions.end(), remove_before);
    for (auto it = stat.versions.begin(); it < new_begin; ++it)
    {
        Poco::File file(getDeleteFilePathWithVersion(*it));
        file.remove();
        num_removed++;
    }
    if (num_removed > 0)
    {
        LOG_TRACE(storage.log, "Removed {} delete files in [{},{}] from {}", num_removed, stat.versions.front(), *(new_begin - 1), name);
        std::vector<UInt64> new_versions {new_begin, stat.versions.end()};
        std::swap(stat.versions, new_versions);
    }
    retry_next_time = !scanned && num_removed == 0;
    return num_removed;
}

UInt32 IMergeTreeDataPart::clearDeleteFilesExceptCurrentVersion() const
{
    if (storage.merging_params.mode != MergeTreeData::MergingParams::Unique)
        throw Exception("No need to clear delete files for Non-Unique table " + storage.log_name, ErrorCodes::LOGICAL_ERROR);

    auto & stat = delete_files_stat;

    /// scan dir for delete files first
    stat.last_scan_dir_time = time(nullptr);
    UInt64 min_version = stat.versions.empty() ? 0 : stat.versions.front();
    auto new_versions = listDeleteFiles(min_version);
    LOG_TRACE(storage.log, "Scanned directory for part {} and found {} delete files >= {}", name, new_versions.size(), min_version);
    std::swap(stat.versions, new_versions);

    /// all versions except current version will be removed.
    UInt32 num_removed = 0;
    auto current_version = delete_version.load(std::memory_order_relaxed);
    for (auto it = stat.versions.begin(); it < stat.versions.end(); ++it)
    {
        if (*it != current_version)
        {
            Poco::File file(getDeleteFilePathWithVersion(*it));
            file.remove();
            num_removed++;
            LOG_TRACE(storage.log, "Remove unneeded delete file at version: {} for part: {}", *it, info.getPartName());
        }
    }
    if (num_removed > 0)
    {
        LOG_TRACE(storage.log, "Total removed {} delete files.", num_removed);
        std::vector<UInt64> tmp = {current_version};
        std::swap(stat.versions, tmp);
    }
    return num_removed;
}

DeleteBitmapPtr IMergeTreeDataPart::getDeleteBitmap() const
{
    if (storage.merging_params.mode == MergeTreeData::MergingParams::Unique)
    {
        /// For UniqueMergeTree, the latest delete bitmap is always cached in the field below
        return delete_bitmap;
    }
    else
    {
        /// For ordinary MergeTree, each part contains at most one delete bitmap.
        /// A part has a delete bitmap if and only if the delete file is contained in the checksums.
        /// A global DeleteBitmapCache is used to cache hot delete bitmaps.
        if (!Poco::File(getDeleteFilePath()).exists())
            return nullptr; /// the part doesn't have a delete bitmap

        Stopwatch watch;
        auto cache = storage.getContext()->getDeleteBitmapCache();
        DeleteBitmapPtr bitmap;
        bool hit_cache = false;

        /// Data part's memory address is used as the cache key.
        String cache_key = getMemoryAddress();

        /// In order to prevent a data part from picking up a delete bitmap cached by
        /// another freed data part with the same memory address, cache is searched only if
        /// the part has inserted its delete bitmap into the cache before.
        if (is_delete_bitmap_cached)
        {
            hit_cache = cache->lookup(cache_key, bitmap);
        }

        if (!hit_cache)
        {
            bitmap = readDeleteFile(true);
            if (!bitmap)
                throw Exception("Get null bitmap for part " + name, ErrorCodes::LOGICAL_ERROR);

            if (!is_delete_bitmap_cached)
            {
                cache->erase(cache_key);
                delete_rows = static_cast<UInt32>(bitmap->cardinality());
                const_cast<IMergeTreeDataPart &>(*this).is_delete_bitmap_cached = true;
            }
            cache->insert(cache_key, bitmap);
            LOG_TRACE(storage.log, "part " + name + " loaded delete bitmap (cardinality=" + toString(delete_rows.load(std::memory_order_relaxed)) + ") in " +
                                   toString(watch.elapsedMilliseconds()) + " ms");
        }

        return bitmap;
    }
}

DeleteBitmapPtr IMergeTreeDataPart::readDeleteFile(bool log_on_error) const
{
    String path = getDeleteFilePath();
    try
    {
        auto in = openForReading(volume->getDisk(), path);
        size_t buf_size;
        readIntBinary(buf_size, *in);
        PODArray<char> buf(buf_size);
        in->read(buf.data(), buf_size);
        Roaring bitmap = Roaring::read(buf.data());
        return std::make_shared<Roaring>(std::move(bitmap));
    }
    catch (...)
    {
        if (log_on_error)
            LOG_ERROR(storage.log, "Failed to load delete file " + path + " : " + DB::getCurrentExceptionMessage(false));
        throw;
    }
}

DeleteBitmapPtr IMergeTreeDataPart::readDeleteFileWithVersion(UInt64 version, bool log_on_error) const
{
    String path = getDeleteFilePathWithVersion(version);
    UInt8 format_version;
    try
    {
        auto in = openForReading(volume->getDisk(), path);
        readIntBinary(format_version, *in);
        if (format_version != delete_file_format_version)
            throw Exception("Unknown delete file version: " + toString(format_version), ErrorCodes::UNKNOWN_FORMAT_VERSION);
        size_t buf_size;
        readIntBinary(buf_size, *in);
        PODArray<char> buf(buf_size);
        in->read(buf.data(), buf_size);
        Roaring bitmap = Roaring::read(buf.data());
        return std::make_shared<Roaring>(std::move(bitmap));
    }
    catch (...)
    {
        if (log_on_error)
            LOG_ERROR(storage.log, "Failed to load delete file {} : {}", path, DB::getCurrentExceptionMessage(false));
        throw;
    }
}

UniqueKeyIndexPtr IMergeTreeDataPart::getUniqueKeyIndex() const
{
    if (storage.merging_params.mode != MergeTreeData::MergingParams::Unique)
    {
        throw Exception("getUniqueKeyIndex of " + storage.log_name + " which doesn't have unique key", ErrorCodes::LOGICAL_ERROR);
    }

    if (numRowsRemovingDeletes() == 0)
        return std::make_shared<UniqueKeyIndex>(); /// return empty index for empty part

    if (uki_type == UkiType::UNKNOWN)
    {
        if (!storage.getContext()->getSettingsRef().enable_disk_based_unique_key_index_method
            || !storage.getSettings()->enable_disk_based_unique_key_index || !Poco::File(getFullPath() + UKI_FILE_NAME).exists())
            uki_type = UkiType::MEMORY;
        else
        {
            uki_type = UkiType::DISK;
            String key = getMemoryAddress();
            storage.unique_key_index_cache->remove(key);
        }
    }

    if (uki_type == UkiType::MEMORY)
    {
        std::unique_lock lock(unique_index_mutex);
        if (!memory_unique_index)
            const_cast<IMergeTreeDataPart *>(this)->loadMemoryUniqueIndex(lock);
        memory_unique_index->setAccessTime(time(nullptr));
        return memory_unique_index;
    }
    else
    {
        if (!storage.unique_key_index_cache)
        {
            throw Exception("unique_key_index_cache of " + storage.log_name + " is nullptr", ErrorCodes::LOGICAL_ERROR);
        }

        /// Data part's memory address is used as the cache key.
        String key = getMemoryAddress();
        auto load_func = [this] { return const_cast<IMergeTreeDataPart *>(this)->loadDiskUniqueIndex(); };
        return storage.unique_key_index_cache->getOrSet(key, std::move(load_func)).first;
    }
}

UniqueRowStorePtr IMergeTreeDataPart::tryGetUniqueRowStore() const
{
    if (storage.merging_params.mode != MergeTreeData::MergingParams::Unique)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "tryGetUniqueRowStore of {} which doesn't have unique key", storage.log_name);
    }

    if (!storage.unique_row_store_cache)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "unique_row_store_cache of {} is nullptr", storage.log_name);
    }

    /// handle the case that parts of old versions which don't have row store file
    if (!Poco::File(getFullPath() + UNIQUE_ROW_STORE_DATA_NAME).exists())
        return nullptr;

    /// Data part's memory address is used as the cache key.
    String key = getMemoryAddress();
    auto load_func = [this] { return const_cast<IMergeTreeDataPart *>(this)->loadUniqueRowStore(); };
    return storage.unique_row_store_cache->getOrSet(key, std::move(load_func)).first;
}

bool IMergeTreeDataPart::getValueFromUniqueIndex(
    const UniqueKeyIndexPtr & unique_key_index, const String & key, UInt32 & rowid, UInt64 * version, UInt8 * is_offline) const
{
    auto num_rows = numRowsRemovingDeletes();
    if (num_rows == 0)
        return false; /// all rows have been deleted

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    String version_slice;
    size_t version_size = 0;
    size_t version_type = 0;
    if (version)
    {
        if (metadata_snapshot->version_type & metadata_snapshot->flag_partition_as_version)
        {
            version_size = 0;
            version_type = 1;
        }
        else if (metadata_snapshot->version_type & metadata_snapshot->flag_explicit_version)
        {
            version_size = sizeof(*version);
            version_type = 2;
        }
    }
    if (is_offline && (metadata_snapshot->version_type & metadata_snapshot->flag_is_offline))
    {
        version_size = sizeof(*is_offline);
        version_type = 3;
    }

    if (!unique_key_index->lookup(key, rowid, version_slice, version_size))
        return false;
    if (delete_bitmap && delete_bitmap->contains(rowid))
        return false;

    switch (version_type)
    {
        case 1:
            *version = getVersionFromPartition();
            break;
        case 2:
            memcpy(version, version_slice.data(), version_size);
            break;
        case 3:
            memcpy(is_offline, version_slice.data(), version_size);
            break;
        default:
            break;
    }
    return true;
}

/** ----------------------- COMPATIBLE CODE BEGIN-------------------------- */
#define META_FIELD_DELIMITER '\0'
void IMergeTreeDataPart::deserializeMetaInfo(const String & metadata)
{
    ReadBufferFromString buffer(metadata);

    deserializeColumns(buffer);

    size_t checksums_offset = 0;
    readText(checksums_offset, buffer);

    assertChar(META_FIELD_DELIMITER, buffer);

    size_t total_size;
    readText(total_size, buffer);
    bytes_on_disk = total_size;

    assertChar(META_FIELD_DELIMITER, buffer);

    readText(rows_count, buffer);

    assertChar(META_FIELD_DELIMITER, buffer);

    deserializePartitionAndMinMaxIndex(buffer);

    loadTTLInfos(buffer, true);

    readText(modification_time, buffer);

    assertChar(META_FIELD_DELIMITER, buffer);

    size_t marks_count = 0;
    readText(marks_count, buffer);

    assertChar(META_FIELD_DELIMITER, buffer);

    size_t index_offset = 0;
    readText(index_offset, buffer);

    assertChar(META_FIELD_DELIMITER, buffer);

    if (checkString("versions\n", buffer)) // check version header
    {
        versions->read(buffer, false);

        assertChar(META_FIELD_DELIMITER, buffer);
    }

    /// old part version dont have codec file, detect codec
    default_codec = detectDefaultCompressionCodec();
    loadIndexGranularity();
}

void IMergeTreeDataPart::deserializeColumns(ReadBuffer & buf)
{
    NamesAndTypesList loaded_columns;
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();
    assertString("columns format version: 1\n", buf);
    size_t count;
    DB::readText(count, buf);
    assertString(" columns:\n", buf);
    loaded_columns.resize(count);
    for (NameAndTypePair & it : loaded_columns)
    {
        readBackQuotedStringWithSQLStyle(it.name, buf);
        assertChar(' ', buf);
        String type_name;
        readString(type_name, buf);
        /// To speed up deserialization, we resolve column type by searching it in storage.columns.
        /// However, for some cases, storage columns may don't match with columns in part metainfo;
        /// for example, if we drop column from HaMergeTree, required column may missing in storage.columns
        /// when we change parts asynchronously. In such case, we directly parse column type from DataTypeFactory.
        auto all_physical_columns = storage.getInMemoryMetadataPtr()->getColumns();

        if (all_physical_columns.hasPhysical(it.name))
        {
            auto type = all_physical_columns.getPhysical(it.name).type;
            if (type->getName() == type_name)
                it.type = type;
            else
                it.type = data_type_factory.get(type_name);
        }
        else
            it.type = data_type_factory.get(type_name);
        skipToNextLineOrEOF(buf);
    }
    setColumns(loaded_columns);
}

void IMergeTreeDataPart::deserializePartitionAndMinMaxIndex(ReadBuffer & buffer)
{
    if (storage.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        DayNum min_date;
        DayNum max_date;
        MergeTreePartInfo::parseMinMaxDatesFromPartName(name, min_date, max_date);

        const auto & date_lut = DateLUT::instance();
        partition = MergeTreePartition(date_lut.toNumYYYYMM(min_date));
        minmax_idx = MinMaxIndex(min_date, max_date);
    }
    else
    {
        partition.read(storage, buffer);
        if (!isEmpty())
            minmax_idx.load(storage, buffer);
    }

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    String calculated_partition_id = partition.getID(metadata_snapshot->getPartitionKey().sample_block);
    if (calculated_partition_id != info.partition_id)
        throw Exception(
            "While loading part " + getFullPath() + ": calculated partition ID: " + calculated_partition_id
            + " differs from partition ID in part name: " + info.partition_id,
            ErrorCodes::CORRUPTED_DATA);
}

/*  -----------------------  COMPATIBLE CODE END -------------------------- */
}

