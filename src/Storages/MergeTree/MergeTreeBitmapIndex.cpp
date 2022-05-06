#include <Storages/MergeTree/MergeTreeBitmapIndex.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <Storages/MergeTree/MergeScheduler.h>
//#include <Storages/MergeTree/MergeTreeDataPart.h>
//#include <Storages/MergeTree/MergeTreeSequentialBlockInputStream.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>

#include <DataStreams/ExpressionBlockInputStream.h>


namespace DB
{

// No more than max_rows_scheduler_merge rows will be built bitmap concurrently.
// We always delay large part, which has rows larger than max_rows_scheduler_merge.
// If there is no part that is being built bitmap, i.e. current_rows = 0,
// build this part no matter how many rows it has.
bool BitmapTracker::check(const MergeTreeData::DataPartPtr & part, bool check_merge)
{
    if (!part)
        return false;

    std::lock_guard<std::mutex> lock(tracker_mutex);

    const BackgroundSchedulePool & pool = context->getSchedulePool();
    size_t total_threads_in_pool = pool.getNumberOfThreads();
    size_t total_merges = context->getMergeList().get().size();

    //std::cout<<"current_task: "<<current_task<<" total_merges: "<<total_merges<<" total_threads_in_pool: "<<total_threads_in_pool<<std::endl;
    if (current_task + total_merges > total_threads_in_pool)
        return false;

    size_t max_rows_to_build = context->getSettingsRef().max_rows_to_schedule_merge;
    if (MergeScheduler::expiredUTCTime(context))
        max_rows_to_build = context->getSettingsRef().strict_rows_to_schedule_merge;

    if (part->rows_count + current_rows > max_rows_to_build)
    {
        if (current_rows != 0)
            return false;
    }

    // If the (current merged rows) + (rows to build) > total_rows_to_schedule_merge, then stop build bitmap.
    // The check_merge flag is set by build_bitmap_index_in_merge, in this case, there may be too many merges
    // and bitmap building tasks to exhaust the memory
    if (check_merge && MergeScheduler::tooManyRowsToMerge(part->rows_count + current_rows, context))
    {
        //std::cout<<" <<<<<<<< return false since too many rows to build " << part->name <<std::endl;
        return false;
    }

    return true;
}

bool BitmapTracker::start(const MergeTreeData::DataPartPtr & part)
{
    if (!part)
        return false;

    std::lock_guard<std::mutex> lock(tracker_mutex);

    size_t max_rows_to_build = context->getSettingsRef().max_rows_to_schedule_merge;
    if (MergeScheduler::expiredUTCTime(context))
        max_rows_to_build = context->getSettingsRef().strict_rows_to_schedule_merge;

    // recheck build size.
    // current_rows accumulation and build size recheck should be under the control of one lock,
    // otherwise, concurrent buildPartIndex will add current_rows at the same time, which causes too many
    // parts to be built.
    if (part->rows_count + current_rows > max_rows_to_build)
    {
        if (current_rows != 0)
            return false;
    }

    current_rows += part->rows_count;
    current_task++;
    return true;
}

void BitmapTracker::end(const MergeTreeData::DataPartPtr & part)
{
    if (!part)
        return;

    std::lock_guard<std::mutex> lock(tracker_mutex);

    current_rows -= part->rows_count;
    current_task--;
}



MergeTreeBitmapIndex::MergeTreeBitmapIndex(MergeTreeData & data_)
    : data(data_)
    , bitmap_tracker(std::make_unique<BitmapTracker>(data.getContext()))
    , log(&Poco::Logger::get("MergeTreeBitmapIndex"))
{

}

bool MergeTreeBitmapIndex::isMarkBitmapIndexColumn(DataTypePtr column_type)
{
    return column_type->isMarkBitmapIndex();
}

bool MergeTreeBitmapIndex::isMarkBitmapIndexColumn(const IDataType & type)
{
    return type.isMarkBitmapIndex();
}

bool MergeTreeBitmapIndex::isMarkBitmapIndexColumn(const String & name)
{
    return MergeTreeBitmapIndex::isMarkBitmapIndexColumn(data.getInMemoryMetadataPtr()->getColumns().get(name).type);
}

bool MergeTreeBitmapIndex::isBitmapIndexColumn(const String & name)
{
    return MergeTreeBitmapIndex::isBitmapIndexColumn(data.getInMemoryMetadataPtr()->getColumns().get(name).type);
}

bool MergeTreeBitmapIndex::isBitmapIndexColumn(DataTypePtr column_type)
{
    /**
     * The old bitmap index is set by BLOOM attribution.
     * To make compatible with BitmapIndex / BLOOM, we should include BLOOM.
     **/
    return (column_type->isBitmapIndex() || column_type->isBloomSet());
}

bool MergeTreeBitmapIndex::isBitmapIndexColumn(const IDataType & type)
{
    /**
     * The old bitmap index is set by BLOOM attribution.
     * To make compatible with BitmapIndex / BLOOM, we should include BLOOM.
     **/
    return  (type.isBitmapIndex() || type.isBloomSet());
}

MergeTreeBitmapIndexStatus MergeTreeBitmapIndex::getMergeTreeBitmapIndexStatus()
{
    MergeTreeBitmapIndexStatus status;
    status.database = data.getStorageID().getDatabaseName();
    status.table = data.getStorageID().getTableName();
    status.parts = parts_for_bitmap.getParts();
    return status;
}

void MergeTreeBitmapIndex::buildPartIndex(const MergeTreeData::DataPartPtr & part)
{
    // TODO dongyifeng fix later
    //auto mutate_lock = std::unique_lock<std::mutex>(part->metainfo->mutate_mutex, std::try_to_lock);

//    if (stop_build_bitmap.isCancelled() || !mutate_lock.owns_lock())
//    {
//        addPartForBitMapIndex(part);
//        return;
//    }

    if (!part->checkState({MergeTreeData::DataPartState::Committed}))
        return;

    const NamesAndTypesList & columns = part->getColumns();
    NamesAndTypesList columns_to_build;
    // For some cases, MergeTreeData has BLOOM flag but part->columns does not.
    // It may be the case which part is copied from other storages without BLOOM.
    // Or the case that we alter the BLOOM column but column.txt has no BLOOM flag.
    // For all the cases, we always trust the metadata in MergeTreeData.
    // Thus for part without BLOOM, set a new flag and build index for them.
    for (const auto & column : columns)
    {
        if(MergeTreeBitmapIndex::isBitmapIndexColumn(column.type) || isBitmapIndexColumn(column.name))
        {
            // TODO dongyifeng handle with bloom keyword when merge bloom feature
            {
                if (!column.type->isBitmapIndex())
                    const_cast<IDataType *>(column.type.get())->setFlags(TYPE_BITMAP_INDEX_FLAG);
            }
            columns_to_build.push_back(column);
        }
    }

//    MergeTreeData::AlterDataPartTransactionPtr transaction = buildPartIndex(part, columns_to_build);
//    if (transaction)
//        transaction->commit();
//    else
//        const_cast<MergeTreeData::DataPart *>(part.get())->metainfo->has_bitmap = false;

}

//MergeTreeData::AlterDataPartTransactionPtr MergeTreeBitmapIndex::buildPartIndex(const MergeTreeData::DataPartPtr & part, const NamesAndTypesList & columns)
//{
//    /// @TODO(pengxindong): support remote parts.
//    if (part->info.storage_type != StorageType::Local)
//        return nullptr;
//
//    MergeTreeData::AlterDataPartTransactionPtr transaction(new MergeTreeData::AlterDataPartTransaction(part)); /// Blocks changes to the part.
//
//    // count the total rows built currently
//    BitmapCounter bitmap_counter(bitmap_tracker.get(), part);
//    if (!bitmap_counter.isBuilt())
//    {
//        addPartForBitMapIndex(part);
//        transaction->clear();
//        return nullptr;
//    }
//
//    NamesWithAliases projection;
//    for (const auto & column : columns)
//    {
//        if (!MergeTreeBitmapIndex::isBitmapIndexColumn(column)) continue;
//
//        String original_column_name = column.name;
//
//        // Skip build this part if it already has adx file and irk file
//        Poco::File adx_file(part->getFullPath()+original_column_name+AB_IDX_EXTENSION);
//        Poco::File irk_file(part->getFullPath()+original_column_name+AB_IRK_EXTENSION);
//        if (adx_file.exists() && irk_file.exists())
//        {
//            LOG_DEBUG(log, "Skip build part " + part->name + " of column " + column.name);
//            continue;
//        }
//
//        String temporary_column_name = original_column_name + " converting";
//        projection.emplace_back(original_column_name, temporary_column_name);
//        column.type->enumerateStreams(
//            [&](const IDataType::SubstreamPath & substream_path)
//            {
//                /// Skip array sizes, because they cannot be modified in ALTER.
//                if (!substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes)
//                    return;
//
//                String original_file_name = IDataType::getFileNameForStream(original_column_name, substream_path);
//                String temporary_file_name = IDataType::getFileNameForStream(temporary_column_name, substream_path);
//
//                transaction->rename_map[temporary_file_name + DATA_FILE_EXTENSION] = "";
//                transaction->rename_map[temporary_file_name + MARKS_FILE_EXTENSION] = "";
//            }, {});
//
//        if (MergeTreeBitmapIndex::isBitmapIndexColumn(*column.type))
//        {
//            if (MergeTreeBitmapIndex::isBloomColumn(*column.type))
//            {
//                transaction->rename_map[IDataType::getFileNameForStream(temporary_column_name, {})+
//                                        BLOOM_FILTER_FILE_EXTENSION] =
//                    IDataType::getFileNameForStream(original_column_name, {})+
//                    BLOOM_FILTER_FILE_EXTENSION;
//
//                transaction->rename_map[IDataType::getFileNameForStream(temporary_column_name, {})+
//                                        RANGE_BLOOM_FILTER_FILE_EXTENSION] =
//                    IDataType::getFileNameForStream(original_column_name, {})+
//                    RANGE_BLOOM_FILTER_FILE_EXTENSION;
//            }
//
//            transaction->rename_map[IDataType::getFileNameForStream(temporary_column_name, {})+
//                                    AB_IDX_EXTENSION] =
//                IDataType::getFileNameForStream(original_column_name, {})+
//                AB_IDX_EXTENSION;
//
//            transaction->rename_map[IDataType::getFileNameForStream(temporary_column_name, {})+
//                                    AB_IRK_EXTENSION] =
//                IDataType::getFileNameForStream(original_column_name, {})+
//                AB_IRK_EXTENSION;
//        }
//
//        LOG_DEBUG(log, "part " + part->name + " of column " + column.name + " will build index");
//    }
//
//    // if all column has been skipped, we just return transaction
//    // if there is no column should be built, return nullptr to reset has_bitmap flag
//    if (transaction->rename_map.empty() || part->isEmpty())
//    {
//        transaction->clear();
//        LOG_TRACE(log, "part " + part->name + " has no column to build bitmap");
//        if (columns.empty())
//            return nullptr;
//        else
//            return transaction;
//    }
//
//    // clean corrupt index files before build index
//    // There are cases that produce corrupt files
//    // 1. systemctl restart will terminate AlterTransaction, which left unfinished fiels
//    // 2. engine corruption will interrupt file write
//    {
//        String path = part->getFullPath();
//        for (const auto & from_to : transaction->rename_map)
//        {
//            try{
//                Poco::File corrupt_file(path + from_to.first);
//                if (corrupt_file.exists())
//                    corrupt_file.remove();
//            }
//            catch (...)
//            {
//                tryLogCurrentException(log, __PRETTY_FUNCTION__);
//                // do nothing
//            }
//        }
//    }
//
//    MergeTreeData::DataPart::Checksums add_checksums;
//    add_checksums.versions = part->versions;
//
//    MarkRanges ranges{MarkRange(0, part->marks_count)};
//    BlockInputStreamPtr part_in = std::make_shared<MergeTreeSequentialBlockInputStream>(
//        data, part, nullptr, columns.getNames(), false, false);
//
//    ExpressionActionsPtr actions = std::make_shared<ExpressionActions>(columns, data.getContext());
//    actions->add(ExpressionAction::project(projection));
//
//    ExpressionBlockInputStream in(part_in, actions);
//
//    auto compression_codec = data.getContext().chooseCompressionCodec(
//        data,
//        part->bytes_on_disk,
//        static_cast<double>(part->bytes_on_disk) / data.getTotalActiveSizeInBytes());
//
//    Block header = in.getHeader();
//    IMergedBlockOutputStream::WrittenOffsetColumns unused_written_offsets;
//    /// Now we disable remote parts building
//    MergedColumnOnlyOutputStream out(data, StorageType::Local, "", "", header, part, true /* sync */, compression_codec, true /* skip_offsets */, unused_written_offsets);
//
//    in.readPrefix();
//    out.writePrefix();
//
//    while (Block b = in.read())
//    {
//        if (stop_build_bitmap.isCancelled())
//        {
//            addPartForBitMapIndex(part);
//            return nullptr;
//        }
//        out.write(b, {.enable_build_index_in_alter = true});
//    }
//
//    in.readSuffix();
//    // Get the checksums.
//    add_checksums = out.writeSuffixAndGetChecksums();
//
//    // delete unused rename_map item
//    // There are cases that bitmap index failed to write and we do not want
//    // an exception caused by such files.
//    // For some logical, like FASTDELETE, a block may be empty so that we will not
//    // generate any index file, so that we should skip these.
//    {
//        String path = part->getFullPath();
//        for (auto it = transaction->rename_map.begin(); it != transaction->rename_map.end();)
//        {
//            String file_path = path + it->first;
//            Poco::File corrupt_file(file_path);
//            if (!corrupt_file.exists())
//            {
//                it = transaction->rename_map.erase(it);
//                continue;
//            }
//            ++it;
//        }
//    }
//
//    // Update the checksums
//    MergeTreeData::DataPart::Checksums new_checksums = part->getChecksumsWithColumnsLock();
//    for (auto it : transaction->rename_map)
//    {
//        if (!it.second.empty() && add_checksums.files.count(it.first))
//            new_checksums.files[it.second] = add_checksums.files[it.first];
//    }
//
//    /// Write the checksums to the temporary file.
//    bool checksums_empty = false;
//    {
//        auto lock = part->getColumnsReadLock();
//        checksums_empty = part->checksums.empty();
//    }
//    if (!checksums_empty)
//    {
//        transaction->new_checksums = new_checksums;
//        WriteBufferFromFile checksums_file(part->getFullPath() + "checksums.txt.tmp", 4096);
//        new_checksums.versions = part->versions;
//        new_checksums.write(checksums_file);
//        transaction->rename_map["checksums.txt.tmp"] = "checksums.txt";
//    }
//
//    /// Write the new column list to the temporary file.
//    {
//        transaction->new_columns = part->columns;
//        WriteBufferFromFile columns_file(part->getFullPath() + "columns.txt.tmp", 4096);
//        transaction->new_columns.writeText(columns_file);
//        transaction->rename_map["columns.txt.tmp"] = "columns.txt";
//    }
//
//    return transaction;
//}


void MergeTreeBitmapIndex::getPartsForBuildingBitmap()
{
    MergeTreeData::DataPartsVector parts = data.getDataPartsVector();
    size_t parts_cnt = 0;
    auto part_columns = data.getInMemoryMetadataPtr()->getColumns().getOrdinary();
    std::map<String, std::set<String>> parts_to_bitmap;
    std::map<String, std::set<String>> neighbors;

    auto existBitmap = [&](const NameAndTypePair & column, const String & part_path) -> bool
    {
        String column_name = column.name;
        String adx_name = column_name + AB_IDX_EXTENSION;
        String irk_name = column_name + AB_IRK_EXTENSION;
        Poco::File adx_file(part_path + adx_name);
        Poco::File irk_file(part_path + irk_name);
        if (adx_file.exists() && irk_file.exists())
            return true;

        return false;
    };

    // First round for infecting parts
    // 1. Collect partition which has bitmap index (source of infection)
    // 2. Collect neighbors of each partition （susceptible)
    String pre_partition_id;
    for (const auto & part : parts)
    {
        for (const NameAndTypePair & column : part_columns)
        {
            if (MergeTreeBitmapIndex::isBitmapIndexColumn(*column.type))
            {
                if (existBitmap(column, part->getFullPath()))
                {
                    parts_to_bitmap[part->info.partition_id].insert(column.name);
                    continue;
                }
            }
        }

        if (pre_partition_id.empty())
            pre_partition_id = part->info.partition_id;
        else
        {
            if (pre_partition_id != part->info.partition_id)
            {
                neighbors[part->info.partition_id].insert(pre_partition_id);
                neighbors[pre_partition_id].insert(part->info.partition_id);
                pre_partition_id = part->info.partition_id;
            }
        }
    }

    // Second round fro infecting parts in the same partition.
    // 1. If one of parts in a partition has bitmap, then all parts in this partition will build bitmap (infection)
    // 2. If one partition is between two neighbors, then all parts in this partition will build bitmap (infection)
    for (const auto & part : parts)
    {
        bool infected_part = false;

        for (const NameAndTypePair & column : part_columns)
        {
            if (MergeTreeBitmapIndex::isBitmapIndexColumn(*column.type))
            {
                if (!existBitmap(column, part->getFullPath()))
                {
                    auto it = parts_to_bitmap.find(part->info.partition_id);
                    if (it != parts_to_bitmap.end() && it->second.count(column.name))
                    {
                        addPartForBitMapIndex(part);
                        parts_cnt++;
                        infected_part = true;
                    }
                }
                else
                    infected_part = true;
            }
        }

        auto it = neighbors.find(part->info.partition_id);
        if (!infected_part && it != neighbors.end() && it->second.size() == 2)
        {
            bool infected = true;
            for (const auto & partition_id : it->second)
            {
                auto part_it = parts_to_bitmap.find(partition_id);
                if (part_it == parts_to_bitmap.end() || part_it->second.empty())
                {
                    infected = false;
                    break;
                }
            }

            if (infected)
            {
                addPartForBitMapIndex(part);
                parts_cnt++;
            }
        }
    }

    LOG_TRACE(log, "Get {} parts for building bitmap.", parts_cnt);
}

bool MergeTreeBitmapIndex::checkForBuildingBitMap()
{
    size_t part_cnt = 0;
    while(!emptyQueueForBuildingBitMap())
    {
        if (stop_build_bitmap.isCancelled())
            return false;

        MergeTreeData::DataPartPtr part = getPartForBitMapIndex();

        // Check whether the part is valid
        {
            // TODO dongyifeng change to lockPartsRead
            auto lock = data.lockParts();
            if (!part || !data.hasPart(part->info, {MergeTreeData::DataPartState::Committed}))
                continue;
        }

        if (bitmap_tracker && !bitmap_tracker->check(part, data.getSettings()->build_bitmap_index_in_merge))
        {
            addPartForBitMapIndex(part);
            continue;
        }

        if (auto containing_part = data.getActiveContainingPart(part->info))
        {
            if (containing_part->info.min_block < part->info.min_block
                || containing_part->info.max_block > part->info.max_block)
                continue;
        }

        // Do not build part if it is to be merged, since the merged
        // part also has bitmap index.
        if (MergeScheduler::inMerge(part->name, data.getContext()))
            continue;

        //TODO dongyifeng fixed when metainfo merged
//        try{
//            const_cast<MergeTreeData::DataPart *>(part.get())->metainfo->has_bitmap = true;
//            buildPartIndex(part);
//        }catch(...){
//            const_cast<MergeTreeData::DataPart *>(part.get())->metainfo->has_bitmap = false;
//            tryLogCurrentException(__PRETTY_FUNCTION__);
//            addPartForBitMapIndex(part);
//        }
        part_cnt++;
    }
    return part_cnt > 0;
}

bool MergeTreeBitmapIndex::onlyBuildBitmap(const NamesAndTypesList & old_columns, const NamesAndTypesList & new_columns)
{
    bool only_build_bitmap = false;

    using NameToType = std::map<String, const IDataType *>;
    NameToType new_types;
    for (const NameAndTypePair & column : new_columns)
        new_types.emplace(column.name, column.type.get());

    for (const NameAndTypePair & column : old_columns)
    {
        if (new_types.count(column.name))
        {
            const auto * new_type = new_types[column.name];
            const auto * old_type = column.type.get();
            if (!new_type->equals(*old_type))
                return false;
            else if ((MergeTreeBitmapIndex::isBitmapIndexColumn(*new_type) && !MergeTreeBitmapIndex::isBitmapIndexColumn(*old_type)))
            {
                if ((old_type->getFlags() & (~(TYPE_BLOOM_FLAG | TYPE_BITMAP_INDEX_FLAG)))
                    == (new_type->getFlags() & (~TYPE_BLOOM_FLAG | TYPE_BITMAP_INDEX_FLAG)))
                    only_build_bitmap = true;
            }
        }
        else
            return false;
    }

    return only_build_bitmap;
}


void MergeTreeBitmapIndex::dropPartIndex(const MergeTreeData::DataPartPtr & part)
{
    /// @TODO(pengxindong): support remote parts
    // TODO dongyifeng add check later
//    if (part->info.storage_type != StorageType::Local)
//        return;

    if (!part->checkState({MergeTreeData::DataPartState::Committed}))
        return;

    const NamesAndTypesList & columns = part->getColumns();
    NamesAndTypesList columns_to_drop;
    for (const auto & column : columns)
    {
        if(MergeTreeBitmapIndex::isBitmapIndexColumn(*column.type))
            columns_to_drop.push_back(column);
    }

//    MergeTreeData::AlterDataPartTransactionPtr transaction = dropPartIndex(part, columns_to_drop);
//    if (transaction)
//    {
//        const_cast<MergeTreeData::DataPart *>(part.get())->metainfo->has_bitmap = false;
//        transaction->commit();
//    }
}

//MergeTreeData::AlterDataPartTransactionPtr MergeTreeBitmapIndex::dropPartIndex(const MergeTreeData::DataPartPtr & part, const NamesAndTypesList & columns)
//{
//    MergeTreeData::AlterDataPartTransactionPtr transaction(new MergeTreeData::AlterDataPartTransaction(part)); /// Blocks changes to the part.
//    for (const auto & column : columns)
//    {
//        String column_name = column.name;
//        if (MergeTreeBitmapIndex::isBitmapIndexColumn(*column.type))
//        {
//            transaction->rename_map[IDataType::getFileNameForStream(column_name, {})+AB_IDX_EXTENSION] = "";
//            transaction->rename_map[IDataType::getFileNameForStream(column_name, {})+AB_IRK_EXTENSION] = "";
//        }
//    }
//
//    if (transaction->rename_map.empty())
//    {
//        transaction->clear();
//        return nullptr;
//    }
//
//    MergeTreeData::DataPart::Checksums checksums = part->getChecksumsWithColumnsLock();
//    for (auto it : transaction->rename_map)
//    {
//        if (it.second.empty())
//            checksums.files.erase(it.first);
//    }
//
//    if (!checksums.empty())
//    {
//        transaction->new_checksums = checksums;
//        WriteBufferFromFile checksums_file(part->getFullPath() + "checksums.txt.tmp", 4096);
//        checksums.versions = part->versions;
//        checksums.write(checksums_file);
//        transaction->rename_map["checksums.txt.tmp"] = "checksums.txt";
//    }
//
//    /// Write the new column to transaction
//    transaction->new_columns = part->columns;
//
//    return transaction;
//}
}
