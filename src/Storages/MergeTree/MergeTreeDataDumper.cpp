#include <Storages/HDFS/HDFSCommon.h>
#include <IO/WriteBufferFromFile.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Storages/MergeTree/MergeTreeDataDumper.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Common/escapeForFileName.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int BAD_HDFS_META_FILE;
        extern const int BAD_HDFS_DATA_FILE;
    }

std::unique_ptr<WriteBuffer> MergeTreeDataDumper::createWriteBuffer(const String & file_name, const StorageType & /*type*/)
{
    switch (type)
    {
#if USE_HDFS
        case StorageType::HDFS:
        {
            return std::make_unique<WriteBufferFromHDFS>(file_name, data.getContext()->getHdfsConnectionParams());
        }
#endif
        default:
            return std::make_unique<WriteBufferFromFile>(file_name);
    }
}

std::unique_ptr<ReadBuffer> MergeTreeDataDumper::createReadBuffer(const String & file_name, const StorageType & /*type*/)
{
    switch (type)
    {
#if USE_HDFS
        case StorageType::HDFS:
        {
            return std::make_unique<ReadBufferFromByteHDFS>(file_name, false, data.getContext()->getHdfsConnectionParams());
        }
#endif
        default:
            return std::make_unique<ReadBufferFromFile>(file_name);
    }
}

void MergeTreeDataDumper::copyMetaFile(ReadBuffer & from, WriteBuffer & to, const size_t file_size)
{
    writeIntBinary(file_size, to);
    copyData(from, to);
}

void MergeTreeDataDumper::writeMetaFileHeader(size_t marks_count, size_t rows_count, WriteBuffer & to)
{
    writeString(magic_code, to);
    writeIntBinary(version.toUnderType(), to);
    writeIntBinary(marks_count, to);
    writeIntBinary(rows_count, to);
    ///TODO: fix writeNull
    // writeNull(MERGE_TREE_STORAGE_LEVEL_1_META_HEADER_SIZE - to.count(), to);
}

void MergeTreeDataDumper::readMetaFileFooter(uint128 & checksum, ReadBuffer & from)
{
    readPODBinary(checksum, from);
}

void MergeTreeDataDumper::writeMetaFileFooter(uint128 checksum, WriteBuffer & to)
{
    writePODBinary(checksum, to);
}

void MergeTreeDataDumper::writeFileNamesAndOffsets(const String name, size_t offset, size_t size, WriteBuffer & to)
{
    writeStringBinary(name, to);
    writeIntBinary(offset, to);
    writeIntBinary(size, to);
}

void MergeTreeDataDumper::writeDataFileHeader(WriteBuffer & to)
{
    writeString(magic_code, to);
    writeIntBinary(version.toUnderType(), to);
    ///TODO fix writeNull
    // writeNull(MERGE_TREE_STORAGE_LEVEL_1_DATA_HEADER_SIZE - to.count(), to);
}

/// Check correctness of meta file in remote storage.
bool MergeTreeDataDumper::checkMeta(MergeTreeMetaBase::DataPartPtr remote_part, uint128 )
{
    String remote_meta_file_path = remote_part->getFullPath() + "/meta";
    /*size_t remote_meta_file_size = HDFSCommon::File(remote_meta_file_path).getSize();
    if (remote_meta_file_size <= sizeof(meta_checksum))
        return false;

    String hdfs_user = data.global_context.getHdfsUser();
    String hdfs_nnproxy = data.lobal_context.getHdfsNNProxy();
#if USE_HDFS
    ReadBufferFromHDFS remote_meta_file(remote_meta_file_path, false, hdfs_user, hdfs_nnproxy);
    remote_meta_file.seek(remote_meta_file_size - sizeof(meta_checksum));
    uint128 remote_meta_checksum {};
    readMetaFileFooter(remote_meta_checksum, remote_meta_file);
#endif
    return remote_meta_checksum == meta_checksum;*/
    return true;
}

/// Check correctness of data file in remote storage,
/// Now we only check data file length.
bool MergeTreeDataDumper::checkData(MergeTreeMetaBase::DataPartPtr remote_part)
{
    String remote_data_file_path = remote_part->getFullPath() + "/data";
    /*size_t remote_data_file_size = HDFSCommon::File(remote_data_file_path).getSize();
    size_t data_files_size = MERGE_TREE_STORAGE_LEVEL_1_DATA_HEADER_SIZE;
    for (auto & file : remote_part->getChecksums()->files)
    {
        data_files_size += file.second.file_size;
    }

    return data_files_size == remote_data_file_size;*/
    return true;
}

void MergeTreeDataDumper::check(MergeTreeMetaBase::DataPartPtr remote_part, uint128 meta_checksum)
{
    if (!checkMeta(remote_part, meta_checksum))
        throw Exception("Fail to check meta in remote part: " + remote_part->name, ErrorCodes::BAD_HDFS_META_FILE);

    if (!checkData(remote_part))
        throw Exception("Fail to check data in remote part: " + remote_part->name, ErrorCodes::BAD_HDFS_DATA_FILE);
}

MergeTreeMetaBase::MutableDataPartPtr
MergeTreeDataDumper::dumpTempPart(MergeTreeMetaBase::DataPartPtr staled_part, const StorageLevel storage_level)
{
    const String TMP_PREFIX = "tmp_dump_";
    String partition_id = staled_part->info.partition_id;
    Int64 min_block = staled_part->info.min_block;
    Int64 max_block = staled_part->info.max_block;
    UInt32 merge_tree_level = staled_part->info.level;
    MergeTreePartInfo new_part_info(partition_id, min_block, max_block, merge_tree_level, 0, storage_level);
    new_part_info.storage_type = StorageType::HDFS;

    String part_name = staled_part->name;
    /// TODO: FIX
    // DiskPtr disk = data.getStorageSelector().getHDFSVolume()->getNextDisk();

    auto space_reservation = data.reserveSpace(staled_part->getBytesOnDisk());
    DiskPtr disk = space_reservation->getDisk();
    /// Create new data part object
    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, disk, 0);
    auto new_part = std::make_shared<MergeTreeDataPart>(data, part_name, new_part_info, single_disk_volume, TMP_PREFIX + part_name);
    new_part->partition.assign(staled_part->partition);
    new_part->prepared_checksums = std::make_shared<MergeTreeDataPart::Checksums>(*staled_part->getChecksums());
    new_part->prepared_checksums->storage_type = StorageType::HDFS;
    new_part->minmax_idx = staled_part->minmax_idx;
    new_part->rows_count = staled_part->rows_count;
    /// TODO: FIX marks_count
    // new_part->marks_count = staled_part->marks_count;
    // new_part->columns = staled_part->columns;
    new_part->setPreparedIndex(staled_part->getIndex());
    new_part->has_bitmap = staled_part->has_bitmap.load();
    new_part->is_temp = true;

    String new_full_path = new_part->getFullPath();
#if USE_HDFS
    auto& fs = getDefaultHdfsFileSystem();
    if (fs->exists(new_full_path))
    {
        LOG_WARNING(log, "Removing old temporary directory " + new_full_path);
        fs->remove(new_full_path, true);
    }
    fs->createDirectories(new_full_path);
#endif

    /// write meta file
    uint128 meta_checksum {};
    String meta_file_name = new_full_path + "/meta";
    {
        std::unique_ptr<WriteBuffer> meta_out = createWriteBuffer(meta_file_name, type);
        HashingWriteBuffer meta_out_hashing(*meta_out);
        /// Meta file header
        /// TODO: fix
        // writeMetaFileHeader(new_part->marks_count, new_part->rows_count, meta_out_hashing);
        new_part->prepared_checksums->files.erase("count.txt");

        /// Meta files in low level storage
        String staled_full_path = staled_part->getFullPath();
        auto copy_meta_file = [&](const String & file_name)
        {
            size_t file_size = 0;
            String file_full_name = staled_full_path + file_name;
            /// Get files size that are not in checksum list by Poco
            if (file_name == "checksums.txt" || file_name == "columns.txt")
                file_size = Poco::File(file_full_name).getSize();
            else
                file_size = staled_part->prepared_checksums->files.at(file_name).file_size;
            std::unique_ptr<ReadBuffer> in = createReadBuffer(file_full_name, StorageType::Local);
            copyMetaFile(*in, meta_out_hashing, file_size);
            new_part->prepared_checksums->files.erase(file_name);
        };
        if (data.primary_key_columns.size())
            copy_meta_file("primary.idx");
        if (data.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            copy_meta_file("partition.dat");
            size_t minmax_idx_size = data.minmax_idx_column_types.size();
            for (size_t i = 0; i < minmax_idx_size; ++i)
            {
                String file_name = "minmax_" + escapeForFileName(data.minmax_idx_columns[i]) + ".idx";
                copy_meta_file(file_name);
            }
        }
        copy_meta_file("columns.txt");

        /// Data files offset
        size_t data_file_offset = MERGE_TREE_STORAGE_LEVEL_1_DATA_HEADER_SIZE;
        for (auto & file : new_part->prepared_checksums->files)
        {
            file.second.file_offset = data_file_offset;
            data_file_offset += file.second.file_size;
        }
        new_part->prepared_checksums->write(meta_out_hashing);

        /// Checksum
        meta_checksum = meta_out_hashing.getHash();
        writeMetaFileFooter(meta_checksum, *meta_out);
        meta_out->next();
    }

    /// Write data file
    String data_file_name = new_full_path + "/data";
    {
        std::unique_ptr<WriteBuffer> data_out = createWriteBuffer(data_file_name, type);
        writeDataFileHeader(*data_out);

        for (auto file : new_part->prepared_checksums->files)
        {
            String file_path = staled_part->getFullPath() + "/" + file.first;
            ReadBufferFromFile from(file_path);
            copyData(from, *data_out);
        }
        data_out->next();
    }

    new_part->modification_time = time(nullptr);
    new_part->bytes_on_disk = new_part->prepared_checksums->getTotalSizeOnDisk();

    check(new_part, meta_checksum);

    return new_part;
}

}
