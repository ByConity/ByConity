#pragma once

#include <Disks/IDisk.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/DeleteBitmapMeta.h>
#include <Transaction/TransactionCommon.h>
#include <Common/ThreadPool.h>

namespace DB
{

class AttachContext
{
public:
    // Operation type including the following four types:
    // 1. Attach from other table
    // 2. Attach detached partition from other table, only support move now
    // 3. Attach from own detached part/partition
    // 4. Attach from hdfs or s3 path
    enum OperationType
    {
        MOVE_FROM_STORAGE, // For S3, only meta pointer is moved
        MOVE_FROM_OTHER_DETACHED,
        MOVE_FROM_OWN_DETACHED,
        MOVE_FROM_PATH,

        COPY_FROM_STORAGE,
        COPY_FROM_OWN_DETACHED,
        // Actually not support now
        COPY_FROM_OTHER_DETACHED,
        COPY_FROM_PATH,
        // Only For S3
        MOVE_FROM_TASK
    };

    struct UndoRecord
    {
        UndoRecord(const DB::DiskPtr & from_disk_, String from_path_) : from_disk(from_disk_), from_path(from_path_) { }

        UndoRecord(
            const DB::DiskPtr & from_disk_, const DiskPtr & to_disk_,
            const String & name_, const String & from, const String & to, bool is_bitmap_ = false)
            : from_disk(from_disk_), to_disk(to_disk_), name(name_), from_path(from), to_path(to), is_bitmap(is_bitmap_)
        {
        }

        UndoRecord(const DB::DiskPtr & from_disk_, const DiskPtr & to_disk_,
            DataModelDeleteBitmapPtr delete_bitmap_model_, const String & from, const String & to, bool is_bitmap_ = true)
            : from_disk(from_disk_), to_disk(to_disk_)
            , delete_bitmap_model(delete_bitmap_model_), from_path(from), to_path(to), is_bitmap(is_bitmap_)
        {
        }

        DiskPtr from_disk;
        DiskPtr to_disk;
        // Used for write undo buffer.
        // For UndoResourceType::Part or UndoResourceType::DeleteBitmap, the path we have to give is relative to storage.
        // So we will use `name` or `delete_bitmap_model` to construct it rather than use `to_path` directly
        String name;
        DataModelDeleteBitmapPtr delete_bitmap_model;
        // Used for actual execution, move or copy from `from_path` to `to_path`. It's relative to disk.
        String from_path;
        String to_path;
        bool is_bitmap;
    };

    using UndoRecords = std::vector<UndoRecord>;

    struct TempResource
    {
        UndoRecords copy_records;
        UndoRecords rename_records;
    };

    AttachContext(const Context & query_ctx_, int pool_expand_thres, int max_thds, LoggerPtr log)
        : query_ctx(query_ctx_)
        , expand_thread_pool_threshold(pool_expand_thres)
        , max_worker_threads(max_thds)
        , new_txn(nullptr)
        , logger(log)
    {
    }

    void determineOperationType(const PartitionCommand & command, bool enable_copy_for_partition_operation);
    bool needCopyForS3() const
    {
        // For S3, only attach/replace partition from src_table needs copy, attach from detached don't need copy
        // Attach from S3 path only supports copy, no matter it's move or copy
        return operation_type == OperationType::COPY_FROM_STORAGE || operation_type == OperationType::COPY_FROM_PATH
            || operation_type == OperationType::MOVE_FROM_PATH;
    }

    void writeRenameRecord(UndoRecord & undo_record);
    void writeCopyRecord(UndoRecord & undo_record);
    // Record delete Meta files name to delete for attaching unique table parts
    void writeMetaFilesNameRecord(UndoRecord & undo_record);
    // Record relative part path in detached directory
    void writeDetachedPartRecord(UndoRecord & undo_record);

    // Method of summary, construct undoRecord outside and call this method
    void writeUndoRecord(UndoRecord & undo_record);
    void submitUndoRecordToKV(Catalog::Catalog & catalog, const StorageID & storage_id, const TxnTimestamp & txn_id);
    // execute copy or move record
    void executeOperation();

    void commit(bool has_exception = false);
    void rollback();

    // Get worker pool, argument is job number, if job_nums is large enough
    // it may reallocate worker pool
    ThreadPool & getWorkerPool(int job_nums);

    // For attach from other table's active partition, we may start a new transaction
    void setAdditionalTxn(const TransactionCnchPtr & txn) { new_txn = txn; }

    void setSourceDirectory(const String & dir) { src_directory = dir; }

    const Context & getQueryContext() { return query_ctx; }

    OperationType operation_type;

private:
    const Context & query_ctx;
    const int expand_thread_pool_threshold;
    const int max_worker_threads;

    TransactionCnchPtr new_txn;
    String src_directory;

    std::unique_ptr<ThreadPool> worker_pool;

    std::mutex mu;
    /// Temporary resource created during ATTACH, including temp dictionary, file movement records...
    std::map<String, TempResource> resources;
    std::map<String, TempResource> meta_files_to_delete;
    std::map<String, TempResource> detached_parts_to_delete;

    LoggerPtr logger;
};
}
