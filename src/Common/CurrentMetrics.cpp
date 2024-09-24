/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Common/StringUtils/StringUtils.h>
#include <Common/CurrentMetrics.h>


/// Available metrics. Add something here as you wish.
#define APPLY_FOR_METRICS(M) \
    M(Query, "Number of executing queries") \
    M(DefaultQuery, "Number of default queries") \
    M(InsertQuery, "Number of insert queries") \
    M(SystemQuery, "Number of system queries") \
    M(Merge, "Number of executing background merges") \
    M(Manipulation, "Number of execting manipulation tasks") \
    M(Consumer, "Number of consumer task") \
    M(Deduper, "Number of dedup task") \
    M(PartMutation, "Number of mutations (ALTER DELETE/UPDATE)") \
    M(ReplicatedFetch, "Number of data parts being fetched from replica") \
    M(ReplicatedSend, "Number of data parts being sent to replicas") \
    M(ReplicatedChecks, "Number of data parts checking for consistency") \
    M(BackgroundPoolTask, "Number of active tasks in BackgroundProcessingPool (merges, mutations, or replication queue bookkeeping)") \
    M(UniqueTableBackgroundPoolTask, \
      "Number of active tasks in BackgroundJobExecutor. This pool is used for doing merge task for unique table") \
    M(BackgroundFetchesPoolTask, "Number of active tasks in BackgroundFetchesPool") \
    M(BackgroundMovePoolTask, "Number of active tasks in BackgroundProcessingPool for moves") \
    M(BackgroundSchedulePoolTask, \
      "Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old " \
      "data parts, altering data parts, replica re-initialization, etc.") \
    M(BackgroundBufferFlushSchedulePoolTask, \
      "Number of active tasks in BackgroundBufferFlushSchedulePool. This pool is used for periodic Buffer flushes") \
    M(BackgroundDistributedSchedulePoolTask, \
      "Number of active tasks in BackgroundDistributedSchedulePool. This pool is used for distributed sends that is done in background.") \
    M(BackgroundMessageBrokerSchedulePoolTask, "Number of active tasks in BackgroundProcessingPool for message streaming") \
    M(BackgroundConsumeSchedulePoolTask, \
      "Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old " \
      "data parts, altering data parts, replica re-initialization, etc.") \
    M(BackgroundRestartSchedulePoolTask, \
      "Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old " \
      "data parts, altering data parts, replica re-initialization, etc.") \
    M(BackgroundHaLogSchedulePoolTask, \
      "Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old " \
      "data parts, altering data parts, replica re-initialization, etc.") \
    M(BackgroundMutationSchedulePoolTask, \
      "Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old " \
      "data parts, altering data parts, replica re-initialization, etc.") \
    M(BackgroundLocalSchedulePoolTask, \
      "Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old " \
      "data parts, altering data parts, replica re-initialization, etc.") \
    M(BackgroundMergeSelectSchedulePoolTask, \
      "Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old " \
      "data parts, altering data parts, replica re-initialization, etc.") \
    M(BackgroundUniqueTableSchedulePoolTask, \
      "Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old " \
      "data parts, altering data parts, replica re-initialization, etc.") \
    M(BackgroundMemoryTableSchedulePoolTask, \
      "Number of active tasks in BackgroundSchedulePool. This pool is used for memory table threads, etc.") \
    M(BackgroundCNCHTopologySchedulePoolTask, \
      "Number of active tasks in BackgroundSchedulePool. This pool is used for topology related background threads.") \
    M(BackgroundPartsMetricsSchedulePoolTask, \
      "Number of active tasks in BackgroundSchedulePool. This pool is used for parts metrics related background threads.") \
    M(BackgroundGCSchedulePoolTask, "Number of active tasks in BackgroundGCSchedulePool. This pool is used for data removing related background threads.") \
    M(BackgroundQueueManagerSchedulePoolTask, \
      "Number of active tasks in BackgroundSchedulePool. This pool is used for queue manager threads.") \
    M(BackgroundBspGCSchedulePoolTask, "Number of active tasks in BspGCSchedulePool. This pool is used for bsp gc task.") \
    M(BackgroundVWPoolSchedulePoolTask, \
      "Number of active tasks in BackgroundVWPoolSchedulePoolTask. This pool is used for update worker groups.") \
    M(CacheDictionaryUpdateQueueBatches, "Number of 'batches' (a set of keys) in update queue in CacheDictionaries.") \
    M(CacheDictionaryUpdateQueueKeys, "Exact number of keys in update queue in CacheDictionaries.") \
    M(DiskSpaceReservedForMerge, \
      "Disk space reserved for currently running background merges. It is slightly more than the total size of currently merging parts.") \
    M(DistributedSend, \
      "Number of connections to remote servers sending data that was INSERTed into Distributed tables. Both synchronous and asynchronous " \
      "mode.") \
    M(QueryPreempted, "Number of queries that are stopped and waiting due to 'priority' setting.") \
    M(TCPConnection, \
      "Number of connections to TCP server (clients with native interface), also included server-server distributed query connections") \
    M(MySQLConnection, "Number of client connections using MySQL protocol") \
    M(HTTPConnection, "Number of connections to HTTP server") \
    M(InterserverConnection, "Number of connections from other replicas to fetch parts") \
    M(PostgreSQLConnection, "Number of client connections using PostgreSQL protocol") \
    M(OpenFileForRead, "Number of files open for reading") \
    M(OpenFileForWrite, "Number of files open for writing") \
    M(Read, "Number of read (read, pread, io_getevents, etc.) syscalls in fly") \
    M(RemoteRead, "Number of read with remote reader in fly") \
    M(Write, "Number of write (write, pwrite, io_getevents, etc.) syscalls in fly") \
    M(NetworkReceive, \
      "Number of threads receiving data from network. Only ClickHouse-related network interaction is included, not by 3rd party " \
      "libraries.") \
    M(NetworkSend, \
      "Number of threads sending data to network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.") \
    M(SendScalars, "Number of connections that are sending data for scalars to remote servers.") \
    M(SendExternalTables, \
      "Number of connections that are sending data for external tables to remote servers. External tables are used to implement GLOBAL " \
      "IN and GLOBAL JOIN operators with distributed subqueries.") \
    M(QueryThread, "Number of query processing threads") \
    M(LeaderReplica, \
      "Number of Replicated tables that are leaders. Leader replica is responsible for assigning merges, cleaning old blocks for " \
      "deduplications and a few more bookkeeping tasks. There may be no more than one leader across all replicas at one moment of time. " \
      "If there is no leader it will be elected soon or it indicate an issue.") \
    M(ReadonlyReplica, \
      "Number of Replicated tables that are currently in readonly state due to re-initialization after ZooKeeper session loss or due to " \
      "startup without ZooKeeper configured.") \
    M(MemoryTracking, "Total amount of memory (bytes) allocated by the server.") \
    M(MemoryTrackingForMerges, \
      "Total amount of memory (bytes) allocated for background merges. Included in MemoryTrackingInBackgroundProcessingPool. Note that " \
      "this value may include a drift when the memory was allocated in a context of background processing pool and freed in other " \
      "context or vice-versa. This happens naturally due to caches for tables indexes and doesn't indicate memory leaks.") \
    M(MemoryTrackingForConsuming, "Total amount of memory (bytes) used by consumer tasks") \
    M(EphemeralNode, "Number of ephemeral nodes hold in ZooKeeper.") \
    M(ZooKeeperSession, \
      "Number of sessions (connections) to ZooKeeper. Should be no more than one, because using more than one connection to ZooKeeper " \
      "may lead to bugs due to lack of linearizability (stale reads) that ZooKeeper consistency model allows.") \
    M(ZooKeeperWatch, "Number of watches (event subscriptions) in ZooKeeper.") \
    M(ZooKeeperRequest, "Number of requests to ZooKeeper in fly.") \
    M(DelayedInserts, \
      "Number of INSERT queries that are throttled due to high number of active data parts for partition in a MergeTree table.") \
    M(ContextLockWait, "Number of threads waiting for lock in Context. This is global lock.") \
    M(StorageBufferRows, "Number of rows in buffers of Buffer tables") \
    M(StorageBufferBytes, "Number of bytes in buffers of Buffer tables") \
    M(DictCacheRequests, "Number of requests in fly to data sources of dictionaries of cache type.") \
    M(Revision, "Revision of the server. It is a number incremented for every release or release candidate except patch releases.") \
    M(VersionInteger, \
      "Version of the server in a single integer number in base-1000. For example, version 11.22.33 is translated to 11022033.") \
    M(RWLockWaitingReaders, "Number of threads waiting for read on a table RWLock.") \
    M(RWLockWaitingWriters, "Number of threads waiting for write on a table RWLock.") \
    M(RWLockActiveReaders, "Number of threads holding read lock in a table RWLock.") \
    M(MergesMutationsMemoryTracking, "Total amount of memory (bytes) allocated by background tasks (merges and mutations).") \
    M(RWLockActiveWriters, "Number of threads holding write lock in a table RWLock.") \
    M(GlobalThread, "Number of threads in global thread pool.") \
    M(GlobalThreadActive, "Number of threads in global thread pool running a task.") \
    M(LocalThread, "Number of threads in local thread pools. The threads in local thread pools are taken from the global thread pool.") \
    M(LocalThreadActive, "Number of threads in local thread pools running a task.") \
    M(ThreadPoolRemoteFSReaderThreads, "Number of threads in the thread pool for remote_filesystem_read_method=threadpool.") \
    M(ThreadPoolRemoteFSReaderThreadsActive, "Number of threads in the thread pool for remote_filesystem_read_method=threadpool running a task.") \
    M(DistributedFilesToInsert, \
      "Number of pending files to process for asynchronous insertion into Distributed tables. Number of files for every shard is summed.") \
    M(BrokenDistributedFilesToInsert, \
      "Number of files for asynchronous insertion into Distributed tables that has been marked as broken. This metric will starts from 0 " \
      "on start. Number of files for every shard is summed.") \
    M(TablesToDropQueueSize, "Number of dropped tables, that are waiting for background data removal.") \
    M(MaxDDLEntryID, "Max processed DDL entry of DDLWorker.") \
    M(PartsTemporary, "The part is generating now, it is not in data_parts list.") \
    M(PartsPreCommitted, "The part is in data_parts, but not used for SELECTs.") \
    M(PartsCommitted, "Active data part, used by current and upcoming SELECTs.") \
    M(PartsOutdated, "Not active data part, but could be used by only current SELECTs, could be deleted after SELECTs finishes.") \
    M(PartsDeleting, "Not active data part with identity refcounter, it is deleting right now by a cleaner.") \
    M(PartsDeleteOnDestroy, "Part was moved to another disk and should be deleted in own destructor.") \
    M(PartsWide, "Wide parts.") \
    M(PartsCompact, "Compact parts.") \
    M(PartsInMemory, "In-memory parts.") \
    M(PartsCNCH, "CNCH parts.") \
    M(MMappedFiles, "Total number of mmapped files.") \
    M(MMappedFileBytes, "Sum size of mmapped file regions.")     \
    M(AsynchronousReadWait, "Number of threads waiting for asynchronous read.") \
    M(MemoryTrackingForExchange, "Total amount of memory (bytes) allocated for complex query exchange") \
    M(ParquetDecoderThreads, "Number of threads in the ParquetBlockInputFormat thread pool.") \
    M(ParquetDecoderThreadsActive, "Number of threads in the ParquetBlockInputFormat thread pool running a task.") \
\
    M(StorageMemoryRows, "Memory table input rows") \
    M(StorageMemoryBytes, "Memory table input bytes") \
\
    M(CnchSDRequestsUpstream, "Number of Service Discovery requests to upstream") \
\
    M(CnchTxnActiveTransactions, "Number of active transactions") \
    M(CnchTxnTransactionRecords, "Number of transaction records") \
\
    M(DiskCacheEvictQueueLength, "Length of disk cache evict queue") \
    M(DiskCacheRoughSingleStatsBucketSize, "Diskcache single statistics bucket size") \
\
    M(BackgroundDedupSchedulePoolTask, "Number of executing background dedup tasks") \
    M(CacheFileSegments, "Number of existing cache file segments") \
    M(CacheDetachedFileSegments, "Number of existing detached cache file segments") \
    M(FilesystemCacheSize, "Filesystem cache size in bytes") \
    M(FilesystemCacheElements, "Filesystem cache elements (file segments)") \
    M(TemporaryFilesForJoin, "Number of temporary files created for JOIN") \
    M(TemporaryFilesUnknown, "Number of temporary files created without known purpose") \
\
    M(SystemCnchPartsInfoRecalculationTasksSize, "Number of background threads in recalculate cnch_parts_info system table.") \
    M(SystemCnchTrashItemsInfoRecalculationTasksSize, "Number of background threads in recalculate cnch_trash_items_info system table.") \
\
    M(StorageS3Threads, "Number of threads in the StorageS3 thread pool.") \
    M(StorageS3ThreadsActive, "Number of threads in the StorageS3 thread pool running a task.") \
    M(StorageS3ThreadsScheduled, "Number of queued or active jobs in the StorageS3 thread pool.") \
\
    M(BigHashItemCount, "BigHash item count") \
    M(BigHashUsedSizeBytes, "BigHash used size in bytes") \
\
    M(BlockCacheUsedSizeBytes, "BlockCache used size in bytes") \
    M(BlockCacheHoleCount, "BlockCache hole count") \
    M(BlockCacheHoleBytesTotal, "BlockCache hole bytes total") \
\
    M(RegionManagerExternalFragmentation, "RegionManager external fragmentation") \
    M(RegionManagerNumInMemBufActive, "RegionManager number of in-memory active buffer") \
    M(RegionManagerNumInMemBufWaitingFlush, "RegionManager number of in-memory waiting flush buffer") \
\
    M(IOSchUserRequests, "UserRequests in io scheduler") \
    M(IOSchRawRequests, "RawRequests in deadline scheduler") \
\
    M(IOUringPendingEvents, "Number of io_uring SQEs waiting to be submitted") \
    M(IOUringInFlightEvents, "Number of io_uring SQEs in flight")

namespace CurrentMetrics
{
    #define M(NAME, DOCUMENTATION) extern const Metric NAME = __COUNTER__;
        APPLY_FOR_METRICS(M)
    #undef M
    constexpr Metric END = __COUNTER__;

    std::atomic<Value> values[END] {};    /// Global variable, initialized by zeros.

    const char * getName(Metric event)
    {
        static const char * strings[] =
        {
        #define M(NAME, DOCUMENTATION) #NAME,
            APPLY_FOR_METRICS(M)
        #undef M
        };

        return strings[event];
    }

    const DB::String getSnakeName(Metric event)
    {
        DB::String res{getName(event)};

        convertCamelToSnake(res);

        return res;
    }

    const char * getDocumentation(Metric event)
    {
        static const char * strings[] =
        {
        #define M(NAME, DOCUMENTATION) DOCUMENTATION,
            APPLY_FOR_METRICS(M)
        #undef M
        };

        return strings[event];
    }

    Metric end() { return END; }
}

#undef APPLY_FOR_METRICS
