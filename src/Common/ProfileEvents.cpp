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

#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>

/// Available events. Add something here as you wish.
#define APPLY_FOR_EVENTS(M) \
    M(Query, \
      "Number of queries to be interpreted and potentially executed. Does not include queries that failed to parse or were rejected due " \
      "to AST size limits, quota limits or limits on the number of simultaneously running queries. May include internal queries " \
      "initiated by ClickHouse itself. Does not count subqueries.") \
    M(SelectQuery, "Same as Query, but only for SELECT queries.") \
    M(InsertQuery, "Same as Query, but only for INSERT queries.") \
    M(SystemQuery, "Same as Query, but only for SYSTEM queries.") \
    M(DefaultQuery, "Same as Query, but only for DEFAULT queries.") \
    M(TimedOutQuery, "Number of queries that timed out") \
    M(FailedQuery, "Number of failed queries.") \
    M(FailedSelectQuery, "Same as FailedQuery, but only for SELECT queries.") \
    M(FailedInsertQuery, "Same as FailedQuery, but only for INSERT queries.") \
    M(InsufficientConcurrencyQuery, "Number of queries that are cancelled due to insufficient concurrency") \
    M(QueryTimeMicroseconds, "Total time of all queries.") \
    M(SelectQueryTimeMicroseconds, "Total time of SELECT queries.") \
    M(InsertQueryTimeMicroseconds, "Total time of INSERT queries.") \
    M(FileOpen, "Number of files opened.") \
    M(Seek, "Number of times the 'lseek' function was called.") \
    M(ReadBufferFromFileDescriptorRead, "Number of reads (read/pread) from a file descriptor. Does not include sockets.") \
    M(ReadBufferFromFileDescriptorReadFailed, "Number of times the read (read/pread) from a file descriptor have failed.") \
    M(ReadBufferFromFileDescriptorReadBytes, "Number of bytes read from file descriptors. If the file is compressed, this will show the compressed data size.") \
    M(WriteBufferFromFileDescriptorWrite, "Number of writes (write/pwrite) to a file descriptor. Does not include sockets.") \
    M(WriteBufferFromFileDescriptorWriteFailed, "Number of times the write (write/pwrite) to a file descriptor have failed.") \
    M(WriteBufferFromFileDescriptorWriteBytes, "Number of bytes written to file descriptors. If the file is compressed, this will show compressed data size.") \
    M(ReadBufferAIORead, "") \
    M(ReadBufferAIOReadBytes, "") \
    M(WriteBufferAIOWrite, "") \
    M(WriteBufferAIOWriteBytes, "") \
    M(ReadCompressedBytes, "Number of bytes (the number of bytes before decompression) read from compressed sources (files, network).") \
    M(CompressedReadBufferBlocks, "Number of compressed blocks (the blocks of data that are compressed independent of each other) read from compressed sources (files, network).") \
    M(CompressedReadBufferBytes, "Number of uncompressed bytes (the number of bytes after decompression) read from compressed sources (files, network).") \
    M(UncompressedCacheHits, "") \
    M(UncompressedCacheMisses, "") \
    M(UncompressedCacheWeightLost, "") \
    M(MMappedFileCacheHits, "Number of times a file has been found in the MMap cache (for the 'mmap' read_method), so we didn't have to mmap it again.") \
    M(MMappedFileCacheMisses, "Number of times a file has not been found in the MMap cache (for the 'mmap' read_method), so we had to mmap it again.") \
    M(OpenedFileCacheHits, "Number of times a file has been found in the opened file cache, so we didn't have to open it again.") \
    M(OpenedFileCacheMisses, "Number of times a file has been found in the opened file cache, so we had to open it again.") \
    M(OpenedFileCacheMicroseconds, "Amount of time spent executing OpenedFileCache methods.") \
    M(IOBufferAllocs, "") \
    M(IOBufferAllocBytes, "") \
    M(ArenaAllocChunks, "") \
    M(ArenaAllocBytes, "") \
    M(FunctionExecute, "") \
    M(TableFunctionExecute, "") \
    M(MarkCacheHits, "") \
    M(MarkCacheMisses, "") \
    M(QueryCacheHits, "") \
    M(QueryCacheMisses, "") \
    M(IntermediateResultCacheHits, "") \
    M(IntermediateResultCacheMisses, "") \
    M(IntermediateResultCacheRefuses, "") \
    M(IntermediateResultCacheWait, "") \
    M(IntermediateResultCacheUncompleted, "") \
    M(IntermediateResultCacheReadBytes, "") \
    M(IntermediateResultCacheWriteBytes, "") \
    M(PrimaryIndexCacheHits, "") \
    M(PrimaryIndexCacheMisses, "") \
    M(PrimaryIndexDiskCacheHits, "") \
    M(PrimaryIndexDiskCacheMisses, "") \
    M(LoadPrimaryIndexMicroseconds, "") \
    M(ChecksumsCacheHits, "") \
    M(ChecksumsCacheMisses, "") \
    M(LoadDataPartFooter, "Number of times load data part footer from remote.") \
    M(LoadChecksums, "Number of times load checksums.") \
    M(LoadRemoteChecksums, "Number of times load checksums from remote.") \
    M(LoadChecksumsMicroseconds, "Times spent loading checksums from remote.") \
    M(CreatedReadBufferOrdinary, "Number of times ordinary read buffer was created for reading data (while choosing among other read methods).") \
    M(CreatedReadBufferDirectIO, "Number of times a read buffer with O_DIRECT was created for reading data (while choosing among other read methods).") \
    M(CreatedReadBufferDirectIOFailed, "Number of times a read buffer with O_DIRECT was attempted to be created for reading data (while choosing among other read methods), but the OS did not allow it (due to lack of filesystem support or other reasons) and we fallen back to the ordinary reading method.") \
    M(CreatedReadBufferAIO, "") \
    M(CreatedReadBufferAIOFailed, "") \
    M(CreatedReadBufferMMap, "") \
    M(CreatedReadBufferMMapFailed, "") \
    M(DiskReadElapsedMicroseconds, "Total time spent waiting for read syscall. This include reads from page cache.") \
    M(DiskWriteElapsedMicroseconds, "Total time spent waiting for write syscall. This include writes to page cache.") \
    M(NetworkReceiveElapsedMicroseconds, \
      "Total time spent waiting for data to receive or receiving data from network. Only ClickHouse-related network interaction is " \
      "included, not by 3rd party libraries.") \
    M(NetworkSendElapsedMicroseconds, \
      "Total time spent waiting for data to send to network or sending data to network. Only ClickHouse-related network interaction is " \
      "included, not by 3rd party libraries..") \
    M(NetworkReceiveBytes, \
      "Total number of bytes received from network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.") \
    M(NetworkSendBytes, \
      "Total number of bytes send to network. Only ClickHouse-related network interaction is included, not by 3rd party libraries.") \
    M(LocalReadThrottlerBytes, "Bytes passed through 'max_local_read_bandwidth_for_server'/'max_local_read_bandwidth' throttler.") \
    M(LocalReadThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_local_read_bandwidth_for_server'/'max_local_read_bandwidth' throttling.") \
    M(ThrottlerSleepMicroseconds, "Total time a query was sleeping to conform the 'max_network_bandwidth' setting.") \
    \
    M(QueryMaskingRulesMatch, "Number of times query masking rules was successfully matched.") \
    \
    M(ReplicatedPartFetches, "Number of times a data part was downloaded from replica of a ReplicatedMergeTree table.") \
    M(ReplicatedPartFailedFetches, "Number of times a data part was failed to download from replica of a ReplicatedMergeTree table.") \
    M(ObsoleteReplicatedParts, "") \
    M(ReplicatedPartMerges, "Number of times data parts of ReplicatedMergeTree tables were successfully merged.") \
    M(ReplicatedPartFetchesOfMerged, "Number of times we prefer to download already merged part from replica of ReplicatedMergeTree table instead of performing a merge ourself (usually we prefer doing a merge ourself to save network traffic). This happens when we have not all source parts to perform a merge or when the data part is old enough.") \
    M(ReplicatedPartMutations, "") \
    M(ReplicatedPartChecks, "") \
    M(ReplicatedPartChecksFailed, "") \
    M(ReplicatedDataLoss, "Number of times a data part that we wanted doesn't exist on any replica (even on replicas that are offline right now). That data parts are definitely lost. This is normal due to asynchronous replication (if quorum inserts were not enabled), when the replica on which the data part was written was failed and when it became online after fail it doesn't contain that data part.") \
    \
    M(InsertedRows, "Number of rows INSERTed to all tables.") \
    M(InsertedBytes, "Number of bytes (uncompressed; for columns as they stored in memory) INSERTed to all tables.") \
    M(DelayedInserts, "Number of times the INSERT of a block to a MergeTree table was throttled due to high number of active data parts for partition.") \
    M(RejectedInserts, "Number of times the INSERT of a block to a MergeTree table was rejected with 'Too many parts' exception due to high number of active data parts for partition.") \
    M(DelayedInsertsMilliseconds, "Total number of milliseconds spent while the INSERT of a block to a MergeTree table was throttled due to high number of active data parts for partition.") \
    M(DistributedDelayedInserts, "Number of times the INSERT of a block to a Distributed table was throttled due to high number of pending bytes.") \
    M(DistributedRejectedInserts, "Number of times the INSERT of a block to a Distributed table was rejected with 'Too many bytes' exception due to high number of pending bytes.") \
    M(DistributedDelayedInsertsMilliseconds, "Total number of milliseconds spent while the INSERT of a block to a Distributed table was throttled due to high number of pending bytes.") \
    M(DuplicatedInsertedBlocks, "Number of times the INSERTed block to a ReplicatedMergeTree table was deduplicated.") \
    \
    M(ZooKeeperInit, "") \
    M(ZooKeeperTransactions, "") \
    M(ZooKeeperList, "") \
    M(ZooKeeperCreate, "") \
    M(ZooKeeperRemove, "") \
    M(ZooKeeperExists, "") \
    M(ZooKeeperGet, "") \
    M(ZooKeeperSet, "") \
    M(ZooKeeperMulti, "") \
    M(ZooKeeperCheck, "") \
    M(ZooKeeperClose, "") \
    M(ZooKeeperWatchResponse, "") \
    M(ZooKeeperUserExceptions, "") \
    M(ZooKeeperHardwareExceptions, "") \
    M(ZooKeeperOtherExceptions, "") \
    M(ZooKeeperWaitMicroseconds, "") \
    M(ZooKeeperBytesSent, "") \
    M(ZooKeeperBytesReceived, "") \
    \
    M(StorageMemoryFlush, "") \
    M(StorageMemoryErrorOnFlush, "") \
    M(StorageMemoryPassedAllMinThresholds, "") \
    M(StorageMemoryPassedTimeMaxThreshold, "") \
    M(StorageMemoryPassedRowsMaxThreshold, "") \
    M(StorageMemoryPassedBytesMaxThreshold, "") \
    \
    M(DistributedConnectionFailTry, "Total count when distributed connection fails with retry") \
    M(DistributedConnectionMissingTable, "") \
    M(DistributedConnectionStaleReplica, "") \
    M(DistributedConnectionFailAtAll, "Total count when distributed connection fails after all retries finished") \
    \
    M(HedgedRequestsChangeReplica, "Total count when timeout for changing replica expired in hedged requests.") \
    \
    M(CompileFunction, "Number of times a compilation of generated LLVM code (to create fused function for complex expressions) was initiated.") \
    M(CompiledFunctionExecute, "Number of times a compiled function was executed.") \
    M(CompileExpressionsMicroseconds, "Total time spent for compilation of expressions to LLVM code.") \
    M(CompileExpressionsBytes, "Number of bytes used for expressions compilation.") \
    \
    M(ExternalSortWritePart, "") \
    M(ExternalSortMerge, "") \
    M(ExternalAggregationWritePart, "") \
    M(ExternalAggregationMerge, "") \
    M(ExternalAggregationCompressedBytes, "") \
    M(ExternalAggregationUncompressedBytes, "") \
    \
    M(SlowRead, "Number of reads from a file that were slow. This indicate system overload. Thresholds are controlled by read_backoff_* settings.") \
    M(ReadBackoff, "Number of times the number of query processing threads was lowered due to slow reads.") \
    M(TaskStealCount, "Number of task stolen from another thread.") \
    \
    M(ReplicaYieldLeadership, \
      "Number of times Replicated table was yielded its leadership due to large replication lag relative to other replicas.") \
    M(ReplicaPartialShutdown, \
      "How many times Replicated table has to deinitialize its state due to session expiration in ZooKeeper. The state is reinitialized " \
      "every time when ZooKeeper is available again.") \
\
    M(SelectedParts, "Number of data parts selected to read from a MergeTree table.") \
    M(SelectedRanges, "Number of (non-adjacent) ranges in all data parts selected to read from a MergeTree table.") \
    M(SelectedMarks, "Number of marks (index granules) selected to read from a MergeTree table.") \
    M(SelectedRows, "Number of rows SELECTed from all tables.") \
    M(SelectedBytes, "Number of bytes (uncompressed; for columns as they stored in memory) SELECTed from all tables.") \
\
    M(PrewhereSelectedMarks, "Number of marks (index granules) selected to read from a MergeTree table after apply prewhere conditions, must <= SelectedMarks.") \
    M(PrewhereSelectedRows, "Number of rows selected after apply prewhere conditions, must be <= SelectedRows") \
\
    M(Manipulation, "Number of manipulations.") \
    M(ManipulationSuccess, "Number of success manipulations.") \
\
    M(IndexGranuleSeekTime, "The total time that skip index spent on seeking files.") \
    M(IndexGranuleReadTime, "The total time that skip index spent on reading files.") \
    M(IndexGranuleCalcTime, "The total time that skip index spent on calculating conditions.") \
\
    M(Merge, "Number of launched background merges.") \
    M(MergedRows, "Rows read for background merges. This is the number of rows before merge.") \
    M(MergedUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) that was read for background merges. This is the number before merge.") \
    M(MergesTimeMilliseconds, "Total time spent for background merges.")\
    \
    M(MergeTreeDataWriterRows, "Number of rows INSERTed to MergeTree tables.") \
    M(MergeTreeDataWriterUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) INSERTed to MergeTree tables.") \
    M(MergeTreeDataWriterCompressedBytes, "Bytes written to filesystem for data INSERTed to MergeTree tables.") \
    M(MergeTreeDataWriterBlocks, "Number of blocks INSERTed to MergeTree tables. Each block forms a data part of level zero.") \
    M(MergeTreeDataWriterBlocksAlreadySorted, "Number of blocks INSERTed to MergeTree tables that appeared to be already sorted.") \
    \
    M(MergeTreeDataProjectionWriterRows, "Number of rows INSERTed to MergeTree tables projection.") \
    M(MergeTreeDataProjectionWriterUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) INSERTed to MergeTree tables projection.") \
    M(MergeTreeDataProjectionWriterCompressedBytes, "Bytes written to filesystem for data INSERTed to MergeTree tables projection.") \
    M(MergeTreeDataProjectionWriterBlocks, "Number of blocks INSERTed to MergeTree tables projection. Each block forms a data part of level zero.") \
    M(MergeTreeDataProjectionWriterBlocksAlreadySorted, "Number of blocks INSERTed to MergeTree tables projection that appeared to be already sorted.") \
    \
    M(CannotRemoveEphemeralNode, "Number of times an error happened while trying to remove ephemeral node. This is not an issue, because our implementation of ZooKeeper library guarantee that the session will expire and the node will be removed.") \
    \
    M(RegexpCreated, "Compiled regular expressions. Identical regular expressions compiled just once and cached forever.") \
    M(ContextLock, "Number of times the lock of Context was acquired or tried to acquire. This is global lock.") \
    \
    M(StorageBufferFlush, "") \
    M(StorageBufferErrorOnFlush, "") \
    M(StorageBufferPassedAllMinThresholds, "") \
    M(StorageBufferPassedTimeMaxThreshold, "") \
    M(StorageBufferPassedRowsMaxThreshold, "") \
    M(StorageBufferPassedBytesMaxThreshold, "") \
    M(StorageBufferPassedTimeFlushThreshold, "") \
    M(StorageBufferPassedRowsFlushThreshold, "") \
    M(StorageBufferPassedBytesFlushThreshold, "") \
    M(StorageBufferLayerLockReadersWaitMilliseconds, "Time for waiting for Buffer layer during reading") \
    M(StorageBufferLayerLockWritersWaitMilliseconds, "Time for waiting free Buffer layer to write to (can be used to tune Buffer layers)") \
    \
    M(DictCacheKeysRequested, "") \
    M(DictCacheKeysRequestedMiss, "") \
    M(DictCacheKeysRequestedFound, "") \
    M(DictCacheKeysExpired, "") \
    M(DictCacheKeysNotFound, "") \
    M(DictCacheKeysHit, "") \
    M(DictCacheRequestTimeNs, "") \
    M(DictCacheRequests, "") \
    M(DictCacheLockWriteNs, "") \
    M(DictCacheLockReadNs, "") \
    \
    M(DistributedSyncInsertionTimeoutExceeded, "") \
    M(DataAfterMergeDiffersFromReplica, "") \
    M(DataAfterMutationDiffersFromReplica, "") \
    M(PolygonsAddedToPool, "") \
    M(PolygonsInPoolAllocatedBytes, "") \
    M(RWLockAcquiredReadLocks, "") \
    M(RWLockAcquiredWriteLocks, "") \
    M(RWLockReadersWaitMilliseconds, "") \
    M(RWLockWritersWaitMilliseconds, "") \
    M(DNSError, "Total count of errors in DNS resolution") \
    \
    M(RealTimeMicroseconds, "Total (wall clock) time spent in processing (queries and other tasks) threads (not that this is a sum).") \
    M(UserTimeMicroseconds, "Total time spent in processing (queries and other tasks) threads executing CPU instructions in user space. This include time CPU pipeline was stalled due to cache misses, branch mispredictions, hyper-threading, etc.") \
    M(SystemTimeMicroseconds, "Total time spent in processing (queries and other tasks) threads executing CPU instructions in OS kernel space. This include time CPU pipeline was stalled due to cache misses, branch mispredictions, hyper-threading, etc.") \
    M(SoftPageFaults, "") \
    M(HardPageFaults, "") \
    M(VoluntaryContextSwitches, "") \
    M(InvoluntaryContextSwitches, "") \
    \
    M(OSIOWaitMicroseconds, "Total time a thread spent waiting for a result of IO operation, from the OS point of view. This is real IO that doesn't include page cache.") \
    M(OSCPUWaitMicroseconds, "Total time a thread was ready for execution but waiting to be scheduled by OS, from the OS point of view.") \
    M(OSCPUVirtualTimeMicroseconds, "CPU time spent seen by OS. Does not include involuntary waits due to virtualization.") \
    M(OSReadBytes, "Number of bytes read from disks or block devices. Doesn't include bytes read from page cache. May include excessive data due to block size, readahead, etc.") \
    M(OSWriteBytes, "Number of bytes written to disks or block devices. Doesn't include bytes that are in page cache dirty pages. May not include data that was written by OS asynchronously.") \
    M(OSReadChars, "Number of bytes read from filesystem, including page cache.") \
    M(OSWriteChars, "Number of bytes written to filesystem, including page cache.") \
    \
    M(PerfCpuCycles, "Total cycles. Be wary of what happens during CPU frequency scaling.")  \
    M(PerfInstructions, "Retired instructions. Be careful, these can be affected by various issues, most notably hardware interrupt counts.") \
    M(PerfCacheReferences, "Cache accesses. Usually this indicates Last Level Cache accesses but this may vary depending on your CPU. This may include prefetches and coherency messages; again this depends on the design of your CPU.") \
    M(PerfCacheMisses, "Cache misses. Usually this indicates Last Level Cache misses; this is intended to be used in con‐junction with the PERFCOUNTHWCACHEREFERENCES event to calculate cache miss rates.") \
    M(PerfBranchInstructions, "Retired branch instructions. Prior to Linux 2.6.35, this used the wrong event on AMD processors.") \
    M(PerfBranchMisses, "Mispredicted branch instructions.") \
    M(PerfBusCycles, "Bus cycles, which can be different from total cycles.") \
    M(PerfStalledCyclesFrontend, "Stalled cycles during issue.") \
    M(PerfStalledCyclesBackend, "Stalled cycles during retirement.") \
    M(PerfRefCpuCycles, "Total cycles; not affected by CPU frequency scaling.") \
    \
    M(PerfCpuClock, "The CPU clock, a high-resolution per-CPU timer") \
    M(PerfTaskClock, "A clock count specific to the task that is running") \
    M(PerfContextSwitches, "Number of context switches") \
    M(PerfCpuMigrations, "Number of times the process has migrated to a new CPU") \
    M(PerfAlignmentFaults, "Number of alignment faults. These happen when unaligned memory accesses happen; the kernel can handle these but it reduces performance. This happens only on some architectures (never on x86).") \
    M(PerfEmulationFaults, "Number of emulation faults. The kernel sometimes traps on unimplemented instructions and emulates them for user space. This can negatively impact performance.") \
    M(PerfMinEnabledTime, "For all events, minimum time that an event was enabled. Used to track event multiplexing influence") \
    M(PerfMinEnabledRunningTime, "Running time for event with minimum enabled time. Used to track the amount of event multiplexing") \
    M(PerfDataTLBReferences, "Data TLB references") \
    M(PerfDataTLBMisses, "Data TLB misses") \
    M(PerfInstructionTLBReferences, "Instruction TLB references") \
    M(PerfInstructionTLBMisses, "Instruction TLB misses") \
    M(PerfLocalMemoryReferences, "Local NUMA node memory reads") \
    M(PerfLocalMemoryMisses, "Local NUMA node memory read misses") \
    M(PerfInequalConditionElapsedMicroseconds, "") \
    M(PerfInequalConditionGetRowMicroseconds, "") \
    M(PerfInequalConditionExecuteMicroseconds, "") \
    M(PerfInequalConditionAppendMicroseconds, "") \
    M(PerfJoinElapsedMicroseconds, "") \
    M(PerfFilterElapsedMicroseconds, "") \
\
    M(QueryCreateTablesMicroseconds, "") \
    M(QuerySendResourcesMicroseconds, "") \
    M(CloudTableDefinitionCacheHits, "") \
    M(CloudTableDefinitionCacheMisses, "") \
\
    M(CreatedHTTPConnections, "Total amount of created HTTP connections (closed or opened).") \
    \
    M(ThreadPoolReaderTaskMicroseconds, "Time spent getting the data in asynchronous reading") \
    M(ThreadPoolReaderScheduleMicroseconds, "Time spent waiting for scheduling.") \
    M(ThreadPoolReaderReadBytes, "Bytes read from a thread pool task in asynchronous reading") \
    M(ThreadPoolReaderSubmit, "The number of submit asynchronous reading tasks.") \
    \
    M(CannotWriteToWriteBufferDiscard, \
      "Number of stack traces dropped by query profiler or signal handler because pipe is full or cannot write to pipe.") \
    M(QueryProfilerSignalOverruns, \
      "Number of times we drop processing of a signal due to overrun plus the number of signals that OS has not delivered due to overrun.") \
    \
    M(CreatedLogEntryForMerge, "Successfully created log entry to merge parts in ReplicatedMergeTree.") \
    M(NotCreatedLogEntryForMerge, "Log entry to merge parts in ReplicatedMergeTree is not created due to concurrent log update by another replica.") \
    M(CreatedLogEntryForMutation, "Successfully created log entry to mutate parts in ReplicatedMergeTree.") \
    M(NotCreatedLogEntryForMutation, "Log entry to mutate parts in ReplicatedMergeTree is not created due to concurrent log update by another replica.") \
    \
    M(S3ReadMicroseconds, "Time of GET and HEAD requests to S3 storage.") \
    M(S3ReadRequestsCount, "Number of GET and HEAD requests to S3 storage.") \
    M(S3ReadRequestsErrors, "Number of non-throttling errors in GET and HEAD requests to S3 storage.") \
    M(S3ReadRequestsThrottling, "Number of 429 and 503 errors in GET and HEAD requests to S3 storage.") \
    M(S3ReadRequestsRedirects, "Number of redirects in GET and HEAD requests to S3 storage.") \
    \
    M(S3WriteMicroseconds, "Time of POST, DELETE, PUT and PATCH requests to S3 storage.") \
    M(S3WriteBytes, "Write bytes (outgoing) in POST, DELETE, PUT and PATCH requests to S3 storage.") \
    M(S3WriteRequestsCount, "Number of POST, DELETE, PUT and PATCH requests to S3 storage.") \
    M(S3WriteRequestsErrors, "Number of non-throttling errors in POST, DELETE, PUT and PATCH requests to S3 storage.") \
    M(S3WriteRequestsThrottling, "Number of 429 and 503 errors in POST, DELETE, PUT and PATCH requests to S3 storage.") \
    M(S3WriteRequestsRedirects, "Number of redirects in POST, DELETE, PUT and PATCH requests to S3 storage.") \
\
    M(S3DeleteObjects, "Number of S3 API DeleteObject(s) calls.") \
    M(S3CopyObject, "Number of S3 API CopyObject calls.") \
    M(S3ListObjects, "Number of S3 API ListObjects calls.") \
    M(S3HeadObject,  "Number of S3 API HeadObject calls.") \
    M(S3CreateMultipartUpload, "Number of S3 API CreateMultipartUpload calls.") \
    M(S3UploadPartCopy, "Number of S3 API UploadPartCopy calls.") \
    M(S3UploadPart, "Number of S3 API UploadPart calls.") \
    M(S3AbortMultipartUpload, "Number of S3 API AbortMultipartUpload calls.") \
    M(S3CompleteMultipartUpload, "Number of S3 API CompleteMultipartUpload calls.") \
    M(S3PutObject, "Number of S3 API PutObject calls.") \
    M(S3GetObject, "Number of S3 API GetObject calls.") \
\
    M(DiskS3DeleteObjects, "Number of DiskS3 API DeleteObject(s) calls.") \
    M(DiskS3CopyObject, "Number of DiskS3 API CopyObject calls.") \
    M(DiskS3ListObjects, "Number of DiskS3 API ListObjects calls.") \
    M(DiskS3HeadObject,  "Number of DiskS3 API HeadObject calls.") \
    M(DiskS3CreateMultipartUpload, "Number of DiskS3 API CreateMultipartUpload calls.") \
    M(DiskS3UploadPartCopy, "Number of DiskS3 API UploadPartCopy calls.") \
    M(DiskS3UploadPart, "Number of DiskS3 API UploadPart calls.") \
    M(DiskS3AbortMultipartUpload, "Number of DiskS3 API AbortMultipartUpload calls.") \
    M(DiskS3CompleteMultipartUpload, "Number of DiskS3 API CompleteMultipartUpload calls.") \
    M(DiskS3PutObject, "Number of DiskS3 API PutObject calls.") \
    M(DiskS3GetObject, "Number of DiskS3 API GetObject calls.") \
\
    M(SchemaInferenceCacheHits, "Number of times the requested source is found in schema cache") \
    M(SchemaInferenceCacheSchemaHits, "Number of times the schema is found in schema cache during schema inference") \
    M(SchemaInferenceCacheNumRowsHits, "Number of times the number of rows is found in schema cache during count from files") \
    M(SchemaInferenceCacheMisses, "Number of times the requested source is not in schema cache") \
    M(SchemaInferenceCacheSchemaMisses, "Number of times the requested source is in cache but the schema is not in cache during schema inference") \
    M(SchemaInferenceCacheNumRowsMisses, "Number of times the requested source is in cache but the number of rows is not in cache while count from files") \
    M(SchemaInferenceCacheEvictions, "Number of times a schema from cache was evicted due to overflow") \
    M(SchemaInferenceCacheInvalidations, "Number of times a schema in cache became invalid due to changes in data") \
    \
    M(QueryMemoryLimitExceeded, "Number of times when memory limit exceeded for query.") \
    M(MarkBitmapIndexReadMicroseconds, "Total time spent in reading mark bitmap index.") \
    M(RemoteFSReadMicroseconds, "Time of reading from remote filesystem.") \
    M(RemoteFSReadBytes, "Read bytes from remote filesystem.") \
    \
    M(RemoteFSSeeks, "Total number of seeks for async buffer") \
    M(RemoteFSPrefetchRequests, "Number of prefetches made with asynchronous reading from remote filesystem") \
    M(RemoteFSCancelledPrefetches, "Number of cancelled prefetches (because of seek)") \
    M(RemoteFSUnusedPrefetches, "Number of prefetches pending at buffer destruction") \
    M(RemoteFSPrefetchedReads, "Number of reads from prefetched buffer") \
    M(RemoteFSPrefetchTaskWait, "Number of waiting when reading from prefetched buffer") \
    M(RemoteFSPrefetchTaskNotWait, "Number of not waiting when reading from prefetched buffer") \
    M(RemoteFSPrefetchedBytes, "Number of bytes from prefetched buffer") \
    M(RemoteFSUnprefetchedReads, "Number of reads from unprefetched buffer") \
    M(RemoteFSUnprefetchedBytes, "Number of bytes from unprefetched buffer") \
    M(RemoteFSLazySeeks, "Number of lazy seeks") \
    M(RemoteFSSeeksWithReset, "Number of seeks which lead to a new connection") \
    M(RemoteFSSeeksOverUntilPosition, "Number of seeks which is greater than read_until_position") \
    M(RemoteFSBuffers, "Number of buffers created for asynchronous reading from remote filesystem") \
    M(RemoteFSAsynchronousReadWaitMicroseconds, "Time spent in waiting for asynchronous remote reads.") \
    M(RemoteFSSynchronousReadWaitMicroseconds, "Time spent in waiting for synchronous remote reads.") \
    M(ReusedDataPartReaders, "Number of reused data part reader.") \
    \
    M(SDRequest, "Number requests sent to SD") \
    M(SDRequestFailed, "Number requests sent to SD that failed") \
    M(SDRequestUpstream, "Number requests sent to SD upstream") \
    M(SDRequestUpstreamFailed, "Number requests sent to SD upstream that failed") \
    \
    M(WriteBufferFromHdfsWriteBytes, "")\
    M(HDFSWriteElapsedMicroseconds, "")\
    M(WriteBufferFromHdfsWrite, "")\
    M(WriteBufferFromHdfsWriteFailed, "")\
    M(HdfsConnect, "") \
    M(HdfsConnectMicroseconds, "") \
    M(HdfsRequestErrors, "") \
    M(HdfsFileOpen, "")\
    M(HdfsFileOpenMicroseconds, "")\
    M(HdfsExists, "") \
    M(HdfsList, "") \
    M(HdfsRename, "") \
    M(ReadBufferFromHdfsRead, "")\
    M(ReadBufferFromHdfsReadFailed, "")\
    M(ReadBufferFromHdfsReadBytes, "")\
    M(HDFSReadElapsedMicroseconds, "")\
    M(HDFSSeek, "")\
    M(HDFSSeekElapsedMicroseconds, "")\
    M(HdfsGetBlkLocMicroseconds, "Total number of milliseconds spent to call getBlockLocations") \
    M(HdfsSlowNodeCount, "Total number of milliseconds spent to call getBlockLocations") \
    M(HdfsFailedNodeCount, "Total number of milliseconds spent to call getBlockLocations") \
\
    M(DiskCacheGetMicroSeconds, "Total time for disk cache get operation") \
    M(DiskCacheAcquireStatsLock, "Total time for acquire table stats lock") \
    M(DiskCacheScheduleCacheTaskMicroseconds, "Total time for schedule disk cache task") \
    M(DiskCacheTaskDropCount, "Total drop count for schedule disk cache task") \
    M(DiskCacheUpdateStatsMicroSeconds, "Total time for update disk cache statistics") \
    M(DiskCacheGetMetaMicroSeconds, "Total time for disk cache get operations") \
    M(DiskCacheGetTotalOps, "Total count of disk cache get operations") \
    M(DiskCacheSetTotalOps, "Total count of disk cache set operations") \
    M(DiskCacheSetTotalBytes, "Total  of disk cache set operations") \
    M(DiskCacheDeviceBytesWritten, "Total bytes written of disk cache device") \
    M(DiskCacheDeviceBytesRead, "Total bytes read of disk cache device") \
    M(DiskCacheDeviceWriteIOErrors, "Total errors of disk cache device write io") \
    M(DiskCacheDeviceReadIOErrors, "Total errors of disk cache device read io") \
    M(DiskCacheDeviceWriteIOLatency, "Latency of disk cache device write io") \
    M(DiskCacheDeviceReadIOLatency, "Latency of disk cache device read io") \
\
    M(ParquetFileOpened, "Number of parquet file read") \
    M(ParquetReadRowGroups, "Number of row groups read by parquet reader") \
    M(ParquetReadRows, "Number of rows read by parquet native reader") \
    M(ParquetPrewhereSkippedPageRows, "Number of rows skipped by page filter") \
    M(ParquetPrewhereSkippedRows, "Number of rows skipped by parquet prewhere") \
    M(ParquetPrewhereSkippedPages, "Number of pages skipped by parquet prewhere") \
    M(ParquetGetDataPageElapsedMicroseconds, "Total elapsed time spent on getting data page when reading parquet, this should include fs read and decompression time") \
    M(ParquetDecodeColumnElapsedMicroseconds, "Total elapsed time spent on decoding columns when reading parquet") \
    M(ParquetDegradeDictionaryElapsedMicroseconds, "Total elapsed time spent on casting column when reading parquet") \
    M(ParquetColumnCastElapsedMicroseconds, "Total elapsed time spent on casting column when reading parquet") \
\
    M(CnchTxnAborted, "Total number of aborted transactions (excludes preempting transactions)") \
    M(CnchTxnCommitted, "Total number of committed transactions") \
    M(CnchTxnExpired, "Total number of expired transactions") \
    M(CnchTxnReadTxnCreated, "Total number of read only transaction created") \
    M(CnchTxnWriteTxnCreated, "Total number of write transaction created") \
    M(CnchTxnCommitV1Failed, "Number of commitV1 failures") \
    M(CnchTxnCommitV2Failed, "Number of commitV2 failures") \
    M(CnchTxnCommitV1ElapsedMilliseconds, "Total number of milliseconds spent to commitV1") \
    M(CnchTxnCommitV2ElapsedMilliseconds, "Total number of milliseconds spent to commitV2") \
    M(CnchTxnPrecommitElapsedMilliseconds, "Total number of milliseconds spent to preempt transactions") \
    M(CnchTxnCommitKVElapsedMilliseconds, "Total number of milliseconds spent to commit transaction in catalog") \
    M(CnchTxnCleanFailed, "Number of times clean a transaction was failed") \
    M(CnchTxnCleanElapsedMilliseconds, "Total number of milliseconds spent to clean transactions") \
    M(CnchTxnAllTransactionRecord, "Total number of transaction records") \
    M(CnchTxnFinishedTransactionRecord, "Total number of finished transaction records") \
    M(CnchTxnRecordCacheHits, "Total number of txn record hits in cache") \
    M(CnchTxnRecordCacheMisses, "Total number of txn record misses in cache") \
    M(CnchWriteDataElapsedMilliseconds, "") \
    M(CnchDumpParts, "") \
    M(CnchDumpPartsElapsedMilliseconds, "") \
    M(CnchDumpPartsBytes, "") \
\
    M(CnchLoadMarksRequests, "") \
    M(CnchLoadMarksBytes, "") \
    M(CnchLoadMarksMicroseconds, "") \
    M(CnchLoadMarksFromDiskCacheRequests, "") \
    M(CnchLoadMarksFromDiskCacheBytes, "") \
    M(CnchLoadMarksFromDiskCacheMicroseconds, "") \
\
    M(CnchPartAllocationSplits, \
      "Number of times part allocation has been split between using bucket number and hashing due to partially clustered bucket table") \
    M(CnchSendResourceRpcCallElapsedMilliseconds, "Total RPC time for send resource to all workers, exclude RPC wait time") \
    M(CnchSendResourceElapsedMilliseconds, "Total time for send resource to all workers") \
    M(CnchDiskCacheNodeUnLocalityParts, "Total count of un-locality disk cache part") \
    \
    M(ServerRpcRequest, "Total number of RPC requests to server") \
    M(ServerRpcElaspsedMicroseconds, "Total time of RPC requests to server, not contain async call join time") \
    M(WorkerRpcRequest, "Total number of RPC requests to worker") \
    M(WorkerRpcElaspsedMicroseconds, "Total time of RPC requests to worker, not contain async call join time") \
    M(CatalogRequest, "Total number of requests in Catalog") \
    M(CatalogElapsedMicroseconds, "Total time of requests in Catalog") \
    M(KvRpcRequest, "Total number of requests to KV") \
    M(KvRpcElapsedMicroseconds, "Total time of requests to KV, not contain SCAN fetch time") \
    \
    M(IntentLockElapsedMilliseconds, "Total time spent to acquire intent locks") \
    M(IntentLockWriteIntentElapsedMilliseconds, "Total time spent to write intents") \
    M(IntentLockPreemptionElapsedMilliseconds, "Total time spent to preempt conflict locks") \
    M(TsCacheCheckElapsedMilliseconds, "Total number of milliseconds spent to check in Timestamp cache") \
    M(TsCacheUpdateElapsedMilliseconds, "Total number of milliseconds spent to update in Timestamp cache") \
    M(CatalogConstructorSuccess, "") \
    M(CatalogConstructorFailed, "") \
    M(CatalogTime, "Total time spent getting data parts from Catalog") \
    M(PrunePartsTime, "Total time spent pruning data parts") \
    M(TotalPartitions, "Number of total partitions") \
    M(PrunedPartitions, "Number of pruned partitions") \
    M(UpdateTableStatisticsSuccess, "") \
    M(UpdateTableStatisticsFailed, "") \
    M(GetTableStatisticsSuccess, "") \
    M(GetTableStatisticsFailed, "") \
    M(RemoveTableStatisticsSuccess, "") \
    M(RemoveTableStatisticsFailed, "") \
    M(UpdateColumnStatisticsSuccess, "") \
    M(UpdateColumnStatisticsFailed, "") \
    M(GetColumnStatisticsSuccess, "") \
    M(GetColumnStatisticsFailed, "") \
    M(GetAllColumnStatisticsSuccess, "") \
    M(GetAllColumnStatisticsFailed, "") \
    M(RemoveColumnStatisticsSuccess, "") \
    M(RemoveColumnStatisticsFailed, "") \
    M(RemoveAllColumnStatisticsSuccess, "") \
    M(RemoveAllColumnStatisticsFailed, "") \
    M(UpdateStatisticsSettingsFailed, "") \
    M(UpdateStatisticsSettingsSuccess, "") \
    M(GetStatisticsSettingsFailed, "") \
    M(GetStatisticsSettingsSuccess, "") \
    M(UpdateSQLBindingFailed, "") \
    M(UpdateSQLBindingSuccess, "") \
    M(GetSQLBindingFailed, "") \
    M(GetSQLBindingSuccess, "") \
    M(GetSQLBindingsFailed, "") \
    M(GetSQLBindingsSuccess, "") \
    M(RemoveSQLBindingFailed, "") \
    M(RemoveSQLBindingSuccess, "") \
    M(UpdatePreparedStatementFailed, "") \
    M(UpdatePreparedStatementSuccess, "") \
    M(GetPreparedStatementFailed, "") \
    M(GetPreparedStatementSuccess, "") \
    M(GetPreparedStatementsFailed, "") \
    M(GetSPreparedStatementsSuccess, "") \
    M(RemovePreparedStatementFailed, "") \
    M(RemovePreparedStatementSuccess, "") \
    M(CreateDatabaseSuccess, "") \
    M(CreateDatabaseFailed, "") \
    M(GetDatabaseSuccess, "") \
    M(GetDatabaseFailed, "") \
    M(IsDatabaseExistsSuccess, "") \
    M(IsDatabaseExistsFailed, "") \
    M(DropDatabaseSuccess, "") \
    M(DropDatabaseFailed, "") \
    M(RenameDatabaseSuccess, "") \
    M(RenameDatabaseFailed, "") \
    M(AlterDatabaseSuccess, "") \
    M(AlterDatabaseFailed, "") \
    M(CreateSnapshotSuccess, "") \
    M(CreateSnapshotFailed, "") \
    M(RemoveSnapshotSuccess, "") \
    M(RemoveSnapshotFailed, "") \
    M(TryGetSnapshotSuccess, "") \
    M(TryGetSnapshotFailed, "") \
    M(GetAllSnapshotsSuccess, "") \
    M(GetAllSnapshotsFailed, "") \
    M(CreateBackupJobSuccess, "") \
    M(CreateBackupJobFailed, "") \
    M(UpdateBackupJobSuccess, "") \
    M(UpdateBackupJobFailed, "") \
    M(GetBackupJobSuccess, "") \
    M(GetBackupJobFailed, "") \
    M(GetAllBackupJobSuccess, "") \
    M(GetAllBackupJobFailed, "") \
    M(RemoveBackupJobSuccess, "") \
    M(RemoveBackupJobFailed, "") \
    M(CreateTableSuccess, "") \
    M(CreateTableFailed, "") \
    M(DropTableSuccess, "") \
    M(DropTableFailed, "") \
    M(CreateUDFSuccess, "") \
    M(CreateUDFFailed, "") \
    M(DropUDFSuccess, "") \
    M(DropUDFFailed, "") \
    M(DetachTableSuccess, "") \
    M(DetachTableFailed, "") \
    M(AttachTableSuccess, "") \
    M(AttachTableFailed, "") \
    M(IsTableExistsSuccess, "") \
    M(IsTableExistsFailed, "") \
    M(AlterTableSuccess, "") \
    M(AlterTableFailed, "") \
    M(RenameTableSuccess, "") \
    M(RenameTableFailed, "") \
    M(SetWorkerGroupForTableSuccess, "") \
    M(SetWorkerGroupForTableFailed, "") \
    M(GetTableSuccess, "") \
    M(GetTableFailed, "") \
    M(TryGetTableSuccess, "") \
    M(TryGetTableFailed, "") \
    M(TryGetTableByUUIDSuccess, "") \
    M(TryGetTableByUUIDFailed, "") \
    M(GetTableByUUIDSuccess, "") \
    M(GetTableByUUIDFailed, "") \
    M(GetTablesInDBSuccess, "") \
    M(GetTablesInDBFailed, "") \
    M(GetAllViewsOnSuccess, "") \
    M(GetAllViewsOnFailed, "") \
    M(SetTableActivenessSuccess, "") \
    M(SetTableActivenessFailed, "") \
    M(GetTableActivenessSuccess, "") \
    M(GetTableActivenessFailed, "") \
    M(GetServerDataPartsInPartitionsSuccess, "") \
    M(GetServerDataPartsInPartitionsFailed, "") \
    M(GetAllServerDataPartsSuccess, "") \
    M(GetAllServerDataPartsFailed, "") \
    M(GetAllServerDataPartsWithDBMSuccess, "") \
    M(GetAllServerDataPartsWithDBMFailed, "") \
    M(GetDataPartsByNamesSuccess, "") \
    M(GetDataPartsByNamesFailed, "") \
    M(GetStagedDataPartsByNamesSuccess, "") \
    M(GetStagedDataPartsByNamesFailed, "") \
    M(GetAllDeleteBitmapsSuccess, "") \
    M(GetAllDeleteBitmapsFailed, "") \
    M(GetStagedPartsSuccess, "") \
    M(GetStagedPartsFailed, "") \
    M(GetDeleteBitmapsInPartitionsSuccess, "") \
    M(GetDeleteBitmapsInPartitionsFailed, "") \
    M(GetDeleteBitmapsFromCacheInPartitionsSuccess, "") \
    M(GetDeleteBitmapsFromCacheInPartitionsFailed, "") \
    M(GetDeleteBitmapByKeysSuccess, "") \
    M(GetDeleteBitmapByKeysFailed, "") \
    M(AddDeleteBitmapsSuccess, "") \
    M(AddDeleteBitmapsFailed, "") \
    M(RemoveDeleteBitmapsSuccess, "") \
    M(RemoveDeleteBitmapsFailed, "") \
    M(FinishCommitSuccess, "") \
    M(FinishCommitFailed, "") \
    M(GetKafkaOffsetsVoidSuccess, "") \
    M(GetKafkaOffsetsVoidFailed, "") \
    M(GetKafkaOffsetsTopicPartitionListSuccess, "") \
    M(GetKafkaOffsetsTopicPartitionListFailed, "") \
    M(ClearOffsetsForWholeTopicSuccess, "") \
    M(ClearOffsetsForWholeTopicFailed, "") \
    M(SetTransactionForKafkaConsumerSuccess, "") \
    M(SetTransactionForKafkaConsumerFailed, "") \
    M(GetTransactionForKafkaConsumerSuccess, "") \
    M(GetTransactionForKafkaConsumerFailed, "") \
    M(ClearKafkaTransactionsForTableSuccess, "") \
    M(ClearKafkaTransactionsForTableFailed, "") \
    M(DropAllPartSuccess, "") \
    M(DropAllPartFailed, "") \
    M(GetPartitionListSuccess, "") \
    M(GetPartitionListFailed, "") \
    M(GetPartitionsFromMetastoreSuccess, "") \
    M(GetPartitionsFromMetastoreFailed, "") \
    M(GetPartitionIDsSuccess, "") \
    M(GetPartitionIDsFailed, "") \
    M(CreateDictionarySuccess, "") \
    M(CreateDictionaryFailed, "") \
    M(GetCreateDictionarySuccess, "") \
    M(GetCreateDictionaryFailed, "") \
    M(DropDictionarySuccess, "") \
    M(DropDictionaryFailed, "") \
    M(DetachDictionarySuccess, "") \
    M(DetachDictionaryFailed, "") \
    M(AttachDictionarySuccess, "") \
    M(AttachDictionaryFailed, "") \
    M(GetDictionariesInDBSuccess, "") \
    M(GetDictionariesInDBFailed, "") \
    M(GetDictionarySuccess, "") \
    M(GetDictionaryFailed, "") \
    M(IsDictionaryExistsSuccess, "") \
    M(IsDictionaryExistsFailed, "") \
    M(TryLockPartInKVSuccess, "") \
    M(TryLockPartInKVFailed, "") \
    M(UnLockPartInKVSuccess, "") \
    M(UnLockPartInKVFailed, "") \
    M(TryResetAndLockConflictPartsInKVSuccess, "") \
    M(TryResetAndLockConflictPartsInKVFailed, "") \
    M(CreateTransactionRecordSuccess, "") \
    M(CreateTransactionRecordFailed, "") \
    M(RemoveTransactionRecordSuccess, "") \
    M(RemoveTransactionRecordFailed, "") \
    M(RemoveTransactionRecordsSuccess, "") \
    M(RemoveTransactionRecordsFailed, "") \
    M(GetTransactionRecordSuccess, "") \
    M(GetTransactionRecordFailed, "") \
    M(TryGetTransactionRecordSuccess, "") \
    M(TryGetTransactionRecordFailed, "") \
    M(SetTransactionRecordSuccess, "") \
    M(SetTransactionRecordFailed, "") \
    M(SetTransactionRecordWithRequestsSuccess, "") \
    M(SetTransactionRecordWithRequestsFailed, "") \
    M(SetTransactionRecordCleanTimeSuccess, "") \
    M(SetTransactionRecordCleanTimeFailed, "") \
    M(SetTransactionRecordStatusWithOffsetsSuccess, "") \
    M(SetTransactionRecordStatusWithOffsetsFailed, "") \
    M(RollbackTransactionSuccess, "") \
    M(RollbackTransactionFailed, "") \
    M(WriteIntentsSuccess, "") \
    M(WriteIntentsFailed, "") \
    M(TryResetIntentsIntentsToResetSuccess, "") \
    M(TryResetIntentsIntentsToResetFailed, "") \
    M(TryResetIntentsOldIntentsSuccess, "") \
    M(TryResetIntentsOldIntentsFailed, "") \
    M(ClearIntentsSuccess, "") \
    M(ClearIntentsFailed, "") \
    M(WritePartsSuccess, "") \
    M(WritePartsFailed, "") \
    M(SetCommitTimeSuccess, "") \
    M(SetCommitTimeFailed, "") \
    M(ClearPartsSuccess, "") \
    M(ClearPartsFailed, "") \
    M(WriteUndoBufferConstResourceSuccess, "") \
    M(WriteUndoBufferConstResourceFailed, "") \
    M(WriteUndoBufferNoConstResourceSuccess, "") \
    M(WriteUndoBufferNoConstResourceFailed, "") \
    M(ClearUndoBufferSuccess, "") \
    M(ClearUndoBufferFailed, "") \
    M(GetUndoBufferSuccess, "") \
    M(GetUndoBufferFailed, "") \
    M(GetAllUndoBufferSuccess, "") \
    M(GetAllUndoBufferFailed, "") \
    M(GetUndoBufferIteratorSuccess, "") \
    M(GetUndoBufferIteratorFailed, "") \
    M(GetUndoBuffersWithKeysSuccess, "") \
    M(GetUndoBuffersWithKeysFailed, "") \
    M(ClearUndoBuffersByKeysSuccess, "") \
    M(ClearUndoBuffersByKeysFailed, "") \
    M(GetTransactionRecordsSuccess, "") \
    M(GetTransactionRecordsFailed, "") \
    M(GetTransactionRecordsTxnIdsSuccess, "") \
    M(GetTransactionRecordsTxnIdsFailed, "") \
    M(GetTransactionRecordsForGCSuccess, "") \
    M(GetTransactionRecordsForGCFailed, "") \
    M(ClearZombieIntentSuccess, "") \
    M(ClearZombieIntentFailed, "") \
    M(WriteFilesysLockSuccess, "") \
    M(WriteFilesysLockFailed, "") \
    M(GetFilesysLockSuccess, "") \
    M(GetFilesysLockFailed, "") \
    M(ClearFilesysLockDirSuccess, "") \
    M(ClearFilesysLockDirFailed, "") \
    M(ClearFilesysLockTxnIdSuccess, "") \
    M(ClearFilesysLockTxnIdFailed, "") \
    M(GetAllFilesysLockSuccess, "") \
    M(GetAllFilesysLockFailed, "") \
    M(InsertTransactionSuccess, "") \
    M(InsertTransactionFailed, "") \
    M(RemoveTransactionSuccess, "") \
    M(RemoveTransactionFailed, "") \
    M(GetActiveTransactionsSuccess, "") \
    M(GetActiveTransactionsFailed, "") \
    M(UpdateServerWorkerGroupSuccess, "") \
    M(UpdateServerWorkerGroupFailed, "") \
    M(GetWorkersInWorkerGroupSuccess, "") \
    M(GetWorkersInWorkerGroupFailed, "") \
    M(GetTableHistoriesSuccess, "") \
    M(GetTableHistoriesFailed, "") \
    M(GetTablesByIDSuccess, "") \
    M(GetTablesByIDFailed, "") \
    M(GetAllDataBasesSuccess, "") \
    M(GetAllDataBasesFailed, "") \
    M(GetAllTablesSuccess, "") \
    M(GetAllTablesFailed, "") \
    M(GetTrashTableIDIteratorSuccess, "") \
    M(GetTrashTableIDIteratorFailed, "") \
    M(GetAllUDFsSuccess, "") \
    M(GetAllUDFsFailed, "") \
    M(GetUDFByNameSuccess, "") \
    M(GetUDFByNameFailed, "") \
    M(GetTrashTableIDSuccess, "") \
    M(GetTrashTableIDFailed, "") \
    M(GetTablesInTrashSuccess, "") \
    M(GetTablesInTrashFailed, "") \
    M(GetDatabaseInTrashSuccess, "") \
    M(GetDatabaseInTrashFailed, "") \
    M(GetAllTablesIDSuccess, "") \
    M(GetAllTablesIDFailed, "") \
    M(GetTablesIDByTenantSuccess, "") \
    M(GetTablesIDByTenantFailed, "") \
    M(GetTableIDByNameSuccess, "") \
    M(GetTableIDByNameFailed, "") \
    M(GetTableIDsByNamesSuccess, "") \
    M(GetTableIDsByNamesFailed, "") \
    M(GetAllWorkerGroupsSuccess, "") \
    M(GetAllWorkerGroupsFailed, "") \
    M(GetAllDictionariesSuccess, "") \
    M(GetAllDictionariesFailed, "") \
    M(ClearDatabaseMetaSuccess, "") \
    M(ClearDatabaseMetaFailed, "") \
    M(ClearTableMetaForGCSuccess, "") \
    M(ClearTableMetaForGCFailed, "") \
    M(ClearDataPartsMetaSuccess, "") \
    M(ClearDataPartsMetaFailed, "") \
    M(ClearStagePartsMetaSuccess, "") \
    M(ClearStagePartsMetaFailed, "") \
    M(ClearDataPartsMetaForTableSuccess, "") \
    M(ClearDataPartsMetaForTableFailed, "") \
    M(ClearDeleteBitmapsMetaForTableSuccess, "") \
    M(ClearDeleteBitmapsMetaForTableFailed, "") \
    M(GetSyncListSuccess, "") \
    M(GetSyncListFailed, "") \
    M(ClearSyncListSuccess, "") \
    M(ClearSyncListFailed, "") \
    M(GetServerPartsByCommitTimeSuccess, "") \
    M(GetServerPartsByCommitTimeFailed, "") \
    M(CreateRootPathSuccess, "") \
    M(CreateRootPathFailed, "") \
    M(DeleteRootPathSuccess, "") \
    M(DeleteRootPathFailed, "") \
    M(GetAllRootPathSuccess, "") \
    M(GetAllRootPathFailed, "") \
    M(CreateMutationSuccess, "") \
    M(CreateMutationFailed, "") \
    M(RemoveMutationSuccess, "") \
    M(RemoveMutationFailed, "") \
    M(GetAllMutationsStorageIdSuccess, "") \
    M(GetAllMutationsStorageIdFailed, "") \
    M(GetAllMutationsSuccess, "") \
    M(GetAllMutationsFailed, "") \
    M(SetTableClusterStatusSuccess, "") \
    M(SetTableClusterStatusFailed, "") \
    M(GetTableClusterStatusSuccess, "") \
    M(GetTableClusterStatusFailed, "") \
    M(IsTableClusteredSuccess, "") \
    M(IsTableClusteredFailed, "") \
    M(SetTablePreallocateVWSuccess, "") \
    M(SetTablePreallocateVWFailed, "") \
    M(GetTablePreallocateVWSuccess, "") \
    M(GetTablePreallocateVWFailed, "") \
    M(GetTablePartitionMetricsSuccess, "") \
    M(GetTablePartitionMetricsFailed, "") \
    M(GetPartitionMetricsFromMetastoreSuccess, "") \
    M(GetPartitionMetricsFromMetastoreFailed, "") \
    M(UpdateTopologiesSuccess, "") \
    M(UpdateTopologiesFailed, "") \
    M(GetTopologiesSuccess, "") \
    M(GetTopologiesFailed, "") \
    M(GetTrashDBVersionsSuccess, "") \
    M(GetTrashDBVersionsFailed, "") \
    M(UndropDatabaseSuccess, "") \
    M(UndropDatabaseFailed, "") \
    M(GetTrashTableVersionsSuccess, "") \
    M(GetTrashTableVersionsFailed, "") \
    M(UndropTableSuccess, "") \
    M(UndropTableFailed, "") \
    M(UpdateResourceGroupSuccess, "") \
    M(UpdateResourceGroupFailed, "") \
    M(GetResourceGroupSuccess, "") \
    M(GetResourceGroupFailed, "") \
    M(RemoveResourceGroupSuccess, "") \
    M(RemoveResourceGroupFailed, "") \
    M(GetInsertionLabelKeySuccess, "") \
    M(GetInsertionLabelKeyFailed, "") \
    M(PrecommitInsertionLabelSuccess, "") \
    M(PrecommitInsertionLabelFailed, "") \
    M(CommitInsertionLabelSuccess, "") \
    M(CommitInsertionLabelFailed, "") \
    M(TryCommitInsertionLabelSuccess, "") \
    M(TryCommitInsertionLabelFailed, "") \
    M(AbortInsertionLabelSuccess, "") \
    M(AbortInsertionLabelFailed, "") \
    M(GetInsertionLabelSuccess, "") \
    M(GetInsertionLabelFailed, "") \
    M(RemoveInsertionLabelSuccess, "") \
    M(RemoveInsertionLabelFailed, "") \
    M(RemoveInsertionLabelsSuccess, "") \
    M(RemoveInsertionLabelsFailed, "") \
    M(ScanInsertionLabelsSuccess, "") \
    M(ScanInsertionLabelsFailed, "") \
    M(ClearInsertionLabelsSuccess, "") \
    M(ClearInsertionLabelsFailed, "") \
    M(CreateVirtualWarehouseSuccess, "") \
    M(CreateVirtualWarehouseFailed, "") \
    M(AlterVirtualWarehouseSuccess, "") \
    M(AlterVirtualWarehouseFailed, "") \
    M(TryGetVirtualWarehouseSuccess, "") \
    M(TryGetVirtualWarehouseFailed, "") \
    M(ScanVirtualWarehousesSuccess, "") \
    M(ScanVirtualWarehousesFailed, "") \
    M(DropVirtualWarehouseSuccess, "") \
    M(DropVirtualWarehouseFailed, "") \
    M(CreateWorkerGroupSuccess, "") \
    M(CreateWorkerGroupFailed, "") \
    M(UpdateWorkerGroupSuccess, "") \
    M(UpdateWorkerGroupFailed, "") \
    M(TryGetWorkerGroupSuccess, "") \
    M(TryGetWorkerGroupFailed, "") \
    M(ScanWorkerGroupsSuccess, "") \
    M(ScanWorkerGroupsFailed, "") \
    M(DropWorkerGroupSuccess, "") \
    M(DropWorkerGroupFailed, "") \
    M(GetNonHostUpdateTimestampFromByteKVSuccess, "") \
    M(GetNonHostUpdateTimestampFromByteKVFailed, "") \
    M(MaskingPolicyExistsSuccess, "") \
    M(MaskingPolicyExistsFailed, "") \
    M(GetMaskingPoliciesSuccess, "") \
    M(GetMaskingPoliciesFailed, "") \
    M(PutMaskingPolicySuccess, "") \
    M(PutMaskingPolicyFailed, "") \
    M(TryGetMaskingPolicySuccess, "") \
    M(TryGetMaskingPolicyFailed, "") \
    M(GetMaskingPolicySuccess, "") \
    M(GetMaskingPolicyFailed, "") \
    M(GetAllMaskingPolicySuccess, "") \
    M(GetAllMaskingPolicyFailed, "") \
    M(GetMaskingPolicyAppliedTablesSuccess, "") \
    M(GetMaskingPolicyAppliedTablesFailed, "") \
    M(GetAllMaskingPolicyAppliedTablesSuccess, "") \
    M(GetAllMaskingPolicyAppliedTablesFailed, "") \
    M(DropMaskingPoliciesSuccess, "") \
    M(DropMaskingPoliciesFailed, "") \
    M(IsHostServerSuccess, "") \
    M(IsHostServerFailed, "") \
    M(CnchDataPartCacheHits, "")\
    M(CnchDataPartCacheMisses, "")\
    M(CnchDataDeleteBitmapCacheHits, "")\
    M(CnchDataDeleteBitmapCacheMisses, "")\
    M(CnchReadSizeFromDiskCache, "") \
    M(CnchReadSizeFromRemote, "") \
    M(CnchReadDataMicroSeconds,"") \
    M(CnchAddStreamsElapsedMilliseconds,"") \
    M(CnchAddStreamsParallelTasks,"") \
    M(CnchAddStreamsParallelElapsedMilliseconds,"") \
    M(CnchAddStreamsSequentialTasks,"") \
    M(CnchAddStreamsSequentialElapsedMilliseconds,"") \
    M(SetBGJobStatusSuccess, "") \
    M(SetBGJobStatusFailed, "") \
    M(GetBGJobStatusSuccess, "") \
    M(GetBGJobStatusFailed, "") \
    M(GetBGJobStatusesSuccess, "") \
    M(GetBGJobStatusesFailed, "") \
    M(DropBGJobStatusSuccess, "") \
    M(DropBGJobStatusFailed, "") \
    M(GetTableTrashItemsMetricsDataFromMetastoreSuccess, "") \
    M(GetTableTrashItemsMetricsDataFromMetastoreFailed, "") \
    M(GetPartsInfoMetricsSuccess, "") \
    M(GetPartsInfoMetricsFailed, "") \
    M(PutSensitiveResourceSuccess, "") \
    M(PutSensitiveResourceFailed, "") \
    M(GetSensitiveResourceSuccess, "") \
    M(GetSensitiveResourceFailed, "") \
    M(PutAccessEntitySuccess, "") \
    M(PutAccessEntityFailed, "") \
    M(TryGetAccessEntitySuccess, "") \
    M(TryGetAccessEntityFailed, "") \
    M(GetAllAccessEntitySuccess, "") \
    M(GetAllAccessEntityFailed, "") \
    M(DropAccessEntitySuccess, "") \
    M(DropAccessEntityFailed, "") \
    M(TryGetAccessEntityNameSuccess, "") \
    M(TryGetAccessEntityNameFailed, "") \
\
    M(PartsToAttach, "") \
    M(NumOfRowsToAttach, "") \
    M(VWQueueMilliseconds, "Total time spent to wait in virtual warehouse queue") \
    M(VWQueueType, "Which type of virtual warehouse queue") \
    M(ScheduleTimeMilliseconds, "Total time spent to schedule plan segment") \
    \
    M(ScheduledDedupTaskNumber, "Total number of scheduled dedup task") \
    M(DeleteBitmapCacheHit, "Total number of times to hit the cache to get delete bitmap for a part") \
    M(DeleteBitmapCacheMiss, "Total number of times to miss the cache to get delete bitmap for a part") \
    M(UniqueKeyIndexMetaCacheHit, "Total number of times to hit the unique key index cache") \
    M(UniqueKeyIndexMetaCacheMiss, "Total number of times to miss the unique key index cache") \
    M(UniqueKeyIndexBlockCacheHit, "Total number of times to hit the unique key index block cache") \
    M(UniqueKeyIndexBlockCacheMiss, "Total number of times to miss the unique key index block cache") \
    \
    M(AllWorkerSize, "Total worker size of worker group") \
    M(HealthWorkerSize, "Number of worker which can execute any type plan segment") \
    M(HeavyLoadWorkerSize, "Number of worker which can only execute source plan segment") \
    M(SourceOnlyWorkerSize, "Number of worker which can only execute source plan segment") \
    M(UnhealthWorkerSize, "Number of unhealthy worker") \
    M(NotConnectedWorkerSize, "Number of not connected worker size") \
    M(SelectHealthWorkerMilliSeconds, "Total time for select health worker") \
    \
    M(PreloadSubmitTotalOps, "Total count of preload submit by writer node") \
    M(PreloadSendTotalOps, "Total count of preload send by server node") \
    M(PreloadExecTotalOps, "Total count of preload run by reader node") \
\
    M(WriteBufferFromS3WriteMicroseconds, "Time spent on writing to S3.") \
    M(WriteBufferFromS3WriteBytes, "Bytes written to S3.") \
    M(WriteBufferFromS3WriteErrors, "Number of exceptions while writing to S3.") \
    \
    M(ReadBufferFromS3ReadCount, "The count of ReadBufferFromS3 read from s3 stream") \
    M(ReadBufferFromS3FailedCount, "ReadBuffer from s3 failed count") \
    M(ReadBufferFromS3ReadBytes, "Bytes size ReadBufferFromS3 read from s3 stream") \
    M(ReadBufferFromS3ReadMicroseconds, "The time spent ReadBufferFromS3 read from s3 stream") \
    M(ReadBufferFromS3StreamInitMicroseconds, "Time spent initializing connection to S3.") \
    M(ReadBufferFromS3Seeks, "The seek count of read buffer from s3.") \
    \
    M(ReadBufferFromRpcStreamFileRead, "remote rpc file data read count") \
    M(ReadBufferFromRpcStreamFileReadFailed, "remote rpc file data read failed count") \
    M(ReadBufferFromRpcStreamFileReadMs, "remote rpc file data read ms") \
    M(ReadBufferFromRpcStreamFileReadBytes, "remote rpc file data read bytes") \
    M(ReadBufferFromRpcStreamFileConnect, "remote rpc file data connect count") \
    M(ReadBufferFromRpcStreamFileConnectFailed, "remote rpc file data connect failed count") \
    M(ReadBufferFromRpcStreamFileConnectMs, "remote rpc file data connect ms") \
    \
    M(IOSchedulerOpenFileMicro, "Time used in open file when using io scheduler") \
    M(IOSchedulerScheduleMicro, "Time used in schedule io request") \
    M(IOSchedulerSubmittedUserRequests, "Number of submitted user request from user") \
    M(IOSchedulerExecutedRawReuqests, "Number of submitted raw request from io scheduler") \
    \
    M(WSReadBufferReadFailed, "WSReadBufferFromFS failed read count") \
    M(WSReadBufferReadCount, "WSReadBufferFromFS read count") \
    M(WSReadBufferReadBytes, "WSReadBufferFromFS readed bytes") \
    M(WSReadBufferReadMicro, "WSReadBufferFromFS readed time in micro seconds") \
    \
    M(PFRAWSReadBufferReadCount, "PFRAWSReadBufferFromFS nextImpl invoked count") \
    M(PFRAWSReadBufferPrefetchCount, "PFRAWSReadBufferFromFS prefetch triggered count") \
    M(PFRAWSReadBufferPrefetchUtilCount, "PFRAWSReadBufferFromFS prefetch used count") \
    M(PFRAWSReadBufferPrefetchWaitMicro, "PFRAWSReadBufferFromFS wait time before prefetch finished when utilize it") \
    M(PFRAWSReadBufferRemoteReadCount, "PFRAWSReadBufferFromFS remote read count, including direct read and prefetch read") \
    M(PFRAWSReadBufferRemoteReadBytes, "PFRAWSReadBufferFromFS triggered remote read bytes") \
    M(PFRAWSReadBufferReadMicro, "PFRAWSReadBufferFromFS read time, including seek and nextImpl") \
    \
    M(S3TrivialReaderReadCount, "S3TrivialReader read count") \
    M(S3TrivialReaderReadMicroseconds, "S3TrivialReader read micro seconds") \
    M(S3TrivialReaderReadBytes, "S3TrivialReader read bytes") \
    M(S3ReadAheadReaderReadCount, "The count of S3ReadAheadReader read from s3 stream") \
    M(S3ReadAheadReaderReadBytes, "The bytes number of S3ReadAheadReader read from s3 stream") \
    M(S3ReadAheadReaderReadMicro, "The time spent on S3ReadAheadReader read from s3 stream") \
    M(S3ReadAheadReaderIgnoreCount, "The count of S3ReadAheadReader ignore from s3 stream") \
    M(S3ReadAheadReaderIgnoreBytes, "The bytes number of S3ReadAheadReader ignore from s3 stream") \
    M(S3ReadAheadReaderIgnoreMicro, "The time spent on S3ReadAheadReader ignore from s3 stream") \
    M(S3ReadAheadReaderGetRequestCount, "The count of S3ReadAheadReader invokes s3 get request.") \
    M(S3ReadAheadReaderExpectReadBytes, "The expected read bytes from S3ReadAheadReader's get request.") \
    M(S3ReadAheadReaderGetRequestMicro, "The time spent on S3ReadAheadReader sends s3 get request.") \
    M(S3ReadAheadReaderSeekTimes, "The seek times of S3ReadAheadReader.") \
    M(S3ReadAheadReaderExpandTimes, "The stream range expand times of S3ReadAheadReader.") \
    M(S3ResetSessions, "Number of HTTP sessions that were reset in S3 read.") \
    M(S3PreservedSessions, "Number of HTTP sessions that were preserved in S3 read.") \
    M(ConnectionPoolIsFullMicroseconds, "Total time spent waiting for a slot in connection pool.") \
    \
    M(PocoHTTPS3GetCount, "") \
    M(PocoHTTPS3GetTime, "") \
    M(PocoHTTPS3GetSessionTime, "") \
    M(CRTHTTPS3GetCount, "") \
    M(CRTHTTPS3GetTime, "") \
    M(CRTHTTPS3WriteBytes, "") \
\
    M(BigHashEvictionCount, "BigHash eviction count") \
    M(BigHashEvictionExpiredCount, "BigHash eviction expired count") \
    M(BigHashLogicalWrittenCount, "BigHash logical written bytes") \
    M(BigHashPhysicalWrittenCount, "BigHash physical written bytes") \
    M(BigHashInsertCount, "BigHash insert count") \
    M(BigHashSuccInsertCount, "BigHash insert succeed count") \
    M(BigHashRemoveCount, "BigHash remove count") \
    M(BigHashSuccRemoveCount, "BigHash remove succeed count") \
    M(BigHashLookupCount, "BigHash lookup count") \
    M(BigHashSuccLookupCount, "BigHash lookup succeed count") \
    M(BigHashIOErrorCount, "BigHash IO error count") \
    M(BigHashBFFalsePositiveCount, "BigHash bloom filter false positive count") \
    M(BigHashBFProbCount, "BigHash bloom filter prob count") \
    M(BigHashBFRejectCount, "BigHash bloom filter reject count") \
\
    M(BlockCacheInsertCount, "BlockCache insert count") \
    M(BlockCacheInsertHashCollisionCount, "BlockCache insert hash collision count") \
    M(BlockCacheSuccInsertCount, "BlockCache insert succeed count") \
    M(BlockCacheLookupFalsePositiveCount, "BlockCache lookup false positive count") \
    M(BlockCacheLookupEntryHeaderChecksumErrorCount, "BlockCache lookup entry header checksum error count") \
    M(BlockCacheLookupValueChecksumErrorCount, "BlockCache lookup value checksum error count") \
    M(BlockCacheRemoveCount, "BlockCache remove count") \
    M(BlockCacheSuccRemoveCount, "BlockCache remove succeed count") \
    M(BlockCacheEvictionLookupMissCount, "BlockCache eviction looukup miss count") \
    M(BlockCacheEvictionExpiredCount, "BlockCache eviction expired count") \
    M(BlockCacheAllocErrorCount, "BlockCache alloc error count") \
    M(BlockCacheLogicWrittenCount, "BlockCache logic written count") \
    M(BlockCacheLookupCount, "BlockCache lookup count") \
    M(BlockCacheSuccLookupCount, "BlockCache succeed lookup count") \
    M(BlockCacheReinsertionErrorCount, "BlockCache reinsertion error count") \
    M(BlockCacheReinsertionCount, "BlockCache reinsertion count") \
    M(BlockCacheReinsertionBytes, "BlockCache reinsertion size in bytes") \
    M(BlockCacheLookupForItemDestructorErrorCount, "BlockCache lookup for item destructor error count") \
    M(BlockCacheRemoveAttemptionCollisions, "BlockCache remove attemption collisions") \
    M(BlockCacheReclaimEntryHeaderChecksumErrorCount, "BlockCache reclaim entry header checksum error count") \
    M(BlockCacheReclaimValueChecksumErrorCount, "BlockCache reclaim value checksum error count") \
    M(BlockCacheCleanupEntryHeaderChecksumErrorCount, "BlockCache cleanup entry header checksum error count") \
    M(BlockCacheCleanupValueChecksumErrorCount, "BlockCache cleanup value checksum error count") \
\
    M(NvmCacheLookupCount, "NvmCache lookup count") \
    M(NvmCacheLookupSuccCount, "NvmCache lookup success count") \
    M(NvmCacheInsertCount, "NvmCache insert count") \
    M(NvmCacheInsertSuccCount, "NvmCache insert success count") \
    M(NvmCacheRemoveCount, "NvmCache remove count") \
    M(NvmCacheRemoveSuccCount, "NvmCache remove success count") \
    M(NvmCacheIoErrorCount, "NvmCache io error count") \
    M(NvmGets, "NvmCache get op count") \
    M(NvmGetMissFast, "NvmCache fast get miss count") \
    M(NvmGetCoalesced, "NvmCache get coalesced count") \
    M(NvmGetMiss, "NvmCache get miss count") \
    M(NvmGetMissDueToInflightRemove, "NvmCache get miss count due to inflight remove") \
    M(NvmPuts, "NvmCache put op count") \
    M(NvmPutErrors, "NvmCache put error count") \
    M(NvmAbortedPutOnTombstone, "NvmCache abort count of puts due to tombstone") \
    M(NvmAbortedPutOnInflightGet, "NvmCache abort count of puts due to inflight get") \
    M(NvmDeletes, "NvmCache remove op count") \
    M(NvmSkippedDeletes, "NvmCache skipped remove op count") \
\
    M(RegionManagerPhysicalWrittenCount, "RegionManager physical written byte count") \
    M(RegionManagerReclaimRegionErrorCount, "RegionManager reclaim region error count") \
    M(RegionManagerReclaimCount, "RegionManager reclaim count") \
    M(RegionManagerReclaimTimeCount, "RegionManager reclaim time count") \
    M(RegionManagerEvictionCount, "RegionManager eviction count") \
    M(RegionManagerNumInMemBufFlushRetries, "RegionManager number of in-memory buffer flush retries") \
    M(RegionManagerNumInMemBufFlushFailures, "RegionManager number of in-memory buffer flush failures") \
    M(RegionManagerNumInMemBufCleanupRetries, "RegionManager number of in-memory buffer cleanup retries") \
    M(RegionManagerCleanRegionRetries, "RegionManager number of clean region retries") \
\
    M(NexusFSHit, "NexusFS hits") \
    M(NexusFSHitInflightInsert, "NexusFS hits on in-flight inserts") \
    M(NexusFSMiss, "NexusFS missed") \
    M(NexusFSPreload, "NexusFS preloads") \
    M(NexusFSDeepRetry, "NexusFS deep retries") \
    M(NexusFSDiskCacheEvict, "NexusFS disk cache evicts") \
    M(NexusFSDiskCacheInsertRetries, "NexusFS disk cache retries when insert") \
    M(NexusFSDiskCacheError, "NexusFS disk cache errors") \
    M(NexusFSDiskCacheBytesRead, "NexusFS disk cache bytes read") \
    M(NexusFSDiskCacheBytesWrite, "NexusFS disk cache bytes write") \
    M(NexusFSReadFromInsertCxt, "NexusFS ReadFromInsertCxt successes") \
    M(NexusFSReadFromInsertCxtRetry, "NexusFS ReadFromInsertCxt retries") \
    M(NexusFSReadFromInsertCxtDeepRetry, "NexusFS ReadFromInsertCxt deep retries") \
    M(NexusFSReadFromInsertCxtBytesRead, "NexusFS ReadFromInsertCxt bytes read") \
    M(NexusFSReadFromInsertCxtNonCopy, "NexusFS ReadFromInsertCxt by non-copying method successes") \
    M(NexusFSReadFromInsertCxtNonCopyBytesRead, "NexusFS ReadFromInsertCxt by non-copying method bytes read") \
    M(NexusFSReadFromDisk, "NexusFS ReadFromDisk successes") \
    M(NexusFSReadFromDiskRetry, "NexusFS ReadFromDisk retries") \
    M(NexusFSReadFromDiskDeepRetry, "NexusFS ReadFromDisk deep retries") \
    M(NexusFSReadFromDiskBytesRead, "NexusFS ReadFromDisk bytes read") \
    M(NexusFSReadFromBuffer, "NexusFS ReadFromBuffer successes") \
    M(NexusFSReadFromBufferRetry, "NexusFS ReadFromBuffer retries") \
    M(NexusFSReadFromBufferDeepRetry, "NexusFS ReadFromBuffer deep retries") \
    M(NexusFSReadFromBufferBytesRead, "NexusFS ReadFromBuffer bytes read") \
    M(NexusFSReadFromBufferNonCopy, "NexusFS ReadFromBuffer by non-copying method successes") \
    M(NexusFSReadFromBufferNonCopyBytesRead, "NexusFS ReadFromBuffer by non-copying method bytes read") \
    M(NexusFSReadFromSourceBytesRead, "NexusFS bytes read from source") \
    M(NexusFSReadFromSourceMicroseconds, "NexusFS read from source microseconds") \
    M(NexusFSTimeout, "NexusFS read timeouts") \
    M(NexusFSPrefetchToBuffer, "NexusFS PrefetchToBuffer successes") \
    M(NexusFSPrefetchToBufferBytesRead, "NexusFS PrefetchToBuffer bytes read") \
    M(NexusFSBufferHit, "NexusFS buffer hits") \
    M(NexusFSBufferMiss, "NexusFS buffer misses") \
    M(NexusFSBufferPreload, "NexusFS buffer preloads") \
    M(NexusFSBufferPreloadRetry, "NexusFS buffer retries in preload") \
    M(NexusFSBufferEmptyCoolingQueue, "NexusFS buffer cooling queue empty") \
    M(NexusFSInodeManagerLookupMicroseconds, "NexusFS InodeManager lookup microseconds") \
    M(NexusFSInodeManagerInsertMicroseconds, "NexusFS InodeManager insert microseconds") \
\
    M(ReadFromNexusFSReadBytes, "Read bytes from nuxusfs.") \
    M(ReadFromNexusFSSeeks, "Total number of seeks for async buffer") \
    M(ReadFromNexusFSPrefetchRequests, "Number of prefetches made with asynchronous reading from nuxusfs") \
    M(ReadFromNexusFSUnusedPrefetches, "Number of prefetches pending at buffer destruction") \
    M(ReadFromNexusFSPrefetchedReads, "Number of reads from prefetched buffer") \
    M(ReadFromNexusFSPrefetchTaskWait, "Number of waiting when reading from prefetched buffer") \
    M(ReadFromNexusFSPrefetchTaskNotWait, "Number of not waiting when reading from prefetched buffer") \
    M(ReadFromNexusFSPrefetchedBytes, "Number of bytes from prefetched buffer") \
    M(ReadFromNexusFSAsynchronousWaitMicroseconds, "Time spent in waiting for asynchronous nuxusfs reads.") \
    M(ReadFromNexusFSSynchronousWaitMicroseconds, "Time spent in waiting for synchronous nuxusfs reads.") \
\
    M(TSORequest, "Number requests sent to TSO") \
    M(TSORequestMicroseconds, "Total time spent in get timestamp from TSO") \
    M(TSOError, "Error logged by TSO Service as a response to CNCH") \
    \
    M(BackupVW, "Whether use backup virtual warehouse or not") \
    M(GetByKeySuccess, "") \
    M(GetByKeyFailed, "") \
\
    M(SkipRowsTimeMicro, "Time used in skip unnecessary rows") \
    M(ReadRowsTimeMicro, "Time used in read necessary rows") \
    M(GinIndexCacheHit, "Cache hit of gin index") \
    M(GinIndexCacheMiss, "Cache miss of gin index") \
    M(PostingReadBytes, "Readed postings list size in bytes") \
    M(QueryRewriterTime, "Total elapsed time spent on QueryRewriter in milliseconds") \
    M(QueryAnalyzerTime, "Total elapsed time spent on QueryAnalyzer in milliseconds") \
    M(QueryPlannerTime, "Total elapsed time spent on QueryPlanner in milliseconds") \
    M(QueryOptimizerTime, "Total elapsed time spent on QueryOptimizer in milliseconds") \
    M(PlanSegmentSplitterTime, "Total elapsed time spent on PlanSegmentSplitter in milliseconds") \
    M(GetMvBaseTableIDSuccess, "") \
    M(GetMvBaseTableIDFailed, "") \
    M(GetMvBaseTableVersionSuccess, "") \
    M(GetMvBaseTableVersionFailed, "") \
    M(UpdateMvMetaIDSuccess, "") \
    M(UpdateMvMetaIDFailed, "") \
\
    M(NumberOfMarkRangesBeforeBeMergedInPKFilter, "Number of mark ranges in primary index filtering before adjacent ranges be merged into more bigger ranges") \
    M(PlanSegmentInstanceRetry, "How many times this plan segment has been retried, only valid under bsp mode") \
\
    M(OrcTotalStripes, "Total Stripes") \
    M(OrcReadStripes, "Total Read Stripes") \
    M(HdfsReadBigAtCount, "ReadBigAt count for Hdfs") \
    M(HdfsReadBigAtBytes, "ReadBitAt bytes for Hdfs") \
    M(OrcSkippedRows, "rows skipped") \
    M(OrcTotalRows, "rows total") \
    M(OrcReadDirectCount, "direct read counts") \
    M(OrcReadCacheCount, "cache read counts") \
    M(OrcIOMergedCount, "") \
    M(OrcIOMergedBytes, "") \
    M(OrcIOSharedCount, "") \
    M(OrcIOSharedBytes, "") \
    M(OrcIODirectCount, "") \
    M(OrcIODirectBytes, "") \
    M(PreparePartsForReadMilliseconds, "The time spend on loading CNCH part from ServerPart on worker when query with table version") \
\
    M(IOUringSQEsSubmitted, "Total number of io_uring SQEs submitted") \
    M(IOUringSQEsResubmits, "Total number of io_uring SQE resubmits performed") \
    M(IOUringCQEsCompleted, "Total number of successfully completed io_uring CQEs") \
    M(IOUringCQEsFailed, "Total number of completed io_uring CQEs with failures") \
    M(IOUringAsynchronousReadWaitMicroseconds, "Time spent in waiting for asynchronous reads in io_uring local read.") \
    M(IOUringSynchronousReadWaitMicroseconds, "Time spent in waiting for synchronous reads in io_uring local read.") \
    M(IOUringReaderIgnoredBytes, "Number of bytes ignored in IOUringReader") \
\
    M(LoadedServerParts, "Total server parts loaded from storage manager by version") \
    M(LoadServerPartsMilliseconds, "The time spend on loading server parts by version from storage data manager.") \
    M(LoadManifestPartsDiskCacheHits, "Disk cache hit count of loading parts from manifest") \
    M(LoadManifestPartsDiskCacheMisses, "Disk cache miss count of loading parts from manifest") \
    M(ManifestCacheHits, "Manifest cache hit count of loading parts from manifest") \
    M(ManifestCacheMisses, "Manifest cache hit count of loading parts from manifest") \
\
    M(DeserializeSkippedCompressedBytes, "Total compressed bytes skipped when deserialize") \
    M(TotalGranulesCount, "The total granules before skipping index needs to read. If there are multiple indexes, the value is the sum value.") \
    M(TotalSkippedGranules, "The total granules that skipping index dropped. If there are multiple indexes, the value is the sum value.") \
    M(GinIndexFilterResultCacheHit, "Number of posting list result cache hit") \
    M(GinIndexFilterResultCacheMiss, "Number of posting list result cache miss") \
    M(PrimaryAndSecondaryIndexFilterTime, "Time used in primary index and secondary indices filterr, in micro seconds") \
\
    M(TableFinishStepPreClearHDFSTableMicroseconds, "") \
    M(TableFinishStepPreClearS3TableMicroseconds, "") \

namespace ProfileEvents
{

#define M(NAME, DOCUMENTATION) extern const Event NAME = __COUNTER__;
    APPLY_FOR_EVENTS(M)
#undef M
constexpr Event END = __COUNTER__;

/// Global variable, initialized by zeros.
Counter global_counters_array[END] {};
/// Initialize global counters statically
Counters global_counters(global_counters_array);

const Event Counters::num_counters = END;


Counters::Counters(VariableContext level_, Counters * parent_)
    : counters_holder(new Counter[num_counters] {}),
      parent(parent_),
      level(level_)
{
    counters = counters_holder.get();
}

void Counters::resetCounters()
{
    if (counters)
    {
        for (Event i = 0; i < num_counters; ++i)
            counters[i].store(0, std::memory_order_relaxed);
    }
}

void Counters::reset()
{
    parent = nullptr;
    resetCounters();
}

uint64_t Counters::getIOReadTime(bool use_async_read) const
{
    if (counters)
    {
        // If we use async read, we only calculate the time of wait for asynchronous result
        if (use_async_read)
        {
            return counters[ProfileEvents::RemoteFSAsynchronousReadWaitMicroseconds]
                + counters[ProfileEvents::RemoteFSSynchronousReadWaitMicroseconds]
                + counters[ProfileEvents::DiskReadElapsedMicroseconds]
                + counters_holder[ProfileEvents::ReadFromNexusFSAsynchronousWaitMicroseconds]
                + counters_holder[ProfileEvents::ReadFromNexusFSSynchronousWaitMicroseconds];
        }
        // Else, we calculate the origin read IO time
        else
        {
            return counters[ProfileEvents::HDFSReadElapsedMicroseconds]
                + counters[ProfileEvents::ReadBufferFromS3ReadMicroseconds]
                + counters[ProfileEvents::DiskReadElapsedMicroseconds]
                + counters_holder[ProfileEvents::ReadFromNexusFSAsynchronousWaitMicroseconds]
                + counters_holder[ProfileEvents::ReadFromNexusFSSynchronousWaitMicroseconds]
                - counters_holder[ProfileEvents::NexusFSReadFromSourceMicroseconds];
        }
    }

    return 0;
}

Counters::Snapshot::Snapshot()
    : counters_holder(new Count[num_counters] {})
{}

uint64_t Counters::Snapshot::getIOReadTime(bool use_async_read) const
{
    if (counters_holder)
    {
        // If we use async read, we only calculate the time of wait for asynchronous result
        if (use_async_read)
        {
            return counters_holder[ProfileEvents::RemoteFSAsynchronousReadWaitMicroseconds]
                + counters_holder[ProfileEvents::RemoteFSSynchronousReadWaitMicroseconds]
                + counters_holder[ProfileEvents::DiskReadElapsedMicroseconds]
                + counters_holder[ProfileEvents::ReadFromNexusFSAsynchronousWaitMicroseconds]
                + counters_holder[ProfileEvents::ReadFromNexusFSSynchronousWaitMicroseconds];
        }
        // Else, we calculate the origin read IO time
        else
        {
            return counters_holder[ProfileEvents::HDFSReadElapsedMicroseconds]
                + counters_holder[ProfileEvents::ReadBufferFromS3ReadMicroseconds]
                + counters_holder[ProfileEvents::DiskReadElapsedMicroseconds]
                + counters_holder[ProfileEvents::ReadFromNexusFSAsynchronousWaitMicroseconds]
                + counters_holder[ProfileEvents::ReadFromNexusFSSynchronousWaitMicroseconds]
                - counters_holder[ProfileEvents::NexusFSReadFromSourceMicroseconds];
        }
    }

    return 0;
}

Counters::Snapshot Counters::getPartiallyAtomicSnapshot() const
{
    Snapshot res;
    for (Event i = 0; i < num_counters; ++i)
        res.counters_holder[i] = counters[i].load(std::memory_order_relaxed);
    return res;
}

const char * getName(Event event)
{
    static const char * strings[] =
    {
    #define M(NAME, DOCUMENTATION) #NAME,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}

const DB::String getSnakeName(Event event)
{
    DB::String res{getName(event)};

    convertCamelToSnake(res);

    return res;
}

const char * getDocumentation(Event event)
{
    static const char * strings[] =
    {
    #define M(NAME, DOCUMENTATION) DOCUMENTATION,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}


Event end() { return END; }

void increment(Event event, Count amount, Metrics::MetricType type, LabelledMetrics::MetricLabels labels, time_t ts)
{
    DB::CurrentThread::getProfileEvents().increment(event, amount);
    try
    {
        if (type != Metrics::MetricType::None)
        {
            Metrics::EmitMetric(type, getSnakeName(event), amount, LabelledMetrics::toString(labels), ts);
        }
    }
    catch (DB::Exception & e)
    {
        LOG_ERROR(getLogger("ProfileEvents"), "Metrics emit metric failed: {}", e.message());
    }
}

}

#undef APPLY_FOR_EVENTS
