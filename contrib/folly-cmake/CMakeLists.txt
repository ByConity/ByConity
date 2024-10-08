set (LIBDIR "${ClickHouse_SOURCE_DIR}/contrib/folly")
set (FOLLLY_DIR "${LIBDIR}/folly")

set (SRCS
${FOLLLY_DIR}/AtomicHashArray.h
${FOLLLY_DIR}/AtomicHashArray-inl.h
${FOLLLY_DIR}/AtomicHashMap.h
${FOLLLY_DIR}/AtomicHashMap-inl.h
${FOLLLY_DIR}/AtomicIntrusiveLinkedList.h
${FOLLLY_DIR}/AtomicLinkedList.h
${FOLLLY_DIR}/AtomicUnorderedMap.h
${FOLLLY_DIR}/Benchmark.cpp
${FOLLLY_DIR}/Benchmark.h
${FOLLLY_DIR}/BenchmarkUtil.h
${FOLLLY_DIR}/Bits.h
${FOLLLY_DIR}/CPortability.h
${FOLLLY_DIR}/CancellationToken.cpp
${FOLLLY_DIR}/CancellationToken.h
${FOLLLY_DIR}/CancellationToken-inl.h
${FOLLLY_DIR}/Chrono.h
${FOLLLY_DIR}/ClockGettimeWrappers.cpp
${FOLLLY_DIR}/ClockGettimeWrappers.h
${FOLLLY_DIR}/ConcurrentBitSet.h
${FOLLLY_DIR}/ConcurrentLazy.h
${FOLLLY_DIR}/ConcurrentSkipList.h
${FOLLLY_DIR}/ConcurrentSkipList-inl.h
${FOLLLY_DIR}/ConstexprMath.h
${FOLLLY_DIR}/ConstructorCallbackList.h
${FOLLLY_DIR}/Conv.cpp
${FOLLLY_DIR}/Conv.h
${FOLLLY_DIR}/CppAttributes.h
${FOLLLY_DIR}/CpuId.h
${FOLLLY_DIR}/DefaultKeepAliveExecutor.h
${FOLLLY_DIR}/Demangle.cpp
${FOLLLY_DIR}/Demangle.h
${FOLLLY_DIR}/DiscriminatedPtr.h
${FOLLLY_DIR}/DynamicConverter.h
${FOLLLY_DIR}/Exception.h
${FOLLLY_DIR}/ExceptionString.cpp
${FOLLLY_DIR}/ExceptionString.h
${FOLLLY_DIR}/ExceptionWrapper.cpp
${FOLLLY_DIR}/ExceptionWrapper.h
${FOLLLY_DIR}/ExceptionWrapper-inl.h
${FOLLLY_DIR}/Executor.cpp
${FOLLLY_DIR}/Executor.h
${FOLLLY_DIR}/Expected.h
${FOLLLY_DIR}/FBString.h
${FOLLLY_DIR}/FBVector.h
${FOLLLY_DIR}/File.cpp
${FOLLLY_DIR}/File.h
${FOLLLY_DIR}/FileUtil.cpp
${FOLLLY_DIR}/FileUtil.h
${FOLLLY_DIR}/Fingerprint.cpp
${FOLLLY_DIR}/Fingerprint.h
${FOLLLY_DIR}/FixedString.h
${FOLLLY_DIR}/FollyMemcpy.cpp
${FOLLLY_DIR}/FollyMemcpy.h
${FOLLLY_DIR}/FollyMemset.cpp
${FOLLLY_DIR}/FollyMemset.h
${FOLLLY_DIR}/Format.cpp
${FOLLLY_DIR}/Format.h
${FOLLLY_DIR}/FormatArg.h
${FOLLLY_DIR}/FormatTraits.h
${FOLLLY_DIR}/Format-inl.h
${FOLLLY_DIR}/Function.h
${FOLLLY_DIR}/GLog.h
${FOLLLY_DIR}/GroupVarint.cpp
${FOLLLY_DIR}/GroupVarint.h
${FOLLLY_DIR}/Hash.h
${FOLLLY_DIR}/IPAddress.cpp
${FOLLLY_DIR}/IPAddress.h
${FOLLLY_DIR}/IPAddressException.h
${FOLLLY_DIR}/IPAddressV4.cpp
${FOLLLY_DIR}/IPAddressV4.h
${FOLLLY_DIR}/IPAddressV6.cpp
${FOLLLY_DIR}/IPAddressV6.h
${FOLLLY_DIR}/Indestructible.h
${FOLLLY_DIR}/IndexedMemPool.h
${FOLLLY_DIR}/IntrusiveList.h
${FOLLLY_DIR}/Lazy.h
${FOLLLY_DIR}/Likely.h
${FOLLLY_DIR}/MPMCPipeline.h
${FOLLLY_DIR}/MPMCQueue.h
${FOLLLY_DIR}/MacAddress.cpp
${FOLLLY_DIR}/MacAddress.h
${FOLLLY_DIR}/MapUtil.h
${FOLLLY_DIR}/Math.h
${FOLLLY_DIR}/MaybeManagedPtr.h
${FOLLLY_DIR}/Memory.h
${FOLLLY_DIR}/MicroLock.cpp
${FOLLLY_DIR}/MicroLock.h
${FOLLLY_DIR}/MicroSpinLock.h
${FOLLLY_DIR}/MoveWrapper.h
${FOLLLY_DIR}/ObserverContainer.h
${FOLLLY_DIR}/Optional.h
${FOLLLY_DIR}/Overload.h
${FOLLLY_DIR}/PackedSyncPtr.h
${FOLLLY_DIR}/Padded.h
${FOLLLY_DIR}/Poly.h
${FOLLLY_DIR}/PolyException.h
${FOLLLY_DIR}/Poly-inl.h
${FOLLLY_DIR}/Portability.h
${FOLLLY_DIR}/Preprocessor.h
${FOLLLY_DIR}/ProducerConsumerQueue.h
${FOLLLY_DIR}/RWSpinLock.h
${FOLLLY_DIR}/Random.cpp
${FOLLLY_DIR}/Random.h
${FOLLLY_DIR}/Random-inl.h
${FOLLLY_DIR}/Range.h
${FOLLLY_DIR}/Replaceable.h
${FOLLLY_DIR}/ScopeGuard.cpp
${FOLLLY_DIR}/ScopeGuard.h
${FOLLLY_DIR}/SharedMutex.cpp
${FOLLLY_DIR}/SharedMutex.h
${FOLLLY_DIR}/Singleton.cpp
${FOLLLY_DIR}/Singleton.h
${FOLLLY_DIR}/SingletonThreadLocal.cpp
${FOLLLY_DIR}/SingletonThreadLocal.h
${FOLLLY_DIR}/Singleton-inl.h
${FOLLLY_DIR}/SocketAddress.cpp
${FOLLLY_DIR}/SocketAddress.h
${FOLLLY_DIR}/SpinLock.h
${FOLLLY_DIR}/String.cpp
${FOLLLY_DIR}/String.h
${FOLLLY_DIR}/String-inl.h
${FOLLLY_DIR}/Subprocess.cpp
${FOLLLY_DIR}/Subprocess.h
${FOLLLY_DIR}/Synchronized.h
${FOLLLY_DIR}/SynchronizedPtr.h
${FOLLLY_DIR}/ThreadCachedInt.h
${FOLLLY_DIR}/ThreadLocal.h
${FOLLLY_DIR}/TimeoutQueue.cpp
${FOLLLY_DIR}/TimeoutQueue.h
${FOLLLY_DIR}/TokenBucket.h
${FOLLLY_DIR}/Traits.h
${FOLLLY_DIR}/Try.cpp
${FOLLLY_DIR}/Try.h
${FOLLLY_DIR}/Try-inl.h
${FOLLLY_DIR}/UTF8String.h
${FOLLLY_DIR}/Unicode.cpp
${FOLLLY_DIR}/Unicode.h
${FOLLLY_DIR}/Unit.h
${FOLLLY_DIR}/Uri.cpp
${FOLLLY_DIR}/Uri.h
${FOLLLY_DIR}/Uri-inl.h
${FOLLLY_DIR}/Utility.h
${FOLLLY_DIR}/Varint.h
${FOLLLY_DIR}/VirtualExecutor.h
${FOLLLY_DIR}/algorithm/simd/FindFixed.h
${FOLLLY_DIR}/algorithm/simd/Movemask.h
${FOLLLY_DIR}/base64.h
${FOLLLY_DIR}/channels/Channel.h
${FOLLLY_DIR}/channels/ChannelCallbackHandle.h
${FOLLLY_DIR}/channels/ChannelProcessor.h
${FOLLLY_DIR}/channels/ChannelProcessor-inl.h
${FOLLLY_DIR}/channels/Channel-fwd.h
${FOLLLY_DIR}/channels/Channel-inl.h
${FOLLLY_DIR}/channels/ConsumeChannel.h
${FOLLLY_DIR}/channels/ConsumeChannel-inl.h
${FOLLLY_DIR}/channels/FanoutChannel.h
${FOLLLY_DIR}/channels/FanoutChannel-inl.h
${FOLLLY_DIR}/channels/FanoutSender.h
${FOLLLY_DIR}/channels/FanoutSender-inl.h
${FOLLLY_DIR}/channels/MaxConcurrentRateLimiter.cpp
${FOLLLY_DIR}/channels/MaxConcurrentRateLimiter.h
${FOLLLY_DIR}/channels/Merge.h
${FOLLLY_DIR}/channels/MergeChannel.h
${FOLLLY_DIR}/channels/MergeChannel-inl.h
${FOLLLY_DIR}/channels/Merge-inl.h
${FOLLLY_DIR}/channels/MultiplexChannel.h
${FOLLLY_DIR}/channels/MultiplexChannel-inl.h
${FOLLLY_DIR}/channels/OnClosedException.h
${FOLLLY_DIR}/channels/Producer.h
${FOLLLY_DIR}/channels/Producer-inl.h
${FOLLLY_DIR}/channels/ProxyChannel.h
${FOLLLY_DIR}/channels/ProxyChannel-inl.h
${FOLLLY_DIR}/channels/RateLimiter.h
${FOLLLY_DIR}/channels/Transform.h
${FOLLLY_DIR}/channels/Transform-inl.h
${FOLLLY_DIR}/channels/detail/AtomicQueue.h
${FOLLLY_DIR}/channels/detail/ChannelBridge.h
${FOLLLY_DIR}/channels/detail/FunctionTraits.h
${FOLLLY_DIR}/channels/detail/IntrusivePtr.h
${FOLLLY_DIR}/channels/detail/MultiplexerTraits.h
${FOLLLY_DIR}/channels/detail/PointerVariant.h
${FOLLLY_DIR}/channels/detail/Utility.h
${FOLLLY_DIR}/chrono/Clock.h
${FOLLLY_DIR}/chrono/Conv.h
${FOLLLY_DIR}/chrono/Hardware.h
${FOLLLY_DIR}/cli/NestedCommandLineApp.cpp
${FOLLLY_DIR}/cli/NestedCommandLineApp.h
${FOLLLY_DIR}/cli/ProgramOptions.cpp
${FOLLLY_DIR}/cli/ProgramOptions.h
${FOLLLY_DIR}/compression/Compression.cpp
${FOLLLY_DIR}/compression/Compression.h
${FOLLLY_DIR}/compression/CompressionContextPool.h
${FOLLLY_DIR}/compression/CompressionContextPoolSingletons.cpp
${FOLLLY_DIR}/compression/CompressionContextPoolSingletons.h
${FOLLLY_DIR}/compression/CompressionCoreLocalContextPool.h
${FOLLLY_DIR}/compression/Instructions.h
${FOLLLY_DIR}/compression/QuotientMultiSet.cpp
${FOLLLY_DIR}/compression/QuotientMultiSet.h
${FOLLLY_DIR}/compression/QuotientMultiSet-inl.h
${FOLLLY_DIR}/compression/Select64.cpp
${FOLLLY_DIR}/compression/Select64.h
${FOLLLY_DIR}/compression/Utils.h
${FOLLLY_DIR}/compression/Zlib.cpp
${FOLLLY_DIR}/compression/Zlib.h
${FOLLLY_DIR}/compression/Zstd.cpp
${FOLLLY_DIR}/compression/Zstd.h
${FOLLLY_DIR}/compression/elias_fano/BitVectorCoding.h
${FOLLLY_DIR}/compression/elias_fano/CodingDetail.h
${FOLLLY_DIR}/compression/elias_fano/EliasFanoCoding.h
${FOLLLY_DIR}/concurrency/AtomicSharedPtr.h
${FOLLLY_DIR}/concurrency/CacheLocality.cpp
${FOLLLY_DIR}/concurrency/CacheLocality.h
${FOLLLY_DIR}/concurrency/ConcurrentHashMap.h
${FOLLLY_DIR}/concurrency/CoreCachedSharedPtr.h
${FOLLLY_DIR}/concurrency/DeadlockDetector.cpp
${FOLLLY_DIR}/concurrency/DeadlockDetector.h
${FOLLLY_DIR}/concurrency/DynamicBoundedQueue.h
${FOLLLY_DIR}/concurrency/PriorityUnboundedQueueSet.h
${FOLLLY_DIR}/concurrency/ProcessLocalUniqueId.cpp
${FOLLLY_DIR}/concurrency/ProcessLocalUniqueId.h
${FOLLLY_DIR}/concurrency/SingletonRelaxedCounter.h
${FOLLLY_DIR}/concurrency/ThreadCachedSynchronized.h
${FOLLLY_DIR}/concurrency/UnboundedQueue.h
${FOLLLY_DIR}/concurrency/container/LockFreeRingBuffer.h
${FOLLLY_DIR}/concurrency/container/RelaxedConcurrentPriorityQueue.h
${FOLLLY_DIR}/concurrency/detail/AtomicSharedPtr-detail.h
${FOLLLY_DIR}/concurrency/detail/ConcurrentHashMap-detail.h
${FOLLLY_DIR}/concurrency/memory/AtomicReadMostlyMainPtr.cpp
${FOLLLY_DIR}/concurrency/memory/AtomicReadMostlyMainPtr.h
${FOLLLY_DIR}/concurrency/memory/PrimaryPtr.h
${FOLLLY_DIR}/concurrency/memory/ReadMostlySharedPtr.h
${FOLLLY_DIR}/concurrency/memory/TLRefCount.h
${FOLLLY_DIR}/container/Access.h
${FOLLLY_DIR}/container/Array.h
${FOLLLY_DIR}/container/BitIterator.h
${FOLLLY_DIR}/container/Enumerate.h
${FOLLLY_DIR}/container/EvictingCacheMap.h
${FOLLLY_DIR}/container/F14Map.h
${FOLLLY_DIR}/container/F14Map-fwd.h
${FOLLLY_DIR}/container/F14Set.h
${FOLLLY_DIR}/container/F14Set-fwd.h
${FOLLLY_DIR}/container/FBVector.h
${FOLLLY_DIR}/container/Foreach.h
${FOLLLY_DIR}/container/Foreach-inl.h
${FOLLLY_DIR}/container/HeterogeneousAccess.h
${FOLLLY_DIR}/container/HeterogeneousAccess-fwd.h
${FOLLLY_DIR}/container/IntrusiveHeap.h
${FOLLLY_DIR}/container/IntrusiveList.h
${FOLLLY_DIR}/container/Iterator.h
${FOLLLY_DIR}/container/MapUtil.h
${FOLLLY_DIR}/container/Merge.h
${FOLLLY_DIR}/container/Reserve.h
${FOLLLY_DIR}/container/SparseByteSet.h
${FOLLLY_DIR}/container/View.h
${FOLLLY_DIR}/container/WeightedEvictingCacheMap.h
${FOLLLY_DIR}/container/detail/BitIteratorDetail.h
${FOLLLY_DIR}/container/detail/F14Defaults.h
${FOLLLY_DIR}/container/detail/F14IntrinsicsAvailability.h
${FOLLLY_DIR}/container/detail/F14MapFallback.h
${FOLLLY_DIR}/container/detail/F14Mask.h
${FOLLLY_DIR}/container/detail/F14Policy.h
${FOLLLY_DIR}/container/detail/F14SetFallback.h
${FOLLLY_DIR}/container/detail/F14Table.cpp
${FOLLLY_DIR}/container/detail/F14Table.h
${FOLLLY_DIR}/container/detail/Util.h
${FOLLLY_DIR}/container/detail/tape_detail.h
${FOLLLY_DIR}/container/heap_vector_types.h
${FOLLLY_DIR}/container/range_traits.h
${FOLLLY_DIR}/container/small_vector.h
${FOLLLY_DIR}/container/sorted_vector_types.h
${FOLLLY_DIR}/container/tape.h
# ${FOLLLY_DIR}/crypto/Blake2xb.cpp
# ${FOLLLY_DIR}/crypto/Blake2xb.h
# ${FOLLLY_DIR}/crypto/LtHash.cpp
# ${FOLLLY_DIR}/crypto/LtHash.h
# ${FOLLLY_DIR}/crypto/LtHash-inl.h
# ${FOLLLY_DIR}/crypto/detail/LtHashInternal.h
# ${FOLLLY_DIR}/crypto/detail/MathOperation_AVX2.cpp
# ${FOLLLY_DIR}/crypto/detail/MathOperation_SSE2.cpp
# ${FOLLLY_DIR}/crypto/detail/MathOperation_Simple.cpp
${FOLLLY_DIR}/detail/AsyncTrace.cpp
${FOLLLY_DIR}/detail/AsyncTrace.h
${FOLLLY_DIR}/detail/AtomicHashUtils.h
${FOLLLY_DIR}/detail/AtomicUnorderedMapUtils.h
${FOLLLY_DIR}/detail/DiscriminatedPtrDetail.h
${FOLLLY_DIR}/detail/FileUtilDetail.cpp
${FOLLLY_DIR}/detail/FileUtilDetail.h
${FOLLLY_DIR}/detail/FileUtilVectorDetail.h
${FOLLLY_DIR}/detail/FingerprintPolynomial.h
${FOLLLY_DIR}/detail/Futex.cpp
${FOLLLY_DIR}/detail/Futex.h
${FOLLLY_DIR}/detail/Futex-inl.h
${FOLLLY_DIR}/detail/GroupVarintDetail.h
${FOLLLY_DIR}/detail/IPAddress.cpp
${FOLLLY_DIR}/detail/IPAddress.h
${FOLLLY_DIR}/detail/IPAddressSource.h
${FOLLLY_DIR}/detail/Iterators.h
${FOLLLY_DIR}/detail/MPMCPipelineDetail.h
${FOLLLY_DIR}/detail/MemoryIdler.cpp
${FOLLLY_DIR}/detail/MemoryIdler.h
${FOLLLY_DIR}/detail/PerfScoped.cpp
${FOLLLY_DIR}/detail/PerfScoped.h
${FOLLLY_DIR}/detail/PolyDetail.h
${FOLLLY_DIR}/detail/RangeCommon.cpp
${FOLLLY_DIR}/detail/RangeCommon.h
${FOLLLY_DIR}/detail/RangeSse42.cpp
${FOLLLY_DIR}/detail/RangeSse42.h
${FOLLLY_DIR}/detail/SimdAnyOf.h
${FOLLLY_DIR}/detail/SimdCharPlatform.h
${FOLLLY_DIR}/detail/SimdForEach.h
${FOLLLY_DIR}/detail/SimpleSimdStringUtils.cpp
${FOLLLY_DIR}/detail/SimpleSimdStringUtils.h
${FOLLLY_DIR}/detail/SimpleSimdStringUtilsImpl.h
${FOLLLY_DIR}/detail/Singleton.h
${FOLLLY_DIR}/detail/SlowFingerprint.h
${FOLLLY_DIR}/detail/SocketFastOpen.cpp
${FOLLLY_DIR}/detail/SocketFastOpen.h
${FOLLLY_DIR}/detail/SplitStringSimd.cpp
${FOLLLY_DIR}/detail/SplitStringSimd.h
${FOLLLY_DIR}/detail/SplitStringSimdImpl.h
${FOLLLY_DIR}/detail/Sse.cpp
${FOLLLY_DIR}/detail/Sse.h
${FOLLLY_DIR}/detail/StaticSingletonManager.cpp
${FOLLLY_DIR}/detail/StaticSingletonManager.h
${FOLLLY_DIR}/detail/ThreadLocalDetail.cpp
${FOLLLY_DIR}/detail/ThreadLocalDetail.h
${FOLLLY_DIR}/detail/TrapOnAvx512.cpp
${FOLLLY_DIR}/detail/TrapOnAvx512.h
${FOLLLY_DIR}/detail/TurnSequencer.h
${FOLLLY_DIR}/detail/TypeList.h
${FOLLLY_DIR}/detail/UniqueInstance.cpp
${FOLLLY_DIR}/detail/UniqueInstance.h
${FOLLLY_DIR}/detail/UnrollUtils.h
${FOLLLY_DIR}/detail/base64_detail/Base64Api.cpp
${FOLLLY_DIR}/detail/base64_detail/Base64Api.h
${FOLLLY_DIR}/detail/base64_detail/Base64Common.h
${FOLLLY_DIR}/detail/base64_detail/Base64Constants.h
${FOLLLY_DIR}/detail/base64_detail/Base64HiddenConstants.h
${FOLLLY_DIR}/detail/base64_detail/Base64SWAR.cpp
${FOLLLY_DIR}/detail/base64_detail/Base64SWAR.h
${FOLLLY_DIR}/detail/base64_detail/Base64Scalar.h
${FOLLLY_DIR}/detail/base64_detail/Base64Simd.h
${FOLLLY_DIR}/detail/base64_detail/Base64_SSE4_2.cpp
${FOLLLY_DIR}/detail/base64_detail/Base64_SSE4_2.h
${FOLLLY_DIR}/detail/base64_detail/Base64_SSE4_2_Platform.h
# ${FOLLLY_DIR}/detail/base64_detail/tests/Base64AgainstScalarTest.cpp
# ${FOLLLY_DIR}/detail/base64_detail/tests/Base64PlatformTest.cpp
# ${FOLLLY_DIR}/detail/base64_detail/tests/Base64SpecialCasesTest.cpp
# ${FOLLLY_DIR}/docs/examples/folly/CancellationCallback.cpp
# ${FOLLLY_DIR}/docs/examples/folly/CancellationSource.cpp
# ${FOLLLY_DIR}/docs/examples/folly/CancellationToken.cpp
# ${FOLLLY_DIR}/docs/examples/folly/DynamicConverter.cpp
# ${FOLLLY_DIR}/docs/examples/folly/ExecutorGuide.cpp
# ${FOLLLY_DIR}/docs/examples/folly/File.cpp
# ${FOLLLY_DIR}/docs/examples/folly/Format.cpp
# ${FOLLLY_DIR}/docs/examples/folly/Function.cpp
# ${FOLLLY_DIR}/docs/examples/folly/Likely.cpp
# ${FOLLLY_DIR}/docs/examples/folly/MapUtil.cpp
# ${FOLLLY_DIR}/docs/examples/folly/ScopeGuard.cpp
# ${FOLLLY_DIR}/docs/examples/folly/ScopeGuard2.cpp
# ${FOLLLY_DIR}/docs/examples/folly/ScopedEventBaseThread.cpp
# ${FOLLLY_DIR}/docs/examples/folly/ScopedEventBaseThread2.cpp
# ${FOLLLY_DIR}/docs/examples/folly/Synchronized.cpp
# ${FOLLLY_DIR}/docs/examples/folly/container/Array.cpp
# ${FOLLLY_DIR}/docs/examples/folly/dynamic.cpp
# ${FOLLLY_DIR}/docs/examples/folly/dynamic/array.cpp
# ${FOLLLY_DIR}/docs/examples/folly/dynamic/object.cpp
# ${FOLLLY_DIR}/docs/examples/folly/experimental/coro/AsyncScope.cpp
# ${FOLLLY_DIR}/docs/examples/folly/experimental/coro/CancellableAsyncScope.cpp
# ${FOLLLY_DIR}/docs/examples/folly/experimental/coro/DetachOnCancel.cpp
# ${FOLLLY_DIR}/docs/examples/folly/experimental/coro/Promise.cpp
# ${FOLLLY_DIR}/docs/examples/folly/experimental/coro/Retry.cpp
# ${FOLLLY_DIR}/docs/examples/folly/experimental/coro/Task.cpp
# ${FOLLLY_DIR}/docs/examples/folly/experimental/coro/WithCancellation.cpp
# ${FOLLLY_DIR}/docs/examples/folly/hash/Hash.cpp
# ${FOLLLY_DIR}/docs/examples/folly/io/IOBuf.cpp
# ${FOLLLY_DIR}/docs/examples/folly/ipaddress.cpp
# ${FOLLLY_DIR}/docs/examples/folly/synchronization/Baton.cpp
${FOLLLY_DIR}/dynamic.h
${FOLLLY_DIR}/dynamic-inl.h
${FOLLLY_DIR}/executors/Async.h
${FOLLLY_DIR}/executors/CPUThreadPoolExecutor.cpp
${FOLLLY_DIR}/executors/CPUThreadPoolExecutor.h
${FOLLLY_DIR}/executors/Codel.cpp
${FOLLLY_DIR}/executors/Codel.h
${FOLLLY_DIR}/executors/DrivableExecutor.h
${FOLLLY_DIR}/executors/EDFThreadPoolExecutor.cpp
${FOLLLY_DIR}/executors/EDFThreadPoolExecutor.h
${FOLLLY_DIR}/executors/ExecutorWithPriority.cpp
${FOLLLY_DIR}/executors/ExecutorWithPriority.h
${FOLLLY_DIR}/executors/ExecutorWithPriority-inl.h
${FOLLLY_DIR}/executors/FiberIOExecutor.h
${FOLLLY_DIR}/executors/FunctionScheduler.cpp
${FOLLLY_DIR}/executors/FunctionScheduler.h
${FOLLLY_DIR}/executors/FutureExecutor.h
${FOLLLY_DIR}/executors/GlobalExecutor.cpp
${FOLLLY_DIR}/executors/GlobalExecutor.h
${FOLLLY_DIR}/executors/GlobalThreadPoolList.cpp
${FOLLLY_DIR}/executors/GlobalThreadPoolList.h
${FOLLLY_DIR}/executors/IOExecutor.h
${FOLLLY_DIR}/executors/IOObjectCache.h
${FOLLLY_DIR}/executors/IOThreadPoolDeadlockDetectorObserver.cpp
${FOLLLY_DIR}/executors/IOThreadPoolDeadlockDetectorObserver.h
${FOLLLY_DIR}/executors/IOThreadPoolExecutor.cpp
${FOLLLY_DIR}/executors/IOThreadPoolExecutor.h
${FOLLLY_DIR}/executors/InlineExecutor.cpp
${FOLLLY_DIR}/executors/InlineExecutor.h
${FOLLLY_DIR}/executors/ManualExecutor.cpp
${FOLLLY_DIR}/executors/ManualExecutor.h
${FOLLLY_DIR}/executors/MeteredExecutor.h
${FOLLLY_DIR}/executors/MeteredExecutor-inl.h
${FOLLLY_DIR}/executors/QueueObserver.cpp
${FOLLLY_DIR}/executors/QueueObserver.h
${FOLLLY_DIR}/executors/QueuedImmediateExecutor.cpp
${FOLLLY_DIR}/executors/QueuedImmediateExecutor.h
${FOLLLY_DIR}/executors/ScheduledExecutor.h
${FOLLLY_DIR}/executors/SequencedExecutor.h
${FOLLLY_DIR}/executors/SerialExecutor.h
${FOLLLY_DIR}/executors/SerialExecutor-inl.h
${FOLLLY_DIR}/executors/SerializedExecutor.h
${FOLLLY_DIR}/executors/SoftRealTimeExecutor.cpp
${FOLLLY_DIR}/executors/SoftRealTimeExecutor.h
${FOLLLY_DIR}/executors/StrandExecutor.cpp
${FOLLLY_DIR}/executors/StrandExecutor.h
${FOLLLY_DIR}/executors/ThreadPoolExecutor.cpp
${FOLLLY_DIR}/executors/ThreadPoolExecutor.h
${FOLLLY_DIR}/executors/ThreadedExecutor.cpp
${FOLLLY_DIR}/executors/ThreadedExecutor.h
${FOLLLY_DIR}/executors/ThreadedRepeatingFunctionRunner.cpp
${FOLLLY_DIR}/executors/ThreadedRepeatingFunctionRunner.h
${FOLLLY_DIR}/executors/TimedDrivableExecutor.cpp
${FOLLLY_DIR}/executors/TimedDrivableExecutor.h
${FOLLLY_DIR}/executors/TimekeeperScheduledExecutor.cpp
${FOLLLY_DIR}/executors/TimekeeperScheduledExecutor.h
${FOLLLY_DIR}/executors/VirtualExecutor.h
${FOLLLY_DIR}/executors/task_queue/BlockingQueue.h
${FOLLLY_DIR}/executors/task_queue/LifoSemMPMCQueue.h
${FOLLLY_DIR}/executors/task_queue/PriorityLifoSemMPMCQueue.h
${FOLLLY_DIR}/executors/task_queue/PriorityUnboundedBlockingQueue.h
${FOLLLY_DIR}/executors/task_queue/UnboundedBlockingQueue.h
${FOLLLY_DIR}/executors/thread_factory/InitThreadFactory.h
${FOLLLY_DIR}/executors/thread_factory/NamedThreadFactory.h
${FOLLLY_DIR}/executors/thread_factory/PriorityThreadFactory.cpp
${FOLLLY_DIR}/executors/thread_factory/PriorityThreadFactory.h
${FOLLLY_DIR}/executors/thread_factory/ThreadFactory.h
${FOLLLY_DIR}/experimental/AutoTimer.h
${FOLLLY_DIR}/experimental/BitVectorCoding.h
${FOLLLY_DIR}/experimental/Bits.h
${FOLLLY_DIR}/experimental/Cleanup.h
${FOLLLY_DIR}/experimental/CodingDetail.h
${FOLLLY_DIR}/experimental/DynamicParser.h
${FOLLLY_DIR}/experimental/DynamicParser-inl.h
${FOLLLY_DIR}/experimental/EliasFanoCoding.h
${FOLLLY_DIR}/experimental/EventCount.h
${FOLLLY_DIR}/experimental/ExecutionObserver.cpp
${FOLLLY_DIR}/experimental/ExecutionObserver.h
${FOLLLY_DIR}/experimental/FlatCombiningPriorityQueue.h
${FOLLLY_DIR}/experimental/FunctionScheduler.h
${FOLLLY_DIR}/experimental/Instructions.h
${FOLLLY_DIR}/experimental/JSONSchema.h
${FOLLLY_DIR}/experimental/LockFreeRingBuffer.h
${FOLLLY_DIR}/experimental/NestedCommandLineApp.h
${FOLLLY_DIR}/experimental/PrimaryPtr.h
${FOLLLY_DIR}/experimental/ProgramOptions.h
${FOLLLY_DIR}/experimental/QuotientMultiSet.h
${FOLLLY_DIR}/experimental/QuotientMultiSet-inl.h
${FOLLLY_DIR}/experimental/ReadMostlySharedPtr.h
${FOLLLY_DIR}/experimental/RelaxedConcurrentPriorityQueue.h
${FOLLLY_DIR}/experimental/STTimerFDTimeoutManager.cpp
${FOLLLY_DIR}/experimental/STTimerFDTimeoutManager.h
${FOLLLY_DIR}/experimental/Select64.h
${FOLLLY_DIR}/experimental/SingleWriterFixedHashMap.h
${FOLLLY_DIR}/experimental/SingletonRelaxedCounter.h
${FOLLLY_DIR}/experimental/TLRefCount.h
${FOLLLY_DIR}/experimental/TestUtil.h
${FOLLLY_DIR}/experimental/ThreadedRepeatingFunctionRunner.h
${FOLLLY_DIR}/experimental/TimerFD.cpp
${FOLLLY_DIR}/experimental/TimerFD.h
${FOLLLY_DIR}/experimental/TimerFDTimeoutManager.cpp
${FOLLLY_DIR}/experimental/TimerFDTimeoutManager.h
# ${FOLLLY_DIR}/experimental/channels/Channel.h
# ${FOLLLY_DIR}/experimental/channels/ChannelCallbackHandle.h
# ${FOLLLY_DIR}/experimental/channels/ChannelProcessor.h
# ${FOLLLY_DIR}/experimental/channels/ChannelProcessor-inl.h
# ${FOLLLY_DIR}/experimental/channels/Channel-fwd.h
# ${FOLLLY_DIR}/experimental/channels/Channel-inl.h
# ${FOLLLY_DIR}/experimental/channels/ConsumeChannel.h
# ${FOLLLY_DIR}/experimental/channels/ConsumeChannel-inl.h
# ${FOLLLY_DIR}/experimental/channels/FanoutChannel.h
# ${FOLLLY_DIR}/experimental/channels/FanoutChannel-inl.h
# ${FOLLLY_DIR}/experimental/channels/FanoutSender.h
# ${FOLLLY_DIR}/experimental/channels/FanoutSender-inl.h
# ${FOLLLY_DIR}/experimental/channels/MaxConcurrentRateLimiter.h
# ${FOLLLY_DIR}/experimental/channels/Merge.h
# ${FOLLLY_DIR}/experimental/channels/MergeChannel.h
# ${FOLLLY_DIR}/experimental/channels/MergeChannel-inl.h
# ${FOLLLY_DIR}/experimental/channels/Merge-inl.h
# ${FOLLLY_DIR}/experimental/channels/MultiplexChannel.h
# ${FOLLLY_DIR}/experimental/channels/MultiplexChannel-inl.h
# ${FOLLLY_DIR}/experimental/channels/OnClosedException.h
# ${FOLLLY_DIR}/experimental/channels/Producer.h
# ${FOLLLY_DIR}/experimental/channels/Producer-inl.h
# ${FOLLLY_DIR}/experimental/channels/ProxyChannel.h
# ${FOLLLY_DIR}/experimental/channels/ProxyChannel-inl.h
# ${FOLLLY_DIR}/experimental/channels/RateLimiter.h
# ${FOLLLY_DIR}/experimental/channels/Transform.h
# ${FOLLLY_DIR}/experimental/channels/Transform-inl.h
# ${FOLLLY_DIR}/experimental/channels/detail/AtomicQueue.h
# ${FOLLLY_DIR}/experimental/channels/detail/ChannelBridge.h
# ${FOLLLY_DIR}/experimental/channels/detail/FunctionTraits.h
# ${FOLLLY_DIR}/experimental/channels/detail/IntrusivePtr.h
# ${FOLLLY_DIR}/experimental/channels/detail/MultiplexerTraits.h
# ${FOLLLY_DIR}/experimental/channels/detail/PointerVariant.h
# ${FOLLLY_DIR}/experimental/channels/detail/Utility.h
# ${FOLLLY_DIR}/experimental/coro/Accumulate.h
# ${FOLLLY_DIR}/experimental/coro/Accumulate-inl.h
# ${FOLLLY_DIR}/experimental/coro/AsyncGenerator.h
# ${FOLLLY_DIR}/experimental/coro/AsyncPipe.h
# ${FOLLLY_DIR}/experimental/coro/AsyncScope.h
# ${FOLLLY_DIR}/experimental/coro/AsyncStack.h
# ${FOLLLY_DIR}/experimental/coro/AutoCleanup.h
# ${FOLLLY_DIR}/experimental/coro/AutoCleanup-fwd.h
# ${FOLLLY_DIR}/experimental/coro/Baton.cpp
# ${FOLLLY_DIR}/experimental/coro/Baton.h
# ${FOLLLY_DIR}/experimental/coro/BlockingWait.h
# ${FOLLLY_DIR}/experimental/coro/BoundedQueue.h
# ${FOLLLY_DIR}/experimental/coro/Cleanup.h
# ${FOLLLY_DIR}/experimental/coro/Collect.h
# ${FOLLLY_DIR}/experimental/coro/Collect-inl.h
# ${FOLLLY_DIR}/experimental/coro/Concat.h
# ${FOLLLY_DIR}/experimental/coro/Concat-inl.h
# ${FOLLLY_DIR}/experimental/coro/Coroutine.h
# ${FOLLLY_DIR}/experimental/coro/CurrentExecutor.h
# ${FOLLLY_DIR}/experimental/coro/DetachOnCancel.h
# ${FOLLLY_DIR}/experimental/coro/Filter.h
# ${FOLLLY_DIR}/experimental/coro/Filter-inl.h
# ${FOLLLY_DIR}/experimental/coro/FutureUtil.h
# ${FOLLLY_DIR}/experimental/coro/Generator.h
# ${FOLLLY_DIR}/experimental/coro/GmockHelpers.h
# ${FOLLLY_DIR}/experimental/coro/GtestHelpers.h
# ${FOLLLY_DIR}/experimental/coro/Invoke.h
# ${FOLLLY_DIR}/experimental/coro/Merge.h
# ${FOLLLY_DIR}/experimental/coro/Merge-inl.h
# ${FOLLLY_DIR}/experimental/coro/Mutex.cpp
# ${FOLLLY_DIR}/experimental/coro/Mutex.h
# ${FOLLLY_DIR}/experimental/coro/Promise.h
# ${FOLLLY_DIR}/experimental/coro/Result.h
# ${FOLLLY_DIR}/experimental/coro/Retry.h
# ${FOLLLY_DIR}/experimental/coro/RustAdaptors.h
# ${FOLLLY_DIR}/experimental/coro/ScopeExit.h
# ${FOLLLY_DIR}/experimental/coro/SharedLock.h
# ${FOLLLY_DIR}/experimental/coro/SharedMutex.cpp
# ${FOLLLY_DIR}/experimental/coro/SharedMutex.h
# ${FOLLLY_DIR}/experimental/coro/SharedPromise.h
# ${FOLLLY_DIR}/experimental/coro/Sleep.h
# ${FOLLLY_DIR}/experimental/coro/Sleep-inl.h
# ${FOLLLY_DIR}/experimental/coro/SmallUnboundedQueue.h
# ${FOLLLY_DIR}/experimental/coro/Task.h
# ${FOLLLY_DIR}/experimental/coro/TimedWait.h
# ${FOLLLY_DIR}/experimental/coro/Timeout.h
# ${FOLLLY_DIR}/experimental/coro/Timeout-inl.h
# ${FOLLLY_DIR}/experimental/coro/Traits.h
# ${FOLLLY_DIR}/experimental/coro/Transform.h
# ${FOLLLY_DIR}/experimental/coro/Transform-inl.h
# ${FOLLLY_DIR}/experimental/coro/UnboundedQueue.h
# ${FOLLLY_DIR}/experimental/coro/ViaIfAsync.h
# ${FOLLLY_DIR}/experimental/coro/WithAsyncStack.h
# ${FOLLLY_DIR}/experimental/coro/WithCancellation.h
# ${FOLLLY_DIR}/experimental/coro/detail/Barrier.h
# ${FOLLLY_DIR}/experimental/coro/detail/BarrierTask.h
# ${FOLLLY_DIR}/experimental/coro/detail/CurrentAsyncFrame.h
# ${FOLLLY_DIR}/experimental/coro/detail/Helpers.h
# ${FOLLLY_DIR}/experimental/coro/detail/InlineTask.h
# ${FOLLLY_DIR}/experimental/coro/detail/Malloc.cpp
# ${FOLLLY_DIR}/experimental/coro/detail/Malloc.h
# ${FOLLLY_DIR}/experimental/coro/detail/ManualLifetime.h
# ${FOLLLY_DIR}/experimental/coro/detail/Traits.h
# ${FOLLLY_DIR}/experimental/crypto/Blake2xb.h
# ${FOLLLY_DIR}/experimental/crypto/LtHash.h
# ${FOLLLY_DIR}/experimental/exception_tracer/ExceptionAbi.h
# ${FOLLLY_DIR}/experimental/exception_tracer/ExceptionCounterLib.cpp
# ${FOLLLY_DIR}/experimental/exception_tracer/ExceptionCounterLib.h
# ${FOLLLY_DIR}/experimental/exception_tracer/ExceptionStackTraceLib.cpp
# ${FOLLLY_DIR}/experimental/exception_tracer/ExceptionTracer.cpp
# ${FOLLLY_DIR}/experimental/exception_tracer/ExceptionTracer.h
# ${FOLLLY_DIR}/experimental/exception_tracer/ExceptionTracerLib.cpp
# ${FOLLLY_DIR}/experimental/exception_tracer/ExceptionTracerLib.h
# ${FOLLLY_DIR}/experimental/exception_tracer/SmartExceptionStackTraceHooks.cpp
# ${FOLLLY_DIR}/experimental/exception_tracer/SmartExceptionTracer.cpp
# ${FOLLLY_DIR}/experimental/exception_tracer/SmartExceptionTracer.h
# ${FOLLLY_DIR}/experimental/exception_tracer/SmartExceptionTracerSingleton.cpp
# ${FOLLLY_DIR}/experimental/exception_tracer/SmartExceptionTracerSingleton.h
# ${FOLLLY_DIR}/experimental/exception_tracer/StackTrace.cpp
# ${FOLLLY_DIR}/experimental/exception_tracer/StackTrace.h
# ${FOLLLY_DIR}/experimental/flat_combining/FlatCombining.h
${FOLLLY_DIR}/experimental/io/AsyncBase.cpp
${FOLLLY_DIR}/experimental/io/AsyncBase.h
# ${FOLLLY_DIR}/experimental/io/AsyncIO.cpp
# ${FOLLLY_DIR}/experimental/io/AsyncIO.h
${FOLLLY_DIR}/experimental/io/AsyncIoUringSocket.cpp
${FOLLLY_DIR}/experimental/io/AsyncIoUringSocket.h
${FOLLLY_DIR}/experimental/io/AsyncIoUringSocketFactory.h
${FOLLLY_DIR}/experimental/io/Epoll.h
${FOLLLY_DIR}/experimental/io/EpollBackend.cpp
${FOLLLY_DIR}/experimental/io/EpollBackend.h
${FOLLLY_DIR}/experimental/io/EventBasePoller.cpp
${FOLLLY_DIR}/experimental/io/EventBasePoller.h
${FOLLLY_DIR}/experimental/io/FsUtil.cpp
${FOLLLY_DIR}/experimental/io/FsUtil.h
${FOLLLY_DIR}/experimental/io/HugePages.cpp
${FOLLLY_DIR}/experimental/io/HugePages.h
${FOLLLY_DIR}/experimental/io/IoUring.cpp
${FOLLLY_DIR}/experimental/io/IoUring.h
${FOLLLY_DIR}/experimental/io/IoUringBackend.cpp
${FOLLLY_DIR}/experimental/io/IoUringBackend.h
${FOLLLY_DIR}/experimental/io/IoUringBase.h
${FOLLLY_DIR}/experimental/io/IoUringEvent.cpp
${FOLLLY_DIR}/experimental/io/IoUringEvent.h
${FOLLLY_DIR}/experimental/io/IoUringEventBaseLocal.cpp
${FOLLLY_DIR}/experimental/io/IoUringEventBaseLocal.h
${FOLLLY_DIR}/experimental/io/IoUringProvidedBufferRing.cpp
${FOLLLY_DIR}/experimental/io/IoUringProvidedBufferRing.h
${FOLLLY_DIR}/experimental/io/Liburing.h
${FOLLLY_DIR}/experimental/io/MuxIOThreadPoolExecutor.cpp
${FOLLLY_DIR}/experimental/io/MuxIOThreadPoolExecutor.h
# ${FOLLLY_DIR}/experimental/io/SimpleAsyncIO.cpp
# ${FOLLLY_DIR}/experimental/io/SimpleAsyncIO.h
${FOLLLY_DIR}/experimental/io/tool/HugePageUtil.cpp
# ${FOLLLY_DIR}/experimental/observer/CoreCachedObserver.h
# ${FOLLLY_DIR}/experimental/observer/HazptrObserver.h
# ${FOLLLY_DIR}/experimental/observer/Observable.h
# ${FOLLLY_DIR}/experimental/observer/Observable-inl.h
# ${FOLLLY_DIR}/experimental/observer/Observer.h
# ${FOLLLY_DIR}/experimental/observer/Observer-inl.h
# ${FOLLLY_DIR}/experimental/observer/Observer-pre.h
# ${FOLLLY_DIR}/experimental/observer/ReadMostlyTLObserver.h
# ${FOLLLY_DIR}/experimental/observer/SimpleObservable.h
# ${FOLLLY_DIR}/experimental/observer/SimpleObservable-inl.h
# ${FOLLLY_DIR}/experimental/observer/WithJitter.h
# ${FOLLLY_DIR}/experimental/observer/WithJitter-inl.h
# ${FOLLLY_DIR}/experimental/observer/detail/Core.cpp
# ${FOLLLY_DIR}/experimental/observer/detail/Core.h
# ${FOLLLY_DIR}/experimental/observer/detail/GraphCycleDetector.h
# ${FOLLLY_DIR}/experimental/observer/detail/ObserverManager.cpp
# ${FOLLLY_DIR}/experimental/observer/detail/ObserverManager.h
# ${FOLLLY_DIR}/experimental/settings/Immutables.h
# ${FOLLLY_DIR}/experimental/settings/Settings.h
# ${FOLLLY_DIR}/experimental/settings/Types.h
# ${FOLLLY_DIR}/experimental/settings/detail/SettingsImpl.h
# ${FOLLLY_DIR}/experimental/symbolizer/Dwarf.cpp
# ${FOLLLY_DIR}/experimental/symbolizer/Dwarf.h
# ${FOLLLY_DIR}/experimental/symbolizer/DwarfImpl.cpp
# ${FOLLLY_DIR}/experimental/symbolizer/DwarfImpl.h
# ${FOLLLY_DIR}/experimental/symbolizer/DwarfLineNumberVM.cpp
# ${FOLLLY_DIR}/experimental/symbolizer/DwarfLineNumberVM.h
# ${FOLLLY_DIR}/experimental/symbolizer/DwarfSection.cpp
# ${FOLLLY_DIR}/experimental/symbolizer/DwarfSection.h
# ${FOLLLY_DIR}/experimental/symbolizer/DwarfUtil.cpp
# ${FOLLLY_DIR}/experimental/symbolizer/DwarfUtil.h
# ${FOLLLY_DIR}/experimental/symbolizer/Elf.cpp
# ${FOLLLY_DIR}/experimental/symbolizer/Elf.h
# ${FOLLLY_DIR}/experimental/symbolizer/ElfCache.cpp
# ${FOLLLY_DIR}/experimental/symbolizer/ElfCache.h
# ${FOLLLY_DIR}/experimental/symbolizer/Elf-inl.h
# ${FOLLLY_DIR}/experimental/symbolizer/LineReader.cpp
# ${FOLLLY_DIR}/experimental/symbolizer/LineReader.h
# ${FOLLLY_DIR}/experimental/symbolizer/SignalHandler.cpp
# ${FOLLLY_DIR}/experimental/symbolizer/SignalHandler.h
# ${FOLLLY_DIR}/experimental/symbolizer/StackTrace.cpp
# ${FOLLLY_DIR}/experimental/symbolizer/StackTrace.h
# ${FOLLLY_DIR}/experimental/symbolizer/SymbolizePrinter.cpp
# ${FOLLLY_DIR}/experimental/symbolizer/SymbolizePrinter.h
# ${FOLLLY_DIR}/experimental/symbolizer/SymbolizedFrame.cpp
# ${FOLLLY_DIR}/experimental/symbolizer/SymbolizedFrame.h
# ${FOLLLY_DIR}/experimental/symbolizer/Symbolizer.cpp
# ${FOLLLY_DIR}/experimental/symbolizer/Symbolizer.h
# ${FOLLLY_DIR}/experimental/symbolizer/detail/Debug.h
# ${FOLLLY_DIR}/experimental/symbolizer/tool/Addr2Line.cpp
# ${FOLLLY_DIR}/experimental/symbolizer/tool/LibSegFault.cpp
${FOLLLY_DIR}/external/farmhash/farmhash.cpp
${FOLLLY_DIR}/external/farmhash/farmhash.h
${FOLLLY_DIR}/external/fast-crc32/avx512_crc32c_v8s3x4.cpp
${FOLLLY_DIR}/external/fast-crc32/avx512_crc32c_v8s3x4.h
${FOLLLY_DIR}/external/fast-crc32/sse_crc32c_v8s3x3.cpp
${FOLLLY_DIR}/external/fast-crc32/sse_crc32c_v8s3x3.h
${FOLLLY_DIR}/ext/test_ext.cpp
${FOLLLY_DIR}/ext/test_ext.h
${FOLLLY_DIR}/fibers/AddTasks.h
${FOLLLY_DIR}/fibers/AddTasks-inl.h
${FOLLLY_DIR}/fibers/AtomicBatchDispatcher.h
${FOLLLY_DIR}/fibers/AtomicBatchDispatcher-inl.h
${FOLLLY_DIR}/fibers/BatchDispatcher.h
${FOLLLY_DIR}/fibers/BatchSemaphore.cpp
${FOLLLY_DIR}/fibers/BatchSemaphore.h
${FOLLLY_DIR}/fibers/Baton.cpp
${FOLLLY_DIR}/fibers/Baton.h
${FOLLLY_DIR}/fibers/Baton-inl.h
${FOLLLY_DIR}/fibers/BoostContextCompatibility.h
${FOLLLY_DIR}/fibers/CallOnce.h
${FOLLLY_DIR}/fibers/EventBaseLoopController.h
${FOLLLY_DIR}/fibers/EventBaseLoopController-inl.h
${FOLLLY_DIR}/fibers/ExecutorBasedLoopController.h
${FOLLLY_DIR}/fibers/ExecutorLoopController.h
${FOLLLY_DIR}/fibers/ExecutorLoopController-inl.h
${FOLLLY_DIR}/fibers/Fiber.cpp
${FOLLLY_DIR}/fibers/Fiber.h
${FOLLLY_DIR}/fibers/FiberManager.cpp
${FOLLLY_DIR}/fibers/FiberManager.h
${FOLLLY_DIR}/fibers/FiberManagerInternal.h
${FOLLLY_DIR}/fibers/FiberManagerInternal-inl.h
${FOLLLY_DIR}/fibers/FiberManagerMap.h
${FOLLLY_DIR}/fibers/FiberManagerMap-inl.h
${FOLLLY_DIR}/fibers/FiberManager-inl.h
${FOLLLY_DIR}/fibers/Fiber-inl.h
${FOLLLY_DIR}/fibers/ForEach.h
${FOLLLY_DIR}/fibers/ForEach-inl.h
${FOLLLY_DIR}/fibers/GenericBaton.h
${FOLLLY_DIR}/fibers/GuardPageAllocator.cpp
${FOLLLY_DIR}/fibers/GuardPageAllocator.h
${FOLLLY_DIR}/fibers/LoopController.h
${FOLLLY_DIR}/fibers/Promise.h
${FOLLLY_DIR}/fibers/Promise-inl.h
${FOLLLY_DIR}/fibers/Semaphore.cpp
${FOLLLY_DIR}/fibers/Semaphore.h
${FOLLLY_DIR}/fibers/SemaphoreBase.cpp
${FOLLLY_DIR}/fibers/SemaphoreBase.h
${FOLLLY_DIR}/fibers/SimpleLoopController.cpp
${FOLLLY_DIR}/fibers/SimpleLoopController.h
${FOLLLY_DIR}/fibers/TimedMutex.h
${FOLLLY_DIR}/fibers/TimedMutex-inl.h
${FOLLLY_DIR}/fibers/WhenN.h
${FOLLLY_DIR}/fibers/WhenN-inl.h
${FOLLLY_DIR}/fibers/async/Async.cpp
${FOLLLY_DIR}/fibers/async/Async.h
${FOLLLY_DIR}/fibers/async/AsyncStack.h
${FOLLLY_DIR}/fibers/async/Baton.h
${FOLLLY_DIR}/fibers/async/Collect.h
${FOLLLY_DIR}/fibers/async/Collect-inl.h
${FOLLLY_DIR}/fibers/async/FiberManager.h
${FOLLLY_DIR}/fibers/async/Future.h
${FOLLLY_DIR}/fibers/async/Promise.h
${FOLLLY_DIR}/fibers/async/Task.h
${FOLLLY_DIR}/fibers/async/WaitUtils.h
${FOLLLY_DIR}/fibers/detail/AtomicBatchDispatcher.cpp
${FOLLLY_DIR}/fibers/detail/AtomicBatchDispatcher.h
${FOLLLY_DIR}/fibers/traits.h
${FOLLLY_DIR}/functional/ApplyTuple.h
${FOLLLY_DIR}/functional/Invoke.h
${FOLLLY_DIR}/functional/Partial.h
${FOLLLY_DIR}/functional/protocol.h
${FOLLLY_DIR}/functional/traits.h
${FOLLLY_DIR}/futures/Barrier.cpp
${FOLLLY_DIR}/futures/Barrier.h
${FOLLLY_DIR}/futures/Cleanup.h
${FOLLLY_DIR}/futures/Future.cpp
${FOLLLY_DIR}/futures/Future.h
${FOLLLY_DIR}/futures/FutureSplitter.h
${FOLLLY_DIR}/futures/Future-inl.h
${FOLLLY_DIR}/futures/Future-pre.h
${FOLLLY_DIR}/futures/HeapTimekeeper.cpp
${FOLLLY_DIR}/futures/HeapTimekeeper.h
${FOLLLY_DIR}/futures/ManualTimekeeper.cpp
${FOLLLY_DIR}/futures/ManualTimekeeper.h
${FOLLLY_DIR}/futures/Portability.h
${FOLLLY_DIR}/futures/Promise.cpp
${FOLLLY_DIR}/futures/Promise.h
${FOLLLY_DIR}/futures/Promise-inl.h
${FOLLLY_DIR}/futures/Retrying.h
${FOLLLY_DIR}/futures/SharedPromise.cpp
${FOLLLY_DIR}/futures/SharedPromise.h
${FOLLLY_DIR}/futures/SharedPromise-inl.h
${FOLLLY_DIR}/futures/ThreadWheelTimekeeper.cpp
${FOLLLY_DIR}/futures/ThreadWheelTimekeeper.h
${FOLLLY_DIR}/futures/WTCallback.h
${FOLLLY_DIR}/futures/detail/Core.cpp
${FOLLLY_DIR}/futures/detail/Core.h
${FOLLLY_DIR}/futures/detail/Types.h
${FOLLLY_DIR}/gen/Base.h
${FOLLLY_DIR}/gen/Base-inl.h
${FOLLLY_DIR}/gen/Combine.h
${FOLLLY_DIR}/gen/Combine-inl.h
${FOLLLY_DIR}/gen/Core.h
${FOLLLY_DIR}/gen/Core-inl.h
${FOLLLY_DIR}/gen/File.h
${FOLLLY_DIR}/gen/File-inl.h
${FOLLLY_DIR}/gen/IStream.h
${FOLLLY_DIR}/gen/Parallel.h
${FOLLLY_DIR}/gen/ParallelMap.h
${FOLLLY_DIR}/gen/ParallelMap-inl.h
${FOLLLY_DIR}/gen/Parallel-inl.h
${FOLLLY_DIR}/gen/String.h
${FOLLLY_DIR}/gen/String-inl.h
${FOLLLY_DIR}/hash/Checksum.cpp
${FOLLLY_DIR}/hash/Checksum.h
${FOLLLY_DIR}/hash/FarmHash.h
${FOLLLY_DIR}/hash/Hash.h
${FOLLLY_DIR}/hash/MurmurHash.h
${FOLLLY_DIR}/hash/SpookyHashV1.cpp
${FOLLLY_DIR}/hash/SpookyHashV1.h
${FOLLLY_DIR}/hash/SpookyHashV2.cpp
${FOLLLY_DIR}/hash/SpookyHashV2.h
${FOLLLY_DIR}/hash/detail/ChecksumDetail.cpp
${FOLLLY_DIR}/hash/detail/ChecksumDetail.h
${FOLLLY_DIR}/hash/detail/Crc32CombineDetail.cpp
${FOLLLY_DIR}/hash/detail/Crc32cDetail.cpp
${FOLLLY_DIR}/init/Init.cpp
${FOLLLY_DIR}/init/Init.h
${FOLLLY_DIR}/init/Phase.cpp
${FOLLLY_DIR}/init/Phase.h
${FOLLLY_DIR}/io/Cursor.cpp
${FOLLLY_DIR}/io/Cursor.h
${FOLLLY_DIR}/io/Cursor-inl.h
${FOLLLY_DIR}/io/GlobalShutdownSocketSet.cpp
${FOLLLY_DIR}/io/GlobalShutdownSocketSet.h
${FOLLLY_DIR}/io/IOBuf.cpp
${FOLLLY_DIR}/io/IOBuf.h
${FOLLLY_DIR}/io/IOBufIovecBuilder.cpp
${FOLLLY_DIR}/io/IOBufIovecBuilder.h
${FOLLLY_DIR}/io/IOBufQueue.cpp
${FOLLLY_DIR}/io/IOBufQueue.h
${FOLLLY_DIR}/io/RecordIO.cpp
${FOLLLY_DIR}/io/RecordIO.h
${FOLLLY_DIR}/io/RecordIO-inl.h
${FOLLLY_DIR}/io/ShutdownSocketSet.cpp
${FOLLLY_DIR}/io/ShutdownSocketSet.h
${FOLLLY_DIR}/io/SocketOptionMap.cpp
${FOLLLY_DIR}/io/SocketOptionMap.h
${FOLLLY_DIR}/io/SocketOptionValue.cpp
${FOLLLY_DIR}/io/SocketOptionValue.h
${FOLLLY_DIR}/io/TypedIOBuf.h
${FOLLLY_DIR}/io/async/AsyncPipe.cpp
${FOLLLY_DIR}/io/async/AsyncPipe.h
${FOLLLY_DIR}/io/async/AsyncSSLSocket.cpp
${FOLLLY_DIR}/io/async/AsyncSSLSocket.h
${FOLLLY_DIR}/io/async/AsyncServerSocket.cpp
${FOLLLY_DIR}/io/async/AsyncServerSocket.h
${FOLLLY_DIR}/io/async/AsyncSignalHandler.cpp
${FOLLLY_DIR}/io/async/AsyncSignalHandler.h
${FOLLLY_DIR}/io/async/AsyncSocket.cpp
${FOLLLY_DIR}/io/async/AsyncSocket.h
${FOLLLY_DIR}/io/async/AsyncSocketBase.h
${FOLLLY_DIR}/io/async/AsyncSocketException.cpp
${FOLLLY_DIR}/io/async/AsyncSocketException.h
${FOLLLY_DIR}/io/async/AsyncSocketTransport.cpp
${FOLLLY_DIR}/io/async/AsyncSocketTransport.h
${FOLLLY_DIR}/io/async/AsyncTimeout.cpp
${FOLLLY_DIR}/io/async/AsyncTimeout.h
${FOLLLY_DIR}/io/async/AsyncTransport.h
${FOLLLY_DIR}/io/async/AsyncTransportCertificate.h
${FOLLLY_DIR}/io/async/AsyncUDPServerSocket.h
${FOLLLY_DIR}/io/async/AsyncUDPSocket.cpp
${FOLLLY_DIR}/io/async/AsyncUDPSocket.h
${FOLLLY_DIR}/io/async/AtomicNotificationQueue.h
${FOLLLY_DIR}/io/async/AtomicNotificationQueue-inl.h
${FOLLLY_DIR}/io/async/CertificateIdentityVerifier.h
${FOLLLY_DIR}/io/async/DecoratedAsyncTransportWrapper.h
${FOLLLY_DIR}/io/async/DelayedDestruction.cpp
${FOLLLY_DIR}/io/async/DelayedDestruction.h
${FOLLLY_DIR}/io/async/DelayedDestructionBase.h
${FOLLLY_DIR}/io/async/DestructorCheck.h
${FOLLLY_DIR}/io/async/EventBase.cpp
${FOLLLY_DIR}/io/async/EventBase.h
${FOLLLY_DIR}/io/async/EventBaseAtomicNotificationQueue.h
${FOLLLY_DIR}/io/async/EventBaseAtomicNotificationQueue-inl.h
${FOLLLY_DIR}/io/async/EventBaseBackendBase.cpp
${FOLLLY_DIR}/io/async/EventBaseBackendBase.h
${FOLLLY_DIR}/io/async/EventBaseLocal.cpp
${FOLLLY_DIR}/io/async/EventBaseLocal.h
${FOLLLY_DIR}/io/async/EventBaseManager.cpp
${FOLLLY_DIR}/io/async/EventBaseManager.h
${FOLLLY_DIR}/io/async/EventBaseThread.cpp
${FOLLLY_DIR}/io/async/EventBaseThread.h
${FOLLLY_DIR}/io/async/EventHandler.cpp
${FOLLLY_DIR}/io/async/EventHandler.h
${FOLLLY_DIR}/io/async/EventUtil.h
${FOLLLY_DIR}/io/async/HHWheelTimer.cpp
${FOLLLY_DIR}/io/async/HHWheelTimer.h
${FOLLLY_DIR}/io/async/HHWheelTimer-fwd.h
${FOLLLY_DIR}/io/async/NotificationQueue.h
${FOLLLY_DIR}/io/async/PasswordInFile.cpp
${FOLLLY_DIR}/io/async/PasswordInFile.h
${FOLLLY_DIR}/io/async/Request.cpp
${FOLLLY_DIR}/io/async/Request.h
${FOLLLY_DIR}/io/async/SSLContext.cpp
${FOLLLY_DIR}/io/async/SSLContext.h
${FOLLLY_DIR}/io/async/SSLOptions.cpp
${FOLLLY_DIR}/io/async/SSLOptions.h
${FOLLLY_DIR}/io/async/ScopedEventBaseThread.cpp
${FOLLLY_DIR}/io/async/ScopedEventBaseThread.h
${FOLLLY_DIR}/io/async/TerminateCancellationToken.cpp
${FOLLLY_DIR}/io/async/TerminateCancellationToken.h
${FOLLLY_DIR}/io/async/TimeoutManager.cpp
${FOLLLY_DIR}/io/async/TimeoutManager.h
${FOLLLY_DIR}/io/async/VirtualEventBase.cpp
${FOLLLY_DIR}/io/async/VirtualEventBase.h
${FOLLLY_DIR}/io/async/WriteChainAsyncTransportWrapper.h
${FOLLLY_DIR}/io/async/WriteFlags.h
${FOLLLY_DIR}/io/async/fdsock/AsyncFdSocket.cpp
${FOLLLY_DIR}/io/async/fdsock/AsyncFdSocket.h
${FOLLLY_DIR}/io/async/fdsock/SocketFds.cpp
${FOLLLY_DIR}/io/async/fdsock/SocketFds.h
${FOLLLY_DIR}/io/async/observer/AsyncSocketObserverContainer.h
${FOLLLY_DIR}/io/async/observer/AsyncSocketObserverInterface.h
${FOLLLY_DIR}/io/async/ssl/BasicTransportCertificate.h
${FOLLLY_DIR}/io/async/ssl/OpenSSLTransportCertificate.h
${FOLLLY_DIR}/io/async/ssl/OpenSSLUtils.cpp
${FOLLLY_DIR}/io/async/ssl/OpenSSLUtils.h
${FOLLLY_DIR}/io/async/ssl/SSLErrors.cpp
${FOLLLY_DIR}/io/async/ssl/SSLErrors.h
${FOLLLY_DIR}/io/async/ssl/TLSDefinitions.h
${FOLLLY_DIR}/io/coro/ServerSocket.cpp
${FOLLLY_DIR}/io/coro/ServerSocket.h
${FOLLLY_DIR}/io/coro/Transport.cpp
${FOLLLY_DIR}/io/coro/Transport.h
${FOLLLY_DIR}/io/coro/TransportCallbackBase.h
${FOLLLY_DIR}/json.h
${FOLLLY_DIR}/json/DynamicConverter.h
${FOLLLY_DIR}/json/DynamicParser.cpp
${FOLLLY_DIR}/json/DynamicParser.h
${FOLLLY_DIR}/json/DynamicParser-inl.h
${FOLLLY_DIR}/json/JSONSchema.cpp
${FOLLLY_DIR}/json/JSONSchema.h
${FOLLLY_DIR}/json/JsonMockUtil.h
${FOLLLY_DIR}/json/JsonTestUtil.cpp
${FOLLLY_DIR}/json/JsonTestUtil.h
${FOLLLY_DIR}/json/bser/Bser.h
${FOLLLY_DIR}/json/bser/Dump.cpp
${FOLLLY_DIR}/json/bser/Load.cpp
${FOLLLY_DIR}/json/dynamic.cpp
${FOLLLY_DIR}/json/dynamic.h
${FOLLLY_DIR}/json/dynamic-inl.h
${FOLLLY_DIR}/json/json.cpp
${FOLLLY_DIR}/json/json.h
${FOLLLY_DIR}/json/json_patch.cpp
${FOLLLY_DIR}/json/json_patch.h
${FOLLLY_DIR}/json/json_pointer.cpp
${FOLLLY_DIR}/json/json_pointer.h
${FOLLLY_DIR}/json/tool/JSONSchemaTester.cpp
${FOLLLY_DIR}/json_patch.h
${FOLLLY_DIR}/json_pointer.h
${FOLLLY_DIR}/lang/Access.h
${FOLLLY_DIR}/lang/Align.h
${FOLLLY_DIR}/lang/Aligned.h
${FOLLLY_DIR}/lang/Assume.h
${FOLLLY_DIR}/lang/Badge.h
${FOLLLY_DIR}/lang/Bits.h
${FOLLLY_DIR}/lang/Builtin.h
${FOLLLY_DIR}/lang/CArray.h
${FOLLLY_DIR}/lang/CString.cpp
${FOLLLY_DIR}/lang/CString.h
${FOLLLY_DIR}/lang/Cast.h
${FOLLLY_DIR}/lang/CheckedMath.h
${FOLLLY_DIR}/lang/CustomizationPoint.h
${FOLLLY_DIR}/lang/Exception.cpp
${FOLLLY_DIR}/lang/Exception.h
${FOLLLY_DIR}/lang/Extern.h
${FOLLLY_DIR}/lang/Hint.h
${FOLLLY_DIR}/lang/Hint-inl.h
${FOLLLY_DIR}/lang/Keep.h
${FOLLLY_DIR}/lang/New.h
${FOLLLY_DIR}/lang/Ordering.h
${FOLLLY_DIR}/lang/Pretty.h
${FOLLLY_DIR}/lang/PropagateConst.h
${FOLLLY_DIR}/lang/RValueReferenceWrapper.h
${FOLLLY_DIR}/lang/SafeAssert.cpp
${FOLLLY_DIR}/lang/SafeAssert.h
${FOLLLY_DIR}/lang/StaticConst.h
${FOLLLY_DIR}/lang/Thunk.h
${FOLLLY_DIR}/lang/ToAscii.cpp
${FOLLLY_DIR}/lang/ToAscii.h
${FOLLLY_DIR}/lang/TypeInfo.h
${FOLLLY_DIR}/lang/UncaughtExceptions.cpp
${FOLLLY_DIR}/lang/UncaughtExceptions.h
${FOLLLY_DIR}/logging/AsyncFileWriter.cpp
${FOLLLY_DIR}/logging/AsyncFileWriter.h
${FOLLLY_DIR}/logging/AsyncLogWriter.cpp
${FOLLLY_DIR}/logging/AsyncLogWriter.h
${FOLLLY_DIR}/logging/AutoTimer.h
${FOLLLY_DIR}/logging/BridgeFromGoogleLogging.cpp
${FOLLLY_DIR}/logging/BridgeFromGoogleLogging.h
${FOLLLY_DIR}/logging/CustomLogFormatter.cpp
${FOLLLY_DIR}/logging/CustomLogFormatter.h
${FOLLLY_DIR}/logging/FileHandlerFactory.cpp
${FOLLLY_DIR}/logging/FileHandlerFactory.h
${FOLLLY_DIR}/logging/FileWriterFactory.cpp
${FOLLLY_DIR}/logging/FileWriterFactory.h
${FOLLLY_DIR}/logging/GlogStyleFormatter.cpp
${FOLLLY_DIR}/logging/GlogStyleFormatter.h
${FOLLLY_DIR}/logging/ImmediateFileWriter.cpp
${FOLLLY_DIR}/logging/ImmediateFileWriter.h
${FOLLLY_DIR}/logging/Init.cpp
${FOLLLY_DIR}/logging/Init.h
${FOLLLY_DIR}/logging/InitWeak.cpp
${FOLLLY_DIR}/logging/LogCategory.cpp
${FOLLLY_DIR}/logging/LogCategory.h
${FOLLLY_DIR}/logging/LogCategoryConfig.cpp
${FOLLLY_DIR}/logging/LogCategoryConfig.h
${FOLLLY_DIR}/logging/LogConfig.cpp
${FOLLLY_DIR}/logging/LogConfig.h
${FOLLLY_DIR}/logging/LogConfigParser.cpp
${FOLLLY_DIR}/logging/LogConfigParser.h
${FOLLLY_DIR}/logging/LogFormatter.h
${FOLLLY_DIR}/logging/LogHandler.h
${FOLLLY_DIR}/logging/LogHandlerConfig.cpp
${FOLLLY_DIR}/logging/LogHandlerConfig.h
${FOLLLY_DIR}/logging/LogHandlerFactory.h
${FOLLLY_DIR}/logging/LogLevel.cpp
${FOLLLY_DIR}/logging/LogLevel.h
${FOLLLY_DIR}/logging/LogMessage.cpp
${FOLLLY_DIR}/logging/LogMessage.h
${FOLLLY_DIR}/logging/LogName.cpp
${FOLLLY_DIR}/logging/LogName.h
${FOLLLY_DIR}/logging/LogStream.cpp
${FOLLLY_DIR}/logging/LogStream.h
${FOLLLY_DIR}/logging/LogStreamProcessor.cpp
${FOLLLY_DIR}/logging/LogStreamProcessor.h
${FOLLLY_DIR}/logging/LogWriter.h
${FOLLLY_DIR}/logging/Logger.cpp
${FOLLLY_DIR}/logging/Logger.h
${FOLLLY_DIR}/logging/LoggerDB.cpp
${FOLLLY_DIR}/logging/LoggerDB.h
${FOLLLY_DIR}/logging/ObjectToString.cpp
${FOLLLY_DIR}/logging/ObjectToString.h
${FOLLLY_DIR}/logging/RateLimiter.cpp
${FOLLLY_DIR}/logging/RateLimiter.h
${FOLLLY_DIR}/logging/StandardLogHandler.cpp
${FOLLLY_DIR}/logging/StandardLogHandler.h
${FOLLLY_DIR}/logging/StandardLogHandlerFactory.cpp
${FOLLLY_DIR}/logging/StandardLogHandlerFactory.h
${FOLLLY_DIR}/logging/StreamHandlerFactory.cpp
${FOLLLY_DIR}/logging/StreamHandlerFactory.h
${FOLLLY_DIR}/logging/example/lib.cpp
${FOLLLY_DIR}/logging/example/lib.h
${FOLLLY_DIR}/logging/example/main.cpp
${FOLLLY_DIR}/logging/xlog.cpp
${FOLLLY_DIR}/logging/xlog.h
${FOLLLY_DIR}/memory/Arena.h
${FOLLLY_DIR}/memory/Arena-inl.h
${FOLLLY_DIR}/memory/JemallocHugePageAllocator.cpp
${FOLLLY_DIR}/memory/JemallocHugePageAllocator.h
${FOLLLY_DIR}/memory/JemallocHugePageAllocatorTest.cpp
${FOLLLY_DIR}/memory/JemallocNodumpAllocator.cpp
${FOLLLY_DIR}/memory/JemallocNodumpAllocator.h
${FOLLLY_DIR}/memory/JemallocNodumpAllocatorTest.cpp
${FOLLLY_DIR}/memory/MallctlHelper.cpp
${FOLLLY_DIR}/memory/MallctlHelper.h
${FOLLLY_DIR}/memory/Malloc.h
${FOLLLY_DIR}/memory/MemoryResource.h
${FOLLLY_DIR}/memory/ReentrantAllocator.cpp
${FOLLLY_DIR}/memory/ReentrantAllocator.h
${FOLLLY_DIR}/memory/SanitizeAddress.cpp
${FOLLLY_DIR}/memory/SanitizeAddress.h
${FOLLLY_DIR}/memory/SanitizeLeak.cpp
${FOLLLY_DIR}/memory/SanitizeLeak.h
${FOLLLY_DIR}/memory/ThreadCachedArena.cpp
${FOLLLY_DIR}/memory/ThreadCachedArena.h
${FOLLLY_DIR}/memory/UninitializedMemoryHacks.h
${FOLLLY_DIR}/memory/detail/MallocImpl.cpp
${FOLLLY_DIR}/memory/detail/MallocImpl.h
${FOLLLY_DIR}/memory/not_null.h
${FOLLLY_DIR}/memory/not_null-inl.h
${FOLLLY_DIR}/net/NetOps.cpp
${FOLLLY_DIR}/net/NetOps.h
${FOLLLY_DIR}/net/NetOpsDispatcher.cpp
${FOLLLY_DIR}/net/NetOpsDispatcher.h
${FOLLLY_DIR}/net/NetworkSocket.h
${FOLLLY_DIR}/net/TcpInfo.cpp
${FOLLLY_DIR}/net/TcpInfo.h
${FOLLLY_DIR}/net/TcpInfoDispatcher.cpp
${FOLLLY_DIR}/net/TcpInfoDispatcher.h
${FOLLLY_DIR}/net/TcpInfoTypes.h
${FOLLLY_DIR}/net/detail/SocketFileDescriptorMap.cpp
${FOLLLY_DIR}/net/detail/SocketFileDescriptorMap.h
${FOLLLY_DIR}/observer/CoreCachedObserver.h
${FOLLLY_DIR}/observer/HazptrObserver.h
${FOLLLY_DIR}/observer/Observable.h
${FOLLLY_DIR}/observer/Observable-inl.h
${FOLLLY_DIR}/observer/Observer.h
${FOLLLY_DIR}/observer/Observer-inl.h
${FOLLLY_DIR}/observer/Observer-pre.h
${FOLLLY_DIR}/observer/ReadMostlyTLObserver.h
${FOLLLY_DIR}/observer/SimpleObservable.h
${FOLLLY_DIR}/observer/SimpleObservable-inl.h
${FOLLLY_DIR}/observer/WithJitter.h
${FOLLLY_DIR}/observer/WithJitter-inl.h
${FOLLLY_DIR}/poly/Nullable.h
${FOLLLY_DIR}/poly/Regular.h
${FOLLLY_DIR}/portability/Asm.h
${FOLLLY_DIR}/portability/Atomic.h
${FOLLLY_DIR}/portability/Builtins.cpp
${FOLLLY_DIR}/portability/Builtins.h
${FOLLLY_DIR}/portability/Config.h
${FOLLLY_DIR}/portability/Constexpr.h
${FOLLLY_DIR}/portability/Dirent.cpp
${FOLLLY_DIR}/portability/Dirent.h
${FOLLLY_DIR}/portability/Event.h
${FOLLLY_DIR}/portability/Fcntl.cpp
${FOLLLY_DIR}/portability/Fcntl.h
${FOLLLY_DIR}/portability/Filesystem.cpp
${FOLLLY_DIR}/portability/Filesystem.h
${FOLLLY_DIR}/portability/FmtCompile.h
${FOLLLY_DIR}/portability/GFlags.h
${FOLLLY_DIR}/portability/GMock.h
${FOLLLY_DIR}/portability/GTest.h
${FOLLLY_DIR}/portability/IOVec.h
${FOLLLY_DIR}/portability/Libgen.cpp
${FOLLLY_DIR}/portability/Libgen.h
${FOLLLY_DIR}/portability/Libunwind.h
${FOLLLY_DIR}/portability/Malloc.cpp
${FOLLLY_DIR}/portability/Malloc.h
${FOLLLY_DIR}/portability/Math.h
${FOLLLY_DIR}/portability/Memory.h
${FOLLLY_DIR}/portability/OpenSSL.cpp
${FOLLLY_DIR}/portability/OpenSSL.h
${FOLLLY_DIR}/portability/PThread.cpp
${FOLLLY_DIR}/portability/PThread.h
${FOLLLY_DIR}/portability/Sched.cpp
${FOLLLY_DIR}/portability/Sched.h
${FOLLLY_DIR}/portability/Sockets.cpp
${FOLLLY_DIR}/portability/Sockets.h
${FOLLLY_DIR}/portability/SourceLocation.h
${FOLLLY_DIR}/portability/Stdio.cpp
${FOLLLY_DIR}/portability/Stdio.h
${FOLLLY_DIR}/portability/Stdlib.cpp
${FOLLLY_DIR}/portability/Stdlib.h
${FOLLLY_DIR}/portability/String.cpp
${FOLLLY_DIR}/portability/String.h
${FOLLLY_DIR}/portability/SysFile.cpp
${FOLLLY_DIR}/portability/SysFile.h
${FOLLLY_DIR}/portability/SysMembarrier.cpp
${FOLLLY_DIR}/portability/SysMembarrier.h
${FOLLLY_DIR}/portability/SysMman.cpp
${FOLLLY_DIR}/portability/SysMman.h
${FOLLLY_DIR}/portability/SysResource.cpp
${FOLLLY_DIR}/portability/SysResource.h
${FOLLLY_DIR}/portability/SysStat.cpp
${FOLLLY_DIR}/portability/SysStat.h
${FOLLLY_DIR}/portability/SysSyscall.h
${FOLLLY_DIR}/portability/SysTime.cpp
${FOLLLY_DIR}/portability/SysTime.h
${FOLLLY_DIR}/portability/SysTypes.h
${FOLLLY_DIR}/portability/SysUio.cpp
${FOLLLY_DIR}/portability/SysUio.h
${FOLLLY_DIR}/portability/Syslog.h
${FOLLLY_DIR}/portability/Time.cpp
${FOLLLY_DIR}/portability/Time.h
${FOLLLY_DIR}/portability/Unistd.cpp
${FOLLLY_DIR}/portability/Unistd.h
${FOLLLY_DIR}/portability/Windows.h
${FOLLLY_DIR}/portability/openat2.h
# ${FOLLLY_DIR}/python/AsyncioExecutor.h
# ${FOLLLY_DIR}/python/GILAwareManualExecutor.cpp
# ${FOLLLY_DIR}/python/GILAwareManualExecutor.h
# ${FOLLLY_DIR}/python/ProactorExecutor.cpp
# ${FOLLLY_DIR}/python/ProactorExecutor.h
# ${FOLLLY_DIR}/python/async_generator.h
# ${FOLLLY_DIR}/python/coro.h
# ${FOLLLY_DIR}/python/error.cpp
# ${FOLLLY_DIR}/python/error.h
# ${FOLLLY_DIR}/python/executor.cpp
# ${FOLLLY_DIR}/python/executor.h
# ${FOLLLY_DIR}/python/fibers.cpp
# ${FOLLLY_DIR}/python/fibers.h
# ${FOLLLY_DIR}/python/futures.h
# ${FOLLLY_DIR}/python/import.h
# ${FOLLLY_DIR}/python/iobuf.cpp
# ${FOLLLY_DIR}/python/iobuf.h
# ${FOLLLY_DIR}/python/iobuf_ext.cpp
# ${FOLLLY_DIR}/python/iobuf_ext.h
${FOLLLY_DIR}/settings/Immutables.cpp
${FOLLLY_DIR}/settings/Immutables.h
${FOLLLY_DIR}/settings/Settings.cpp
${FOLLLY_DIR}/settings/Settings.h
${FOLLLY_DIR}/settings/Types.cpp
${FOLLLY_DIR}/settings/Types.h
${FOLLLY_DIR}/settings/detail/SettingsImpl.h
${FOLLLY_DIR}/small_vector.h
${FOLLLY_DIR}/sorted_vector_types.h
${FOLLLY_DIR}/ssl/OpenSSLCertUtils.cpp
${FOLLLY_DIR}/ssl/OpenSSLCertUtils.h
${FOLLLY_DIR}/ssl/OpenSSLHash.cpp
${FOLLLY_DIR}/ssl/OpenSSLHash.h
${FOLLLY_DIR}/ssl/OpenSSLKeyUtils.cpp
${FOLLLY_DIR}/ssl/OpenSSLKeyUtils.h
${FOLLLY_DIR}/ssl/OpenSSLPtrTypes.h
${FOLLLY_DIR}/ssl/OpenSSLTicketHandler.h
${FOLLLY_DIR}/ssl/OpenSSLVersionFinder.h
${FOLLLY_DIR}/ssl/PasswordCollector.cpp
${FOLLLY_DIR}/ssl/PasswordCollector.h
${FOLLLY_DIR}/ssl/SSLSession.h
${FOLLLY_DIR}/ssl/SSLSessionManager.cpp
${FOLLLY_DIR}/ssl/SSLSessionManager.h
${FOLLLY_DIR}/ssl/detail/OpenSSLSession.cpp
${FOLLLY_DIR}/ssl/detail/OpenSSLSession.h
${FOLLLY_DIR}/stats/BucketedTimeSeries.h
${FOLLLY_DIR}/stats/BucketedTimeSeries-inl.h
${FOLLLY_DIR}/stats/DigestBuilder.h
${FOLLLY_DIR}/stats/DigestBuilder-inl.h
${FOLLLY_DIR}/stats/Histogram.h
${FOLLLY_DIR}/stats/Histogram-inl.h
${FOLLLY_DIR}/stats/MultiLevelTimeSeries.h
${FOLLLY_DIR}/stats/MultiLevelTimeSeries-inl.h
${FOLLLY_DIR}/stats/QuantileEstimator.cpp
${FOLLLY_DIR}/stats/QuantileEstimator.h
${FOLLLY_DIR}/stats/QuantileEstimator-inl.h
${FOLLLY_DIR}/stats/QuantileHistogram.h
${FOLLLY_DIR}/stats/QuantileHistogram-inl.h
${FOLLLY_DIR}/stats/StreamingStats.h
${FOLLLY_DIR}/stats/TDigest.cpp
${FOLLLY_DIR}/stats/TDigest.h
${FOLLLY_DIR}/stats/TimeseriesHistogram.h
${FOLLLY_DIR}/stats/TimeseriesHistogram-inl.h
${FOLLLY_DIR}/stats/detail/Bucket.h
${FOLLLY_DIR}/stats/detail/BufferedStat.h
${FOLLLY_DIR}/stats/detail/BufferedStat-inl.h
${FOLLLY_DIR}/stats/detail/DoubleRadixSort.cpp
${FOLLLY_DIR}/stats/detail/DoubleRadixSort.h
${FOLLLY_DIR}/stats/detail/SlidingWindow.h
${FOLLLY_DIR}/stats/detail/SlidingWindow-inl.h
${FOLLLY_DIR}/stop_watch.h
${FOLLLY_DIR}/synchronization/AsymmetricThreadFence.cpp
${FOLLLY_DIR}/synchronization/AsymmetricThreadFence.h
${FOLLLY_DIR}/synchronization/AtomicNotification.cpp
${FOLLLY_DIR}/synchronization/AtomicNotification.h
${FOLLLY_DIR}/synchronization/AtomicNotification-inl.h
${FOLLLY_DIR}/synchronization/AtomicRef.h
${FOLLLY_DIR}/synchronization/AtomicStruct.h
${FOLLLY_DIR}/synchronization/AtomicUtil.h
${FOLLLY_DIR}/synchronization/AtomicUtil-inl.h
${FOLLLY_DIR}/synchronization/Baton.h
${FOLLLY_DIR}/synchronization/CallOnce.h
${FOLLLY_DIR}/synchronization/DelayedInit.h
${FOLLLY_DIR}/synchronization/DistributedMutex.cpp
${FOLLLY_DIR}/synchronization/DistributedMutex.h
${FOLLLY_DIR}/synchronization/DistributedMutex-inl.h
${FOLLLY_DIR}/synchronization/EventCount.h
${FOLLLY_DIR}/synchronization/FlatCombining.h
${FOLLLY_DIR}/synchronization/Hazptr.cpp
${FOLLLY_DIR}/synchronization/Hazptr.h
${FOLLLY_DIR}/synchronization/HazptrDomain.cpp
${FOLLLY_DIR}/synchronization/HazptrDomain.h
${FOLLLY_DIR}/synchronization/HazptrHolder.h
${FOLLLY_DIR}/synchronization/HazptrObj.h
${FOLLLY_DIR}/synchronization/HazptrObjLinked.h
${FOLLLY_DIR}/synchronization/HazptrRec.h
${FOLLLY_DIR}/synchronization/HazptrThrLocal.h
${FOLLLY_DIR}/synchronization/HazptrThreadPoolExecutor.cpp
${FOLLLY_DIR}/synchronization/HazptrThreadPoolExecutor.h
${FOLLLY_DIR}/synchronization/Hazptr-fwd.h
${FOLLLY_DIR}/synchronization/Latch.h
${FOLLLY_DIR}/synchronization/LifoSem.h
${FOLLLY_DIR}/synchronization/Lock.h
${FOLLLY_DIR}/synchronization/MicroSpinLock.h
${FOLLLY_DIR}/synchronization/NativeSemaphore.h
${FOLLLY_DIR}/synchronization/ParkingLot.cpp
${FOLLLY_DIR}/synchronization/ParkingLot.h
${FOLLLY_DIR}/synchronization/PicoSpinLock.h
${FOLLLY_DIR}/synchronization/RWSpinLock.h
${FOLLLY_DIR}/synchronization/Rcu.cpp
${FOLLLY_DIR}/synchronization/Rcu.h
${FOLLLY_DIR}/synchronization/RelaxedAtomic.h
${FOLLLY_DIR}/synchronization/SanitizeThread.cpp
${FOLLLY_DIR}/synchronization/SanitizeThread.h
${FOLLLY_DIR}/synchronization/SaturatingSemaphore.h
${FOLLLY_DIR}/synchronization/SmallLocks.h
${FOLLLY_DIR}/synchronization/ThrottledLifoSem.h
${FOLLLY_DIR}/synchronization/Utility.h
${FOLLLY_DIR}/synchronization/WaitOptions.cpp
${FOLLLY_DIR}/synchronization/WaitOptions.h
${FOLLLY_DIR}/synchronization/detail/AtomicUtils.h
${FOLLLY_DIR}/synchronization/detail/Hardware.cpp
${FOLLLY_DIR}/synchronization/detail/Hardware.h
${FOLLLY_DIR}/synchronization/detail/HazptrUtils.h
${FOLLLY_DIR}/synchronization/detail/InlineFunctionRef.h
${FOLLLY_DIR}/synchronization/detail/Sleeper.cpp
${FOLLLY_DIR}/synchronization/detail/Sleeper.h
${FOLLLY_DIR}/synchronization/detail/Spin.h
${FOLLLY_DIR}/synchronization/detail/ThreadCachedLists.h
${FOLLLY_DIR}/synchronization/detail/ThreadCachedReaders.h
${FOLLLY_DIR}/synchronization/detail/ThreadCachedTag.h
${FOLLLY_DIR}/synchronization/example/HazptrLockFreeLIFO.h
${FOLLLY_DIR}/synchronization/example/HazptrSWMRSet.h
${FOLLLY_DIR}/synchronization/example/HazptrWideCAS.h
${FOLLLY_DIR}/system/AtFork.cpp
${FOLLLY_DIR}/system/AtFork.h
${FOLLLY_DIR}/system/EnvUtil.cpp
${FOLLLY_DIR}/system/EnvUtil.h
${FOLLLY_DIR}/system/HardwareConcurrency.cpp
${FOLLLY_DIR}/system/HardwareConcurrency.h
${FOLLLY_DIR}/system/MemoryMapping.cpp
${FOLLLY_DIR}/system/MemoryMapping.h
${FOLLLY_DIR}/system/Pid.cpp
${FOLLLY_DIR}/system/Pid.h
${FOLLLY_DIR}/system/Shell.cpp
${FOLLLY_DIR}/system/Shell.h
${FOLLLY_DIR}/system/ThreadId.cpp
${FOLLLY_DIR}/system/ThreadId.h
${FOLLLY_DIR}/system/ThreadName.cpp
${FOLLLY_DIR}/system/ThreadName.h
# ${FOLLLY_DIR}/testing/TestUtil.cpp
# ${FOLLLY_DIR}/testing/TestUtil.h
# ${FOLLLY_DIR}/tool/BenchmarkCompare.cpp
${FOLLLY_DIR}/tracing/AsyncStack.cpp
${FOLLLY_DIR}/tracing/AsyncStack.h
${FOLLLY_DIR}/tracing/AsyncStack-inl.h
${FOLLLY_DIR}/tracing/ScopedTraceSection.h
${FOLLLY_DIR}/tracing/StaticTracepoint.h
${FOLLLY_DIR}/tracing/StaticTracepoint-ELF.h
)

add_library(folly OBJECT
  ${SRCS}
  ${CMAKE_CURRENT_SOURCE_DIR}/folly/folly-config.h
)

target_compile_definitions(folly PRIVATE FOLLY_SCOPE_DEFINITION)

target_link_libraries(folly PRIVATE 
    libevent
    boost::headers_only
    double-conversion
    gflags::gflags
    glog
    fmt
    gtest)

if (ENABLE_JEMALLOC)
  target_link_libraries(folly PRIVATE jemalloc)
endif ()

if (OS_LINUX)
  target_link_libraries(folly PRIVATE liburing)
endif ()

target_include_directories(folly PRIVATE ${JEMALLOC_INCLUDE_DIR})
target_include_directories(folly SYSTEM BEFORE PUBLIC $<TARGET_PROPERTY:glog,INTERFACE_INCLUDE_DIRECTORIES>)
target_include_directories(folly SYSTEM BEFORE PUBLIC $<TARGET_PROPERTY:fmt,INTERFACE_INCLUDE_DIRECTORIES>)
target_include_directories(folly SYSTEM BEFORE PUBLIC $<TARGET_PROPERTY:libevent,INTERFACE_INCLUDE_DIRECTORIES>)
target_include_directories(folly SYSTEM BEFORE PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
target_include_directories(folly SYSTEM BEFORE PUBLIC $<BUILD_INTERFACE:${LIBDIR}>)
