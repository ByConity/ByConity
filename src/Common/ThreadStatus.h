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

#pragma once

#include <Common/Logger.h>
#include <Core/SettingsEnums.h>
#include <Interpreters/Context_fwd.h>
#include <IO/Progress.h>
#include <Common/MemoryTracker.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/ProfileEvents.h>
#include <common/StringRef.h>

#include <boost/noncopyable.hpp>
#include <bthread/mutex.h>

#include <functional>
#include <map>
#include <stack>
#include <memory>


namespace Poco
{
    class Logger;
}


namespace DB
{

class QueryStatus;
class ThreadStatus;
class QueryProfilerReal;
class QueryProfilerCpu;
class QueryThreadLog;
struct OpenTelemetrySpanHolder;
class TasksStatsCounters;
struct RUsageCounters;
struct PerfEventsCounters;
class TaskStatsInfoGetter;
class InternalTextLogsQueue;
using InternalTextLogsQueuePtr = std::shared_ptr<InternalTextLogsQueue>;
using InternalTextLogsQueueWeakPtr = std::weak_ptr<InternalTextLogsQueue>;


/** Thread group is a collection of threads dedicated to single task
  * (query or other process like background merge).
  *
  * ProfileEvents (counters) from a thread are propagated to thread group.
  *
  * Create via CurrentThread::initializeQuery (for queries) or directly (for various background tasks).
  * Use via CurrentThread::getGroup.
  */
class ThreadGroupStatus;
using ThreadGroupStatusPtr = std::shared_ptr<ThreadGroupStatus>;
class ThreadGroupStatus
{
public:
    ThreadGroupStatus(MemoryTracker * memory_tracker_ = nullptr)
        : memory_tracker(memory_tracker_ ? *memory_tracker_ : private_memory_tracker)
    {
    }

    mutable bthread::Mutex mutex;

    ProfileEvents::Counters performance_counters{VariableContext::Process};
    MemoryTracker private_memory_tracker{VariableContext::Process};
    MemoryTracker & memory_tracker;
    /// for MaxIOThreadProfileEvents
    std::shared_ptr<ProfileEvents::Counters::Snapshot> max_io_thread_profile_counters;
    UInt64 max_io_time_us{0};
    uint64_t max_io_time_thread_ms{0};
    String max_io_time_thread_name;

    ContextWeakPtr query_context;
    ContextWeakPtr global_context;

    InternalTextLogsQueueWeakPtr logs_queue_ptr;
    std::function<void()> fatal_error_callback;

    std::vector<UInt64> thread_ids;

    static ThreadGroupStatusPtr createForBackgroundProcess(ContextPtr storage_context);

    /// The first thread created this thread group
    UInt64 master_thread_id = 0;

    LogsLevel client_logs_level = LogsLevel::none;

    String query;
    UInt64 normalized_query_hash = 0;
};


/**
 * Since merge is executed with multiple threads, this class
 * switches the parent MemoryTracker as part of the thread group to account all the memory used.
 */
class ThreadGroupSwitcher : private boost::noncopyable
{
public:
    explicit ThreadGroupSwitcher(ThreadGroupStatusPtr thread_group);
    ~ThreadGroupSwitcher();

private:
    ThreadGroupStatusPtr prev_thread_group;
};


extern thread_local ThreadStatus * current_thread;

/** Encapsulates all per-thread info (ProfileEvents, MemoryTracker, query_id, query context, etc.).
  * The object must be created in thread function and destroyed in the same thread before the exit.
  * It is accessed through thread-local pointer.
  *
  * This object should be used only via "CurrentThread", see CurrentThread.h
  */
class ThreadStatus : public boost::noncopyable
{
public:
    /// Linux's PID (or TGID) (the same id is shown by ps util)
    const UInt64 thread_id = 0;
    /// Also called "nice" value. If it was changed to non-zero (when attaching query) - will be reset to zero when query is detached.
    Int32 os_thread_priority = 0;

    /// TODO: merge them into common entity
    ProfileEvents::Counters performance_counters{VariableContext::Thread};
    MemoryTracker memory_tracker{VariableContext::Thread};

    /// Small amount of untracked memory (per thread atomic-less counter)
    Int64 untracked_memory = 0;
    /// Each thread could new/delete memory in range of (-untracked_memory_limit, untracked_memory_limit) without access to common counters.
    Int64 untracked_memory_limit = 4 * 1024 * 1024;

    /// Statistics of read and write rows/bytes
    Progress progress_in;
    Progress progress_out;

    using Deleter = std::function<void()>;
    Deleter deleter;

    // This is the current most-derived OpenTelemetry span for this thread. It
    // can be changed throughout the query execution, whenever we enter a new
    // span or exit it. See OpenTelemetrySpanHolder that is normally responsible
    // for these changes.
    OpenTelemetryTraceContext thread_trace_context;

    enum OverflowFlag {Date = 1, Float = 2, Integer = 4, Decimal = 8, Time = 16};
    void setOverflow(OverflowFlag flag)
    {
        overflow_bits |= flag;
    }

    void unsetOverflow(OverflowFlag flag)
    {
        overflow_bits &= ~flag;
    }

    bool getOverflow(OverflowFlag flag) const
    {
        return overflow_bits & flag;
    }

    bool getOverflow() const
    {
        return overflow_bits;
    }

    void resetOverflow()
    {
        overflow_bits = 0;
    }

protected:
    /// Set the bit if the value is overflow (out of range) during parsing
    /// Used under mysql dialect for convert overflow to null
    /// TODO(fredwang) remove this flag --- return the flag from the date deserialization/parsing functions directly.
    uint8_t overflow_bits = 0;

    ThreadGroupStatusPtr thread_group;

    std::atomic<int> thread_state{ThreadState::DetachedFromQuery};

    /// Is set once
    ContextWeakPtr global_context;
    /// Use it only from current thread
    ContextWeakPtr query_context;

    String query_id;
    UInt64 xid = 0;

    /// A logs queue used by TCPHandler to pass logs to a client
    InternalTextLogsQueueWeakPtr logs_queue_ptr;

    bool performance_counters_finalized = false;
    UInt64 query_start_time_nanoseconds = 0;
    UInt64 query_start_time_microseconds = 0;
    time_t query_start_time = 0;
    size_t queries_started = 0;

    // CPU and Real time query profilers
    std::unique_ptr<QueryProfilerReal> query_profiler_real;
    std::unique_ptr<QueryProfilerCpu> query_profiler_cpu;

    LoggerPtr log = nullptr;

    friend class CurrentThread;

    /// Use ptr not to add extra dependencies in the header
    std::unique_ptr<RUsageCounters> last_rusage;
    std::unique_ptr<TasksStatsCounters> taskstats;

    /// Is used to send logs from logs_queue to client in case of fatal errors.
    std::function<void()> fatal_error_callback;

    /// Used to save all the involved queries tenant_id
    std::stack<String> tenant_ids;

    /// when we handle external table parsing, we need to disable tenant logic temporarily
    bool disable_tenant = false;

public:
    ThreadStatus();
    ~ThreadStatus();

    ThreadGroupStatusPtr getThreadGroup() const
    {
        return thread_group;
    }

    enum ThreadState
    {
        DetachedFromQuery = 0,  /// We just created thread or it is a background thread
        AttachedToQuery,        /// Thread executes enqueued query
        Died,                   /// Thread does not exist
    };

    int getCurrentState() const
    {
        return thread_state.load(std::memory_order_relaxed);
    }

    StringRef getQueryId() const
    {
        return query_id;
    }

    UInt64 getTransactionId() const
    {
        return xid;
    }

    void setTransactionId(UInt64 xid_)
    {
        xid = xid_;
    }

    bool isEnableTenant() const
    {
        return !disable_tenant;
    }

    void disableTenant()
    {
        disable_tenant = true;
    }

    void enableTenant()
    {
        disable_tenant = false;
    }

    String getTenantId() const
    {
        String result;
        if (!tenant_ids.empty())
            return tenant_ids.top();
        return result;
    }

    void pushTenantId(const String& new_tenant_id)
    {
        tenant_ids.push(new_tenant_id);
    }

    void popTenantId()
    {
        if (!tenant_ids.empty())
            tenant_ids.pop();
    }

    auto getQueryContext() const
    {
        return query_context.lock();
    }

    ContextPtr getGlobalContext() const
    {
        return global_context.lock();
    }

    /// Starts new query and create new thread group for it, current thread becomes master thread of the query
    void initializeQuery(MemoryTracker * memory_tracker_ = nullptr);

    /// Attaches slave thread to existing thread group
    void attachQuery(const ThreadGroupStatusPtr & thread_group_, bool check_detached = true);

    InternalTextLogsQueuePtr getInternalTextLogsQueue() const
    {
        return thread_state == Died ? nullptr : logs_queue_ptr.lock();
    }

    void attachInternalTextLogsQueue(const InternalTextLogsQueuePtr & logs_queue,
                                     LogsLevel client_logs_level);

    /// Callback that is used to trigger sending fatal error messages to client.
    void setFatalErrorCallback(std::function<void()> callback);
    void onFatalError();

    /// Sets query context for current master thread and its thread group
    /// NOTE: query_context have to be alive until detachQuery() is called
    void attachQueryContext(ContextPtr query_context);

    /// Update several ProfileEvents counters
    void updatePerformanceCounters();

    /// Update ProfileEvents and dumps info to system.query_thread_log
    void finalizePerformanceCounters();

    /// Detaches thread from the thread group and the query, dumps performance counters if they have not been dumped
    void detachQuery(bool exit_if_already_detached = false, bool thread_exits = false);

    void tryUpdateMaxIOThreadProfile(bool use_async_read);
    void flushUntrackedMemory();

protected:
    void applyQuerySettings();

    void initPerformanceCounters();

    void initQueryProfiler();

    void finalizeQueryProfiler();

    void logToQueryThreadLog(QueryThreadLog & thread_log, const String & current_database, std::chrono::time_point<std::chrono::system_clock> now);

    void assertState(const std::initializer_list<int> & permitted_states, const char * description = nullptr) const;


private:
    void setupState(const ThreadGroupStatusPtr & thread_group_);
};

/**
 * Creates ThreadStatus for the main thread.
 */
class MainThreadStatus : public ThreadStatus
{
public:
    static MainThreadStatus & getInstance();
    static ThreadStatus * get() { return main_thread; }
    static bool isMainThread() { return main_thread == current_thread; }

    ~MainThreadStatus();

private:
    MainThreadStatus();

    static ThreadStatus * main_thread;
};

}
