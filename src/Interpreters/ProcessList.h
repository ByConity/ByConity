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

#include <Core/Defines.h>
#include <DataStreams/BlockIO.h>
#include <IO/Progress.h>
#include <Interpreters/CancellationCode.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/QueryPriorities.h>
#include <ResourceGroup/IResourceGroupManager.h>
#include <Storages/IStorage_fwd.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <Poco/Condition.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/Throttler.h>
#include <parallel_hashmap/phmap.h>

#include <list>
#include <map>
#include <memory>
#include <unordered_map>


namespace CurrentMetrics
{
    extern const Metric Query;
    extern const Metric DefaultQuery;
}

namespace DB
{

struct Settings;
class IAST;

struct ProcessListForUser;
class QueryStatus;
class ThreadStatus;
class ProcessListEntry;

namespace ProcessListHelper
{
    enum QueryTypeImpl : uint32_t
    {
        Default = 0,
        Insert = 1,
        System = 2,
        Proxy = 3,
    };

    enum SubQueryTypeImpl : uint32_t
    {
        Simple = 0,
        Complex = 1,
    };

    constexpr auto toString(QueryTypeImpl type)
    {
        switch (type)
        {
            case QueryTypeImpl::Default:
                return "Default";
            case QueryTypeImpl::Insert:
                return "Insert";
            case QueryTypeImpl::System:
                return "System";
            case QueryTypeImpl::Proxy:
                return "Proxy";
            default:
                return "Unknown";
        }
    }

    constexpr auto toString(SubQueryTypeImpl type)
    {
        switch (type)
        {
            case SubQueryTypeImpl::Simple:
                return "Simple";
            case SubQueryTypeImpl::Complex:
                return "Complex";
            default:
                return "Unknown";
        }
    }
}

using ProcessListQueryType = ProcessListHelper::QueryTypeImpl;
using ProcessListSubQueryType = ProcessListHelper::SubQueryTypeImpl;
constexpr uint32_t ProcessListQueryTypeNum = uint32_t(ProcessListQueryType::Proxy) + 1;

/** List of currently executing queries.
  * Also implements limit on their number.
  */

/** Information of process list element.
  * To output in SHOW PROCESSLIST query. Does not contain any complex objects, that do something on copy or destructor.
  */
struct QueryStatusInfo
{
    String query;
    double elapsed_seconds;
    size_t read_rows;
    size_t read_bytes;
    size_t total_rows;
    size_t disk_cache_read_bytes;
    size_t written_rows;
    size_t written_bytes;
    size_t written_duration;
    Int64 memory_usage;
    Int64 peak_memory_usage;
    String operator_level;
    ClientInfo client_info;
    bool is_cancelled;

    /// Optional fields, filled by query
    std::vector<UInt64> thread_ids;
    std::shared_ptr<ProfileEvents::Counters::Snapshot> profile_counters;
    String max_io_time_thread_name;
    uint64_t max_io_time_thread_ms;
    std::shared_ptr<ProfileEvents::Counters::Snapshot> max_io_thread_profile_counters;
    std::shared_ptr<Settings> query_settings;
    std::string current_database;
    String query_rewrite_by_view;
};

/// Query and information about its execution.
class QueryStatus : public WithContext
{
protected:
    friend class ProcessList;
    friend class ThreadStatus;
    friend class CurrentThread;
    friend class ProcessListEntry;

    ProcessListQueryType type;
    String query;
    ClientInfo client_info;

    /// Info about all threads involved in query execution
    ThreadGroupStatusPtr thread_group;

    Stopwatch watch;

    /// Progress of input stream
    Progress progress_in;
    /// Progress of output stream
    Progress progress_out;

    /// Used to externally check for the query time limits
    /// They are saved in the constructor to limit the overhead of each call to checkTimeLimit()
    ExecutionSpeedLimits limits;
    OverflowMode overflow_mode;

    QueryPriorities::Handle priority_handle;
    IResourceGroup::Handle resource_group_handle;

    CurrentMetrics::Increment num_queries_increment{CurrentMetrics::Query};
    CurrentMetrics::Increment query_type_increment{CurrentMetrics::DefaultQuery};
    bool is_unlimited;

    /// True if query cancellation is in progress right now
    /// ProcessListEntry should not be destroyed if is_cancelling is true
    /// Flag changes is synced with ProcessListBase::mutex and notified with ProcessList::cancelled_cv
    bool is_cancelling { false };

    /// Highest bit indicates the kill signal is sended by query itself (internal)
    static constexpr UInt8 INTERNAL_KILL_BIT = 0x80;
    /// Lowest bit indicates the kill signal is sended by exteranl
    static constexpr UInt8 EXTERNAL_KILL_BIT = 0x01;
    /// KILL was send to the query
    std::atomic<UInt8> is_killed{0};

    void setUserProcessList(ProcessListForUser * user_process_list_);
    /// Be careful using it. For example, queries field of ProcessListForUser could be modified concurrently.
    const ProcessListForUser * getUserProcessList() const { return user_process_list; }


    mutable bthread::Mutex executors_mutex;

    /// Array of PipelineExecutors to be cancelled when a cancelQuery is received
    std::vector<PipelineExecutor *> executors;

    mutable bthread::Mutex query_streams_mutex;

    /// Streams with query results, point to BlockIO from executeQuery()
    /// This declaration is compatible with notes about BlockIO::process_list_entry:
    ///  there are no cyclic dependencies: BlockIO::in,out point to objects inside ProcessListElement (not whole object)
    BlockInputStreamPtr query_stream_in;
    BlockOutputStreamPtr query_stream_out;

    enum QueryStreamsStatus
    {
        NotInitialized,
        Initialized,
        Released
    };

    QueryStreamsStatus query_streams_status{NotInitialized};

    ProcessListForUser * user_process_list = nullptr;

    String query_rewrite_by_view;

    String pipeline_info;
    /// for storing the graphs of ASTs, plans, and pipelines
    /// [graph name, graphviz format string]
    std::shared_ptr<std::vector<std::pair<String, String>>> graphviz;

public:

    QueryStatus(
        ContextPtr context_,
        const String & query_,
        const ClientInfo & client_info_,
        QueryPriorities::Handle && priority_handle_,
        IResourceGroup::Handle && resource_group_handle_,
        CurrentMetrics::Metric & query_type_metric,
        const bool is_unlimited_);

    ~QueryStatus();

    auto getType() const
    {
        return type;
    }

    const ClientInfo & getClientInfo() const
    {
        return client_info;
    }

    ProgressValues getProgressIn() const
    {
        return progress_in.getValues();
    }

    ProgressValues getProgressOut() const
    {
        return progress_out.getValues();
    }

    ThrottlerPtr getUserNetworkThrottler();

    bool isUnlimitedQuery() const
    {
        return is_unlimited;
    }

    bool updateProgressIn(const Progress & value)
    {
        CurrentThread::updateProgressIn(value);
        progress_in.incrementPiecewiseAtomically(value);

        if (priority_handle)
            priority_handle->waitIfNeed(std::chrono::seconds(1));        /// NOTE Could make timeout customizable.

        return !is_killed.load(std::memory_order_relaxed);
    }

    bool updateProgressOut(const Progress & value)
    {
        CurrentThread::updateProgressOut(value);
        progress_out.incrementPiecewiseAtomically(value);

        return !is_killed.load(std::memory_order_relaxed);
    }

    QueryStatusInfo getInfo(bool get_thread_list = false, bool get_profile_events = false, bool get_settings = false) const;

    void addGraphviz(const String& name, const String& graph) {
        graphviz->emplace_back(name, graph);
    }

    std::shared_ptr<std::vector<std::pair<String, String>>> getGraphviz() const {
        return graphviz;
    }

    /// Copies pointers to in/out streams
    void setQueryStreams(const BlockIO & io);

    void setQueryRewriteByView(const String & rewrite_query);

    /// Frees in/out streams
    void releaseQueryStreams();

    /// It means that ProcessListEntry still exists, but stream was already destroyed
    bool streamsAreReleased();

    /// Get query in/out pointers from BlockIO
    bool tryGetQueryStreams(BlockInputStreamPtr & in, BlockOutputStreamPtr & out) const;

    CancellationCode cancelQuery(bool kill, bool internal);

    bool isKilled() const { return is_killed; }

    bool isInternalKill() const { return is_killed & 0x80; }

    /// Adds a pipeline to the QueryStatus
    void addPipelineExecutor(PipelineExecutor * e);

    /// Removes a pipeline to the QueryStatus
    void removePipelineExecutor(PipelineExecutor * e);

    /// Dump pipeline info to `opeator_level` of QueryStatusInfo
    void dumpPipelineInfo(PipelineExecutor * e);

    bool checkCpuTimeLimit(String node_name);
    /// Checks the query time limits (cancelled or timeout)
    bool checkTimeLimit();
    [[noreturn]] void throwKilledException();
    /// Same as checkTimeLimit but it never throws
    [[nodiscard]] bool checkTimeLimitSoft();
    Int64 getUsedMemory() const { return thread_group == nullptr ? 0 : thread_group->memory_tracker.get(); }

};


/// Information of process list for user.
struct ProcessListForUserInfo
{
    Int64 memory_usage;
    Int64 peak_memory_usage;

    // Optional field, filled by request.
    std::shared_ptr<ProfileEvents::Counters::Snapshot> profile_counters;
};


/// Data about queries for one user.
struct ProcessListForUser
{
    ProcessListForUser();
    using Element = std::shared_ptr<QueryStatus>;
    using Container = phmap::flat_hash_map<std::string, Element,
        phmap::priv::hash_default_hash<std::string>,
        phmap::priv::hash_default_eq<std::string>,
        std::allocator<std::pair<std::string, Element>>>;

    /// query_id -> ProcessListElement(s). There can be multiple queries with the same query_id as long as all queries except one are cancelled.
    Container queries;

    ProfileEvents::Counters user_performance_counters{VariableContext::User, &ProfileEvents::global_counters};
    /// Limit and counter for memory of all simultaneously running queries of single user.
    MemoryTracker user_memory_tracker{VariableContext::User};

    /// Count network usage for all simultaneously running queries of single user.
    ThrottlerPtr user_throttler;

    ProcessListForUserInfo getInfo(bool get_profile_events = false) const;

    /// Clears MemoryTracker for the user.
    /// Sometimes it is important to reset the MemoryTracker, because it may accumulate skew
    ///  due to the fact that there are cases when memory can be allocated while processing the query, but released later.
    /// Clears network bandwidth Throttler, so it will not count periods of inactivity.
    void resetTrackers()
    {
        user_memory_tracker.reset();
        if (user_throttler)
            user_throttler.reset();
    }
};


class ProcessList;


/// Keeps iterator to process list and removes element in destructor.
class ProcessListEntry
{
private:
    ProcessList & parent;
    std::shared_ptr<QueryStatus> it;

protected:
    friend class PlanSegmentProcessList;
    std::shared_ptr<QueryStatus> getPtr() { return it; }

public:
    ProcessListEntry(ProcessList & parent_, QueryStatus * it_)
        : parent(parent_), it(it_) {}

    ProcessListEntry(ProcessList & parent_, std::shared_ptr<QueryStatus> it_)
        : parent(parent_), it(it_) {}

    ~ProcessListEntry();

    std::shared_ptr<QueryStatus> operator->() { return it; }
    std::shared_ptr<const QueryStatus> operator->() const { return it; }

    QueryStatus & get() { return *it; }
    const QueryStatus & get() const { return *it; }
};


class ProcessList
{
public:
    using Element = std::shared_ptr<QueryStatus>;
    using UserToQueriesElement = std::shared_ptr<ProcessListForUser>;
    using Entry = ProcessListEntry;

    using Container = phmap::parallel_flat_hash_map<std::string, Element,
        phmap::priv::hash_default_hash<std::string>,
        phmap::priv::hash_default_eq<std::string>,
        std::allocator<std::pair<std::string, Element>>,
        4, bthread::Mutex>;

    /// list, for iterators not to invalidate. NOTE: could replace with cyclic buffer, but not worth.
    using Info = std::vector<QueryStatusInfo>;
    using UserInfo = std::unordered_map<String, ProcessListForUserInfo>;

    /// User -> queries
    using UserToQueries = std::unordered_map<String, ProcessListForUser>;

protected:
    friend class ProcessListEntry;

    mutable bthread::Mutex mutex;
    mutable bthread::ConditionVariable have_space;        /// Number of currently running queries has become less than maximum.

    /// List of queries
    Container processes;
    /// Notify about cancelled queries (done with ProcessListBase::mutex acquired).
    mutable bthread::ConditionVariable cancelled_cv;

    size_t max_size = 0;        /// 0 means no limit. Otherwise, when limit exceeded, an exception is thrown.

    /// Stores per-user info: queries, statistics and limits
    UserToQueries user_to_queries;

    /// Stores info about queries grouped by their priority
    QueryPriorities priorities;

    /// Limit network bandwidth for all users
    ThrottlerPtr total_network_throttler;

    /// Limit network bandwidth for hdfs download
    ThrottlerPtr hdfs_download_network_throttler;

    /// Call under lock. Finds process with specified current_user and current_query_id.
    std::shared_ptr<QueryStatus> tryGetProcessListElement(const String & current_query_id, const String & current_user);

public:
    using EntryPtr = std::shared_ptr<ProcessListEntry>;

    /** Register running query. Returns refcounted object, that will remove element from list in destructor.
      * If too many running queries - wait for not more than specified (see settings) amount of time.
      * If timeout is passed - throw an exception.
      * Don't count KILL QUERY queries.
      */
    EntryPtr insert(const String & query_, const IAST * ast, ContextPtr query_context, bool force = false);

    void checkRunningQuery(ContextPtr query_context, bool is_unlimited_query, bool force = false);

    /// Number of currently executing queries.
    size_t size() const { return processes.size(); }

    /// Get current state of process list.
    Info getInfo(bool get_thread_list = false, bool get_profile_events = false, bool get_settings = false) const;

    /// Get current state of process list per user.
    UserInfo getUserInfo(bool get_profile_events = false) const;

    void setMaxSize(size_t max_size_)
    {
        std::lock_guard lock(mutex);
        max_size = max_size_;
        processes.reserve(max_size * 2);
    }

    /// Try call cancel() for input and output streams of query with specified id and user
    CancellationCode sendCancelToQuery(const String & current_query_id, const String & current_user, bool kill = false);

    void killAllQueries();

    ThrottlerPtr getHDFSDownloadThrottler() const { return hdfs_download_network_throttler; }
};

}
