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

#include <Processors/IProcessor.h>
#include <Processors/Executors/PollingQueue.h>
#include <Processors/Executors/ThreadsQueue.h>
#include <Processors/Executors/TasksQueue.h>
#include <Processors/Executors/ExecutingGraph.h>
#include <Common/ThreadPool.h>
#include <Common/EventCounter.h>
#include <common/logger_useful.h>

#include <queue>
#include <stack>
#include <mutex>

namespace DB
{

class QueryStatus;
class ExecutingGraph;
class ReadProgressCallback;
using ExecutingGraphPtr = std::unique_ptr<ExecutingGraph>;
using ReadProgressCallbackPtr = std::unique_ptr<ReadProgressCallback>;

struct PipelineExecutorOptions
{
    bool need_processors_profiles = false;
    bool report_processors_profile = false;
};

/// Executes query pipeline.
class PipelineExecutor
{
public:
    /// Get pipeline as a set of processors.
    /// Processors should represent full graph. All ports must be connected, all connected nodes are mentioned in set.
    /// Executor doesn't own processors, just stores reference.
    /// During pipeline execution new processors can appear. They will be added to existing set.
    ///
    /// Explicit graph representation is built in constructor. Throws if graph is not correct.
    explicit PipelineExecutor(Processors & processors_, QueryStatus * elem = nullptr, const PipelineExecutorOptions & executor_options = PipelineExecutorOptions());
    ~PipelineExecutor();

    /// Execute pipeline in multiple threads. Must be called once.
    /// In case of exception during execution throws any occurred.
    void execute(size_t num_threads);

    /// Execute single step. Step will be stopped when yield_flag is true.
    /// Execution is happened in a single thread.
    /// Return true if execution should be continued.
    bool executeStep(std::atomic_bool * yield_flag = nullptr);

    const Processors & getProcessors() const { return processors; }

    const ExecutingGraphPtr & getExecutingGraph() const { return graph; }

    /// Cancel execution. May be called from another thread.
    void cancel();

    /// Checks the query time limits (cancelled or timeout). Throws on cancellation or when time limit is reached and the query uses "break"
    bool checkTimeLimit();
    /// Same as checkTimeLimit but it never throws. It returns false on cancellation or time limit reached
    [[nodiscard]] bool checkTimeLimitSoft();

    String dumpPipeline() const;

    bool isExecutionInitialized() const
    {
        return is_execution_initialized;
    }

    /// Set callback for read progress.
    /// It would be called every time when processor reports read progress.
    void setReadProgressCallback(ReadProgressCallbackPtr callback);

    bool isCancelled() const
    {
        return cancelled;
    }

private:
    Processors & processors;
    std::mutex processors_mutex;

    bool need_processors_profiles;

    bool report_processors_profile;

    ExecutingGraphPtr graph;

    ReadProgressCallbackPtr read_progress_callback;

    using Stack = std::stack<UInt64>;

    /// Queue with pointers to tasks. Each thread will concurrently read from it until finished flag is set.
    /// Stores processors need to be prepared. Preparing status is already set for them.
    TaskQueue<ExecutingGraph::Node> task_queue;

    /// Queue which stores tasks where processors returned Async status after prepare.
    /// If multiple threads are using, main thread will wait for async tasks.
    /// For single thread, will wait for async tasks only when task_queue is empty.
    PollingQueue async_task_queue;
    size_t num_waiting_async_tasks = 0;

    ThreadsQueue threads_queue;
    std::mutex task_queue_mutex;

    /// Flag that checks that initializeExecution was called.
    bool is_execution_initialized = false;
    std::atomic_bool cancelled;
    std::atomic_bool finished;

    Poco::Logger * log = &Poco::Logger::get("PipelineExecutor");

    /// Things to stop execution to expand pipeline.
    struct ExpandPipelineTask
    {
        ExecutingGraph::Node * node_to_expand;
        Stack * stack;
        size_t num_waiting_processing_threads = 0;
        std::mutex mutex;
        std::condition_variable condvar;

        ExpandPipelineTask(ExecutingGraph::Node * node_to_expand_, Stack * stack_)
            : node_to_expand(node_to_expand_), stack(stack_) {}
    };

    std::atomic<size_t> num_processing_executors;
    std::atomic<ExpandPipelineTask *> expand_pipeline_task;

    /// Context for each thread.
    struct ExecutorContext
    {
        /// Will store context for all expand pipeline tasks (it's easy and we don't expect many).
        /// This can be solved by using atomic shard ptr.
        std::list<ExpandPipelineTask> task_list;

        std::queue<ExecutingGraph::Node *> async_tasks;
        std::atomic_bool has_async_tasks = false;

        std::condition_variable condvar;
        std::mutex mutex;
        bool wake_flag = false;

        /// Currently processing node.
        ExecutingGraph::Node * node = nullptr;

        /// Exception from executing thread itself.
        std::exception_ptr exception;

#ifndef NDEBUG
        /// Time for different processing stages.
        UInt64 total_time_ns = 0;
        UInt64 execution_time_ns = 0;
        UInt64 processing_time_ns = 0;
        UInt64 wait_time_ns = 0;
#endif
    };

    std::vector<std::unique_ptr<ExecutorContext>> executor_contexts;
    std::mutex executor_contexts_mutex;

    /// Processor ptr -> node number
    using ProcessorsMap = std::unordered_map<const IProcessor *, UInt64>;
    ProcessorsMap processors_map;

    /// Now it's used to check if query was killed.
    QueryStatus * process_list_element = nullptr;

    /// Graph related methods.
    bool expandPipeline(Stack & stack, UInt64 pid);

    using Queue = std::queue<ExecutingGraph::Node *>;

    /// Pipeline execution related methods.
    void addChildlessProcessorsToStack(Stack & stack);
    bool tryAddProcessorToStackIfUpdated(ExecutingGraph::Edge & edge, Queue & queue, Queue & async_queue, size_t thread_number);
    static void addJob(ExecutingGraph::Node * execution_state);
    // TODO: void addAsyncJob(UInt64 pid);

    /// Prepare processor with pid number.
    /// Check parents and children of current processor and push them to stacks if they also need to be prepared.
    /// If processor wants to be expanded, ExpandPipelineTask from thread_number's execution context will be used.
    bool prepareProcessor(UInt64 pid, size_t thread_number, Queue & queue, Queue & async_queue, std::unique_lock<std::mutex> node_lock);
    bool doExpandPipeline(ExpandPipelineTask * task, bool processing);

    /// Continue executor (in case there are tasks in queue).
    void wakeUpExecutor(size_t thread_num);

    void initializeExecution(size_t num_threads); /// Initialize executor contexts and task_queue.
    void finalizeExecution(); /// Check all processors are finished.

    /// Methods connected to execution.
    void executeImpl(size_t num_threads);
    void executeStepImpl(size_t thread_num, size_t num_threads, std::atomic_bool * yield_flag = nullptr);
    void executeSingleThread(size_t thread_num, size_t num_threads);
    void finish();

    void dumpPipelineToFile(const String & suffix) const;

    void reportProcessorProfile(const IProcessor * processor) const;
    void reportProcessorProfileOnCancel(const Processors & processors) const;
};

using PipelineExecutorPtr = std::shared_ptr<PipelineExecutor>;

}
