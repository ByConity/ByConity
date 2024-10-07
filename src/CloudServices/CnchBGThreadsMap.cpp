/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <CloudServices/CnchBGThreadsMap.h>

#include <CloudServices/CnchRefreshMaterializedViewThread.h>
#include <CloudServices/CnchMergeMutateThread.h>
#include <CloudServices/CnchPartGCThread.h>
#include <CloudServices/CnchManifestCheckpointThread.h>
#include <Interpreters/Context.h>
#include <ResourceManagement/ResourceReporter.h>
#include <Storages/Kafka/CnchKafkaConsumeManager.h>
#include <CloudServices/CnchPartGCThread.h>
#include <CloudServices/DedupWorkerManager.h>
#include <CloudServices/ReclusteringManagerThread.h>
#include <CloudServices/CnchObjectColumnSchemaAssembleThread.h>

#include <Databases/MySQL/MaterializedMySQLSyncThreadManager.h>
#include <CloudServices/CnchPartMoverThread.h>

#include <regex>

namespace DB
{

namespace ErrorCodes
{
    extern const int CNCH_BG_THREAD_NOT_FOUND;
}


CnchBGThreadsMap::CnchBGThreadsMap(ContextPtr global_context_, CnchBGThreadType t) : WithContext(global_context_), type(t)
{
}

CnchBGThreadPtr CnchBGThreadsMap::getThread(const StorageID & storage_id) const
{
    auto t = tryGetThread(storage_id);
    if (!t)
        throw Exception(
            ErrorCodes::CNCH_BG_THREAD_NOT_FOUND, "Background thread {} for {} not found", toString(type), storage_id.getNameForLogs());
    return t;
}

CnchBGThreadPtr CnchBGThreadsMap::createThread(const StorageID & storage_id)
{
    if (type == CnchBGThreadType::PartGC)
    {
        return std::make_shared<CnchPartGCThread>(getContext(), storage_id);
    }
    else if (type == CnchBGThreadType::MergeMutate)
    {
        return std::make_shared<CnchMergeMutateThread>(getContext(), storage_id);
    }
    else if (type == CnchBGThreadType::Consumer)
    {
        return std::make_shared<CnchKafkaConsumeManager>(getContext(), storage_id);
    }
    else if (type == CnchBGThreadType::DedupWorker)
    {
        return std::make_shared<DedupWorkerManager>(getContext(), storage_id);
    }
    else if (type == CnchBGThreadType::Clustering)
    {
        return std::make_shared<ReclusteringManagerThread>(getContext(), storage_id);
    }
    else if (type == CnchBGThreadType::ObjectSchemaAssemble)
    {
        return std::make_shared<CnchObjectColumnSchemaAssembleThread>(getContext(), storage_id);
    }
    else if (type == CnchBGThreadType::MaterializedMySQL)
    {
        return std::make_shared<MaterializedMySQLSyncThreadManager>(getContext(), storage_id);
    }
    else if (type == CnchBGThreadType::CnchRefreshMaterializedView)
    {
        return std::make_shared<CnchRefreshMaterializedViewThread>(getContext(), storage_id);
    }
    else if (type == CnchBGThreadType::PartMover)
    {
        return std::make_shared<CnchPartMoverThread>(getContext(), storage_id);
    }
    else if (type == CnchBGThreadType::ManifestCheckpoint)
    {
        return std::make_shared<CnchManifestCheckpointThread>(getContext(), storage_id);
    }
    else
    {
        throw Exception(String("Not supported background thread ") + toString(type), ErrorCodes::NOT_IMPLEMENTED);
    }
}

void CnchBGThreadsMap::controlThread(const StorageID & storage_id, CnchBGThreadAction action)
{
    if (storage_id.empty())
    {
        if (action == CnchBGThreadAction::Remove)
        {
            stopAll();
            clear();
        }
        return;
    }

    switch (action)
    {
        case CnchBGThreadAction::Start:
            startThread(storage_id);
            break;

        case CnchBGThreadAction::Stop:
            stopThread(storage_id);
            break;

        case CnchBGThreadAction::Remove:
            tryRemoveThread(storage_id);
            break;

        case CnchBGThreadAction::Drop:
            tryDropThread(storage_id);
            break;

        case CnchBGThreadAction::Wakeup:
            wakeupThread(storage_id);
            break;
    }
}

CnchBGThreadPtr CnchBGThreadsMap::getOrCreateThread(const StorageID & storage_id)
{
    auto [t, _] = getOrCreate(storage_id.uuid, [this, storage_id] { return createThread(storage_id); });
    return std::move(t);
}

CnchBGThreadPtr CnchBGThreadsMap::startThread(const StorageID & storage_id)
{
    auto t = getOrCreateThread(storage_id);

    const auto & pattern = getContext()->getSettingsRef().blocklist_for_merge_thread_regex.value;
    if (type == CnchBGThread::MergeMutate && !pattern.empty() && std::regex_search(storage_id.table_name, std::regex(pattern)))
    {
        // Create new MergeThread but not start it,
        // to prevent daemon_manager send duplicate startMergeThread request
        auto log = getLogger("CnchBGThreadsMap");
        LOG_DEBUG(log, "Cancel start MergeThread for table {}, since table on the blacklist.", storage_id.getNameForLogs());
    }
    else
        t->start();
    return t;
}

void CnchBGThreadsMap::stopThread(const StorageID & storage_id) const
{
    getThread(storage_id)->stop();
}

void CnchBGThreadsMap::tryRemoveThread(const StorageID & storage_id)
{
    auto t = tryGetThread(storage_id);
    if (!t)
    {
        LOG_DEBUG(getLogger("CnchBGThreadsMap"), "{} for {} not found", toString(type), storage_id.getNameForLogs());
        return;
    }

    t->stop();
    erase(storage_id.uuid);
}

void CnchBGThreadsMap::tryDropThread(const StorageID & storage_id)
{
    auto t = tryGetThread(storage_id);
    if (!t)
    {
        LOG_DEBUG(getLogger("CnchBGThreadsMap"), "{} for {} not found", toString(type), storage_id.getNameForLogs());
        return;
    }

    t->stop();
    erase(storage_id.uuid);
    t->drop();
}

void CnchBGThreadsMap::wakeupThread(const StorageID & storage_id)
{
    getOrCreateThread(storage_id)->wakeup();
}

std::map<StorageID, CnchBGThreadStatus> CnchBGThreadsMap::getStatusMap() const
{
    std::map<StorageID, CnchBGThreadStatus> res;
    withAll([&res](const CnchBGThreadPtr & t) { res.try_emplace(t->getStorageID(), t->getThreadStatus()); });
    return res;
}

void CnchBGThreadsMap::stopAll()
{
    for (auto & [_, t] : getAll())
        t->stop();
}

void CnchBGThreadsMap::cleanup()
{
    std::vector<CnchBGThreadPtr> threads; /// Hold threads to release after unlocking

    {
        std::lock_guard lock(cells_mutex);
        for (auto it = cells.begin(); it != cells.end();)
        {
            if (it->second->error())
            {
                LOG_WARNING(
                    getLogger("CnchBGThreadsMap"),
                    "{} for {} got error, remove it",
                    toString(type),
                    it->second->getStorageID().getNameForLogs());
                threads.push_back(std::move(it->second));
                it = cells.erase(it);
            }
            else
                ++it;
        }
    }
}

CnchBGThreadsMapArray::CnchBGThreadsMapArray(ContextPtr global_context_) : WithContext(global_context_)
{
    for (auto i = CnchBGThread::ServerMinType; i <= CnchBGThread::ServerMaxType; ++i)
        threads_array[i] = std::make_unique<CnchBGThreadsMap>(global_context_, static_cast<CnchBGThreadType>(i));

    if (global_context_->getServerType() == ServerType::cnch_worker && global_context_->getResourceManagerClient())
    {
        resource_reporter_task = std::make_unique<ResourceReporterTask>(global_context_);
    }

    cleaner = global_context_->getSchedulePool().createTask("CnchBGThreadsCleaner", [this] { cleanThread(); });
    cleaner->activateAndSchedule();
}

CnchBGThreadsMapArray::~CnchBGThreadsMapArray()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void CnchBGThreadsMapArray::shutdown()
{
    ThreadPool pool(CnchBGThread::ServerMaxType - CnchBGThread::ServerMinType + 1);

    for (auto i = CnchBGThread::ServerMinType; i <= CnchBGThread::ServerMaxType; ++i)
    {
        if (auto * t = threads_array[i].get())
            pool.scheduleOrThrowOnError([t] { t->stopAll(); });
    }

    pool.wait();

    if (resource_reporter_task)
        resource_reporter_task.reset();

    /// `cleaner` must be stopped as well
    if (cleaner)
        cleaner->deactivate();
}

UInt32 CnchBGThreadsMapArray::getEpoch()
{
    if (unlikely(!resource_reporter_task))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Resource reporter is not initialized");
    return resource_reporter_task->getEpoch();
}

void CnchBGThreadsMapArray::cleanThread()
{
    try
    {
        for (auto i = CnchBGThread::ServerMinType; i <= CnchBGThread::ServerMaxType; ++i)
            threads_array[i]->cleanup();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    cleaner->scheduleAfter(30 * 1000);
}

void CnchBGThreadsMapArray::startResourceReport()
{
    resource_reporter_task->start();
}

void CnchBGThreadsMapArray::stopResourceReport()
{
    resource_reporter_task->stop();
}

bool CnchBGThreadsMapArray::isResourceReportRegistered()
{
    return resource_reporter_task->registered();
}

}
