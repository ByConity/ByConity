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

#pragma once

#include <cstddef>
#include <Common/Exception.h>
#include <common/defines.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_CNCH_BG_THREAD_ACTION;
    extern const int UNKNOWN_CNCH_BG_THREAD_TYPE;
}

/**
 *  Use enum in nested namespace instead enum class.
 *  Because we want to pass it to protos easily, while it offers weaker compile-time check.
 *  So be careful with those enums !
 */
namespace CnchBGThread
{
    /// NOTE: when introducing a new type, remember to update
    /// 1. {Server|Daemon}{Min|Max}Type accordingly
    /// 2. toString(CnchBGThreadType type)
    enum Type : unsigned int
    {
        Empty = 0,

        /// server types
        PartGC = 1,
        MergeMutate = 2,
        Consumer = 3,
        MemoryBuffer = 4, /// NOTE: Just reserved for compatibility.
        DedupWorker = 5,
        Clustering = 6,
        MaterializedMySQL = 7,
        ObjectSchemaAssemble = 8,
        CnchRefreshMaterializedView = 9,
        PartMover = 10,
        ManifestCheckpoint = 11,

        /// DM types
        GlobalGC = 20, /// reserve several entries
        TxnGC = 21,
        AutoStatistics = 22,
        Backup = 23,

        /// worker types (perhaps this should not be included in CnchBGThread?)
        ResourceReport = 30,
    };

    constexpr unsigned int ServerMinType = PartGC;
    constexpr unsigned int ServerMaxType = ManifestCheckpoint;
    constexpr unsigned int NumServerType = ServerMaxType + 1;
    constexpr unsigned int DaemonMinType = GlobalGC;
    constexpr unsigned int DaemonMaxType = Backup;

    /// when introducing a new type, remember to update toCnchBGThreadAction()
    enum Action : unsigned int
    {
        Start = 0,
        Stop = 1, /// Just stop scheduling but not remove
        Remove = 2, /// Remove from memory
        Drop = 3, /// Used when DROP TABLE
        Wakeup = 4,
    };

    enum Status : unsigned int
    {
        Running = 0,
        Stopped = 1,
        Removed = 2,
    };
}
using CnchBGThreadType = CnchBGThread::Type;
using CnchBGThreadAction = CnchBGThread::Action;
using CnchBGThreadStatus = CnchBGThread::Status;

constexpr auto toString(CnchBGThreadType type)
{
    switch (type)
    {
        case CnchBGThreadType::Empty:
            return "Empty";
        case CnchBGThreadType::PartGC:
            return "PartGCThread";
        case CnchBGThreadType::MergeMutate:
            return "MergeMutateThread";
        case CnchBGThreadType::Clustering:
            return "ClusteringThread";
        case CnchBGThreadType::Consumer:
            return "ConsumerManager";
        case CnchBGThreadType::DedupWorker:
            return "DedupWorkerManager";
        case CnchBGThreadType::GlobalGC:
            return "GlobalGCThread";
        case CnchBGThreadType::AutoStatistics:
            return "AutoStatistics";
        case CnchBGThreadType::TxnGC:
            return "TxnGCThread";
        case CnchBGThreadType::ResourceReport:
            return "ResourceReport";
        case CnchBGThreadType::ObjectSchemaAssemble:
            return "ObjectSchemaAssembleThread";
        case CnchBGThreadType::MemoryBuffer:
            return "MemoryBuffer";
        case CnchBGThreadType::MaterializedMySQL:
            return "MaterializedMySQL";
        case CnchBGThreadType::CnchRefreshMaterializedView:
            return "CnchRefreshMaterializedView";
        case CnchBGThreadType::PartMover:
            return "PartMoverThread";
        case CnchBGThreadType::Backup:
            return "BackupThread";
        case CnchBGThreadType::ManifestCheckpoint:
            return "ManifestCheckpoint";
    }
    __builtin_unreachable();
}

constexpr auto isServerBGThreadType(size_t t)
{
    return CnchBGThread::ServerMinType <= t && t <= CnchBGThread::ServerMaxType;
}

inline CnchBGThreadType toServerBGThreadType(size_t t)
{
    if (unlikely(!isServerBGThreadType(t)))
        throw Exception(ErrorCodes::UNKNOWN_CNCH_BG_THREAD_TYPE, "Unknown server bg thread type: {}", t);
    return static_cast<CnchBGThreadType>(t);
}

constexpr auto iDaemonBGThreadType(size_t t)
{
    return CnchBGThread::DaemonMinType <= t && t <= CnchBGThread::DaemonMaxType;
}

inline CnchBGThreadAction toCnchBGThreadAction(size_t action)
{
    if (unlikely(action > CnchBGThreadAction::Wakeup))
        throw Exception(ErrorCodes::UNKNOWN_CNCH_BG_THREAD_ACTION, "Unknown bg thread action: {}", action);

    return static_cast<CnchBGThreadAction>(action);
}

constexpr auto toString(CnchBGThreadAction action)
{
    switch (action)
    {
        case CnchBGThreadAction::Start:
            return "Start";
        case CnchBGThreadAction::Stop:
            return "Stop";
        case CnchBGThreadAction::Remove:
            return "Remove";
        case CnchBGThreadAction::Drop:
            return "Drop";
        case CnchBGThreadAction::Wakeup:
            return "Wakeup";
    }
    __builtin_unreachable();
}

constexpr auto toString(CnchBGThreadStatus status)
{
    switch (status)
    {
        case CnchBGThreadStatus::Running:
            return "Running";
        case CnchBGThreadStatus::Stopped:
            return "Stopped";
        case CnchBGThreadStatus::Removed:
            return "Removed";
    }
    __builtin_unreachable();
}

}
