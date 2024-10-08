#pragma once

#include <Backups/BackupUtils.h>
#include <Catalog/Catalog.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Protos/cnch_worker_rpc.pb.h>
#include <brpc/controller.h>

namespace DB
{

using BackupCopyTasks = std::vector<Protos::BackupCopyTask>;

struct CopyTaskChecker
{
    brpc::CallId call_id;
    ExceptionHandlerPtr handler;
    BackupCopyTasks tasks_for_client;

    CopyTaskChecker(brpc::CallId call_id_, ExceptionHandlerPtr handler_, BackupCopyTasks tasks_for_client_)
        : call_id(call_id_), handler(handler_), tasks_for_client(tasks_for_client_)
    {
    }
};

using CopyTaskCheckers = std::vector<CopyTaskChecker>;

Protos::BackupCopyTask createBackupCopyTask(
    const String & source_disk, const String & source_path, const String & destination_disk, const String & destination_path);

void sendCopyTasksToWorker(BackupTaskPtr & backup_task, const BackupCopyTasks & copy_tasks, const ContextPtr & context);
}
