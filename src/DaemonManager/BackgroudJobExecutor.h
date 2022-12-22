#pragma once

#include <Interpreters/Context_fwd.h>
#include <CloudServices/CnchBGThreadCommon.h>
#include <Interpreters/StorageID.h>

namespace DB::DaemonManager
{

struct BGJobInfo;
class IBackgroundJobExecutor
{
public:
    bool start(const BGJobInfo & info);
    bool stop(const BGJobInfo & info);
    bool remove(const BGJobInfo & info);
    bool drop(const BGJobInfo & info);
    bool wakeup(const BGJobInfo & info);

    virtual bool start(const StorageID & storage_id, const String & host_port) = 0;
    virtual bool stop(const StorageID & storage_id, const String & host_port) = 0;
    virtual bool remove(const StorageID & storage_id, const String & host_port) = 0;
    virtual bool drop(const StorageID & storage_id, const String & host_port) = 0;
    virtual bool wakeup(const StorageID & storage_id, const String & host_port) = 0;

    virtual ~IBackgroundJobExecutor() = default;
};

class BackgroundJobExecutor : public IBackgroundJobExecutor
{
public:
    BackgroundJobExecutor(const Context & context, CnchBGThreadType type);
    BackgroundJobExecutor() = delete;
    BackgroundJobExecutor(const BackgroundJobExecutor &) = delete;
    BackgroundJobExecutor(BackgroundJobExecutor &&) = delete;
    BackgroundJobExecutor & operator = (const BackgroundJobExecutor &) = delete;
    BackgroundJobExecutor & operator = (BackgroundJobExecutor &&) = delete;
    bool start(const StorageID & storage_id, const String & host_port) override;
    bool stop(const StorageID & storage_id, const String & host_port) override;
    bool remove(const StorageID & storage_id, const String & host_port) override;
    bool drop(const StorageID & storage_id, const String & host_port) override;
    bool wakeup(const StorageID & storage_id, const String & host_port) override;
private:
    const Context & context;
    const CnchBGThreadType type;
};

}
