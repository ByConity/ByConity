#pragma once

#include <Interpreters/Context_fwd.h>
#include <CloudServices/CnchBGThreadCommon.h>

namespace DB
{

namespace DaemonManager
{

struct BGJobInfo;
class IBackgroundJobExecutor
{
public:
    virtual bool start(const BGJobInfo & info) = 0;
    virtual bool stop(const BGJobInfo & info) = 0;
    virtual bool remove(const BGJobInfo & info) = 0;
    virtual bool drop(const BGJobInfo & info) = 0;
    virtual ~IBackgroundJobExecutor() {}
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
    bool start(const BGJobInfo & info) override;
    bool stop(const BGJobInfo & info) override;
    bool remove(const BGJobInfo & info) override;
    bool drop(const BGJobInfo & info) override;
private:
    const Context & context;
    const CnchBGThreadType type;
};

} // end namespace DaemonManager

} // end namespace DB
