#pragma once

#include <CloudServices/CnchBGThreadCommon.h>
#include <Core/BackgroundSchedulePool.h>
#include <Poco/Logger.h>

namespace DB::DaemonManager
{
class IDaemonJob
{
public:
    virtual void init() = 0;
    virtual void start() = 0;
    virtual void stop() = 0;
    virtual ~IDaemonJob() {}
    void setInterval(unsigned int interval_ms_) { interval_ms = interval_ms_; }
protected:
    virtual void execute() = 0;
    unsigned int interval_ms = 0;
};

class DaemonJob : public IDaemonJob, public WithMutableContext
{
public:
    DaemonJob(ContextMutablePtr global_context_, CnchBGThreadType type_)
        : WithMutableContext(global_context_), type{type_}, log(&Poco::Logger::get(toString(type)))
    {}

    void init() override;
    void start() override final;
    void stop() override final;
    CnchBGThreadType getType() const { return type; }
    Poco::Logger * getLog() { return log; }
protected:
    void execute() override final;
    virtual bool executeImpl() = 0;
    const CnchBGThreadType type;
    Poco::Logger * log;
private:
    BackgroundSchedulePool::TaskHolder task;
};

using DaemonJobPtr = std::shared_ptr<DaemonJob>;

} // end namespace DB::DaemonManager
