#pragma once

#include <Common/CGroup/CpuSet.h>
#include <Common/ThreadPool.h>
#include <Interpreters/Context.h>
#include <Core/BackgroundSchedulePool.h>

namespace DB
{
class CpuSetScaleOperator
{
private:
    CpuSetPtr cpu_set;
    float high_water_level;
    float low_water_level;
    Cpus max_cpus;
    float current_cpu_usage = 0.0f;
    size_t sample_cnt = 0;
    Poco::Logger * log;
public:
    CpuSetScaleOperator(CpuSetPtr & cpu_set_, float high_water_level_,
                       float low_water_level_, Cpus max_cpus_)
        :cpu_set(cpu_set_), high_water_level(high_water_level_),
        low_water_level(low_water_level_), max_cpus(max_cpus_),
        log(&Poco::Logger::get("CpuSetScaleOperator")){}

    size_t get_sample_cnt() {return sample_cnt;}

    void reset()
    {
        current_cpu_usage = 0.0f;
        sample_cnt = 0;
    }

    void scale();

    void sample();

    CpuSetScaleOperator(CpuSetScaleOperator && op) noexcept;
};

class CpuSetScaleManager
{
public:
    CpuSetScaleManager(BackgroundSchedulePool & schedule_pool_,
                                size_t interval_ = 1000, size_t scale_sample_cnt_ = 10)
    :log(&Poco::Logger::get("CpuSetScaleManager")), interval(interval_), scale_sample_cnt(scale_sample_cnt_)
    {
        task = schedule_pool_.createTask("CpuSetScaleManager", [this](){ thread_func(); });
    }

    void registerCpuSet(CpuSetPtr cpu_set_, float high_water_level_,
                        float low_water_level_, Cpus max_cpus_);

    void run();

    void cancel() {task->deactivate();}

    void loadCpuSetFromConfig(const Poco::Util::AbstractConfiguration & config);

    ~CpuSetScaleManager(){cancel();}

private:
    std::vector<CpuSetScaleOperator> cpu_set_scale_operators;
    BackgroundSchedulePool::TaskHolder task;
    Poco::Logger * log;
    size_t interval;
    size_t scale_sample_cnt;

    void thread_func();
};

using CpuSetScaleManagerPtr = std::shared_ptr<CpuSetScaleManager>;
}
