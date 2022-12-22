#pragma once
#include <memory>
#include <mutex>
#include <utility>
#include <Core/Types.h>


namespace DB
{
struct Cpus
{
    std::vector<bool> data;
    String toString();
    explicit Cpus(std::vector<bool> & data_):data(std::move(data_)){}
    explicit Cpus(String data);
    Cpus(Cpus & cpus){data = cpus.data;}
    Cpus(const Cpus & cpus){data = cpus.data;}
    Cpus & operator= (Cpus && cpus) noexcept
    {
        data = std::move(cpus.data);
        return *this;
    }
    Cpus & operator= (const Cpus & cpus) = default;
    Cpus() = default;

private:
    std::vector<bool> parse(const String & cpus_str);
};

class CpuSet
{
private:
    friend class CGroupManager;
    void createCpuSetController();
    struct PassKey
    {
        explicit PassKey() {}
    };

    String name;
    String dir_path;
    bool enable_exclusive;
    Cpus cpus;
    std::recursive_mutex mutex;

    static const String TASK_FILE;
    static const String CPU_EXCLUSIVE;
    static const String CPUS;
    static const String MEMS;
    static const String PROC;

public:
    CpuSet(String name_, String path_);
    CpuSet(PassKey pass_key, String name_, String path_, Cpus cpus_, bool enable_exclusive_);
    ~CpuSet() = default;
    void addTask(size_t tid);
    void addTasks(const std::vector<size_t> & tids);
    std::vector<size_t> getTasks();
    void resetCpuSet();
    Cpus getCpus();
    String getName(){ return name; }
};

using CpuSetPtr = std::shared_ptr<CpuSet>;
}
