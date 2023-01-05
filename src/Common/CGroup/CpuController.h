#pragma once
#include <Core/Types.h>
#include <mutex>
#include <memory>

namespace DB
{

class CpuController
{
private:
    friend class CGroupManager;
    static const String SHARE;
    static const String TASK_FILE;
    String dir_path;
    String name;
    struct PassKey{};
    void init(UInt64 share);
    std::recursive_mutex mutex;
public:
    CpuController(PassKey pass_key, String name, String dir_path, UInt64 share);
    void addTask(size_t tid);
    void addTasks(const std::vector<size_t> & tids);
    std::vector<size_t> getTasks();
    UInt64 getShare();
    void setShare(UInt64 share);
};

using CpuControllerPtr = std::shared_ptr<CpuController>;

}

