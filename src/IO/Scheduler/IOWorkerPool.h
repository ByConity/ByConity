#pragma once

#include <Core/Types.h>

namespace DB::IO::Scheduler {

class IOWorkerPool {
public:
    virtual ~IOWorkerPool() {}

    virtual void startup() = 0;
    virtual void shutdown() = 0;

    virtual String status() const {return "";}
};

}
