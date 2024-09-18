#include <Common/MemoryTrackerBlockerInThread.h>

// MemoryTrackerBlockerInThread
thread_local uint64_t MemoryTrackerBlockerInThread::counter;
thread_local VariableContext MemoryTrackerBlockerInThread::level;
MemoryTrackerBlockerInThread::MemoryTrackerBlockerInThread(VariableContext level_)
    : previous_level(level)
{
    ++counter;
    level = level_;
}
MemoryTrackerBlockerInThread::~MemoryTrackerBlockerInThread()
{
    --counter;
    level = previous_level;
}
