#include <DaemonManager/DaemonJob.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <DaemonManager/Metrics.h>
#include <Interpreters/Context.h>

#include <time.h>
#include <sstream>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DaemonManager
{

bvar::Adder<int> & getExecuteMetric(CnchBGThreadType type)
{
    using namespace DB::DaemonManager::BRPCMetrics;
    switch (type)
    {
        case CnchBGThreadType::PartGC:
            return g_executeImpl_PartGC;
        case CnchBGThreadType::MergeMutate:
            return g_executeImpl_MergeMutate;
        case CnchBGThreadType::Consumer:
            return g_executeImpl_Consumer;
        case CnchBGThreadType::MemoryBuffer:
            return g_executeImpl_MemoryBuffer;
        case CnchBGThreadType::DedupWorker:
            return g_executeImpl_DedupWorker;
        case CnchBGThreadType::GlobalGC:
            return g_executeImpl_GlobalGC;
        case CnchBGThreadType::TxnGC:
            return g_executeImpl_TxnGC;
        case CnchBGThreadType::Clustering:
            return g_executeImpl_Clustering;
        default:
            throw Exception(String{"No metric add for daemon job type "} + toString(type) + ", this is coding mistake", ErrorCodes::LOGICAL_ERROR);
    }
}

bvar::Adder<int> & getExecuteErrorMetric(CnchBGThreadType type)
{
    using namespace DB::DaemonManager::BRPCMetrics;
    switch (type)
    {
        case CnchBGThreadType::PartGC:
            return g_executeImpl_PartGC_error;
        case CnchBGThreadType::MergeMutate:
            return g_executeImpl_MergeMutate_error;
        case CnchBGThreadType::Consumer:
            return g_executeImpl_Consumer_error;
        case CnchBGThreadType::MemoryBuffer:
            return g_executeImpl_MemoryBuffer_error;
        case CnchBGThreadType::DedupWorker:
            return g_executeImpl_DedupWorker_error;
        case CnchBGThreadType::GlobalGC:
            return g_executeImpl_GlobalGC_error;
        case CnchBGThreadType::TxnGC:
            return g_executeImpl_TxnGC_error;
        case CnchBGThreadType::Clustering:
            return g_executeImpl_Clustering_error;
        default:
            throw Exception(String{"No error metric add for daemon job type "} + toString(type) + ", this is coding mistake", ErrorCodes::LOGICAL_ERROR);
    }
}

void DaemonJob::init()
{
    task = getContext()->getSchedulePool().createTask(toString(type), [this]() { execute(); });
}

void DaemonJob::start()
{
    try
    {
        if (task)
            task->activateAndSchedule();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void DaemonJob::stop()
{
    try
    {
        if (task)
            task->deactivate();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}

void DaemonJob::execute()
{
    try
    {
        LOG_TRACE(log, __PRETTY_FUNCTION__);
        getExecuteMetric(getType()) << 1;
        bool ret = executeImpl();
        if (!ret)
            getExecuteErrorMetric(getType()) << 1;
        task->scheduleAfter(interval_ms);
    }
    catch (...)
    {
        tryLogCurrentException(log, String("Error occurs during daemon ") + toString(getType()) + " execution");
        getExecuteErrorMetric(getType()) << 1;
        task->scheduleAfter(interval_ms);
    }
}

} // end namespace DaemonManager
} // end namespace DB

