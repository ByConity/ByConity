#include <chrono>
#include <Interpreters/Context.h>
#include <Interpreters/QueueManager.h>
#include <Interpreters/WorkerStatusManager.h>
#include <common/logger_useful.h>

namespace CurrentMetrics
{
extern const Metric BackgroundQueueManagerSchedulePoolTask;
}

namespace DB
{


const char * queueResultStatusToString(QueueResultStatus status)
{
    switch (status)
    {
        case QueueResultStatus::QueueSuccess:
            return "QueueSuccess";
        case QueueResultStatus::QueueCancel:
            return "QueueCancel";
        case QueueResultStatus::QueueFailed:
            return "QueueFailed";
        case QueueResultStatus::QueueOverSize:
            return "QueueOverSize";
        case QueueResultStatus::QueueStop:
            return "QueueStop";
        case QueueResultStatus::QueueTimeOut:
            return "QueueTimeOut";
        default:
            return "QueueUnkown";
    }
}
QueueManagerTriggerTask::~QueueManagerTriggerTask()
{
    stop();
}

QueueManagerTriggerTask::QueueManagerTriggerTask(
    BackgroundSchedulePool & pool_, QueueManager * query_manager_, UInt64 interval_, const std::string & name_)
    : interval(interval_), queue_manager(query_manager_)
{
    task = pool_.createTask(name_, [&] { threadFunc(); });
}

void QueueManagerTriggerTask::threadFunc()
{
    if (!queue_manager->isStop())
    {
        queue_manager->trigger();
        task->scheduleAfter(interval.load(std::memory_order_relaxed));
    }
}

ResourceQeueueThrottler::ResourceQeueueThrottler(ContextPtr context_) : WithContext(context_)
{
}

bool ResourceQeueueThrottler::isThrottling(QueueInfo * queue_info)
{
    auto context = getContext();
    auto worker_status = context->getWorkerStatusManager()->getWorkerGroupStatus(queue_info->vw_name, queue_info->wg_name);
    if (worker_status == nullptr)
    {
        LOG_DEBUG(
            getLogger("QueueManager"),
            "{} ResourceQeueueThrottler {}.{} is nullptr",
            queue_info->query_id,
            queue_info->vw_name,
            queue_info->wg_name);
        return false;
    }
    if (worker_status->getWorkerGroupHealth() == WorkerGroupHealthStatus::Critical)
    {
        LOG_TRACE(getLogger("QueueManager"), "{} ResourceQeueueThrottler throttle", queue_info->query_id);
        return true;
    }
    return false;
}

void VWConcurrencyQeueueThrottler::release(const String & vw)
{
    std::unique_lock lk(mutex);
    LOG_TRACE(getLogger("QueueManager"), "VWConcurrencyQeueueThrottler minus {} parallel size", vw);
    vw_parallel_map[vw]--;
}

QueueThrottlerDeleterPtr VWConcurrencyQeueueThrottler::getDeleter(const String & vw)
{
    return std::make_shared<QeueueThrottlerDeleter>(this, vw);
}

bool VWConcurrencyQeueueThrottler::isThrottling(QueueInfo * queue_info)
{
    std::unique_lock lk(mutex);
    if (vw_parallel_map[queue_info->vw] >= queue_manager->getVWParallelizeSize())
    {
        LOG_TRACE(getLogger("QueueManager"), "VWConcurrencyQeueueThrottler throttle");
        return true;
    }

    LOG_TRACE(
        getLogger("QueueManager"), "{} VWConcurrencyQeueueThrottler add {} parallel size", queue_info->query_id, queue_info->vw);
    vw_parallel_map[queue_info->vw]++;
    queue_info->context->setQueueDeleter(getDeleter(getWorkerGroupName(queue_info->vw_name, queue_info->wg_name)));
    return false;
}

QueueResultStatus QueueManager::enqueue(QueueInfoPtr queue_info_ptr, UInt64 timeout_ms)
{
    if (isStop())
        return QueueResultStatus::QueueStop;
    std::unique_lock lk(mutex);
    auto & current_vw_queue = query_queues[queue_info_ptr->vw];
    if (current_vw_queue.size() > query_queue_size)
    {
        return QueueResultStatus::QueueOverSize;
    }

    current_vw_queue.push_back(queue_info_ptr);

    if (queue_info_ptr->cv.wait_for(lk, std::chrono::milliseconds(timeout_ms)) == std::cv_status::timeout)
    {
        return QueueResultStatus::QueueTimeOut;
    }

    return queue_info_ptr->result;
}

void QueueManager::cancel(const String & query_id)
{
    std::lock_guard lk(mutex);
    for (auto & [vw, vw_queue] : query_queues)
    {
        LOG_DEBUG(log, "cancel {} from vw {}", query_id, vw);
        for (auto iter = vw_queue.begin(); iter != vw_queue.end(); iter++)
        {
            if ((*iter)->query_id == query_id)
            {
                (*iter)->result = QueueResultStatus::QueueCancel;
                (*iter)->cv.notify_one();
                vw_queue.erase(iter);
                return;
            }
        }
    }
}

QueueManager::QueueManager(ContextWeakMutablePtr context_) : WithContext(context_), log(getLogger("QueueManager"))
{
    LOG_DEBUG(log, "Start QueueManager");
    schedule_pool.emplace(1, CurrentMetrics::BackgroundQueueManagerSchedulePoolTask, "QueuePool");
    queue_manager_trigger_task = std::make_unique<QueueManagerTriggerTask>(*schedule_pool, this, 100, "QueueTask");
    queue_manager_trigger_task->start();
    vw_concurrency_throttler = std::make_shared<VWConcurrencyQeueueThrottler>(this);
    throttlers.emplace_back(std::make_shared<ResourceQeueueThrottler>(getContext()));
    throttlers.push_back(vw_concurrency_throttler);
}

void QueueManager::shutdown()
{
    std::unique_lock lk(mutex);
    stop();
    queue_manager_trigger_task->stop();
    schedule_pool.reset();
    for (auto & [vw, vw_queue] : query_queues)
    {
        LOG_TRACE(log, "going to trigger vw {}", vw);
        triggerVW(vw_queue, QueueResultStatus::QueueStop, query_queue_size);
    }
}

QueueManager::~QueueManager()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void QueueManager::trigger()
{
    std::unique_lock lk(mutex);
    for (auto & [vw, vw_queue] : query_queues)
    {
        triggerVW(vw_queue, QueueResultStatus::QueueSuccess, batch_size);
    }
}

void QueueManager::triggerVW(QueryQueue & vw_queue, QueueResultStatus status, size_t trigger_batch_size)
{
    while (trigger_batch_size-- && !vw_queue.empty())
    {
        auto iter = vw_queue.begin();
        auto & queue_info_ptr = *iter;
        LOG_TRACE(log, "process query {} vw {}", queue_info_ptr->query_id, queue_info_ptr->vw);

        for (auto & throttler : throttlers)
        {
            if (throttler->isThrottling(queue_info_ptr.get()))
                return;
        }
        queue_info_ptr->result = status;
        queue_info_ptr->cv.notify_one();
        vw_queue.pop_front();
    }
}

void QueueManager::loadConfig(const QMConfiguration & qm_config)
{
    vw_parallelize_size = qm_config.max_vw_parallelize_size.safeGet();
    batch_size = qm_config.batch_size.safeGet();
    query_queue_size = qm_config.query_queue_size.safeGet();
    queue_manager_trigger_task->setInterval(qm_config.trigger_interval.safeGet());
}
} // DB
