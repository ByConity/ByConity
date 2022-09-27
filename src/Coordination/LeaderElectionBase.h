#include <chrono>
#include <cstddef>
#include <thread>
#include <common/types.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePool.h>
#include <Coordination/LeaderElection.h>

namespace DB
{

class LeaderElectionBase
{
public:
    explicit LeaderElectionBase(size_t wait_ms_): wait_ms(wait_ms_) {}

    virtual ~LeaderElectionBase() = default;

    virtual void onLeader() = 0;
    virtual void exitLeaderElection() = 0;
    virtual void enterLeaderElection() = 0;

    void startLeaderElection(BackgroundSchedulePool & pool)
    {
        enterLeaderElection();

        restart_task = pool.createTask("ElectionRestartTask", [&]() { run(); });
        restart_task->activateAndSchedule();
    }

    void run()
    {
        try
        {
            if (!current_zookeeper || !current_zookeeper->expired())
            {
                exitLeaderElection();
                enterLeaderElection();
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        restart_task->scheduleAfter(wait_ms);
    }

protected:
    size_t wait_ms;
    zkutil::ZooKeeperPtr current_zookeeper;
    zkutil::LeaderElectionPtr leader_election;
    BackgroundSchedulePool::TaskHolder restart_task;
};

}
