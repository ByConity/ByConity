#include <Core/Types.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/DistributedStages/PlanSegment.h>
#include <Interpreters/DistributedStages/AddressInfo.h>
#include <Interpreters/CancellationCode.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Parsers/IAST_fwd.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/Stopwatch.h>
#include <Interpreters/SegmentScheduler.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/Config/ConfigProcessor.h>
#include <gtest/gtest.h>

namespace DB
{
class MockSegmentScheduler : public SegmentScheduler
{
public:
    MockSegmentScheduler() = default;
    virtual ~MockSegmentScheduler() override {}

    virtual AddressInfos sendPlanSegment(PlanSegment *, bool, ContextPtr, std::shared_ptr<DAGGraph>, std::vector<size_t>) override
    {
        AddressInfos res;
        AddressInfo address("127.0.0.1", 9000, "test", "test", 9001, 9002);
        res.emplace_back(std::move(address));
        std::cerr << "call sendPlanSegment!" << std::endl;
        return res;
    }
};
}
