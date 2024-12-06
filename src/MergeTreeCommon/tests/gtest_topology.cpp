#include <MergeTreeCommon/CnchServerTopology.h>
#include <MergeTreeCommon/CnchTopologyManager.h>
#include <Protos/DataModelHelpers.h>
#include <gtest/gtest.h>


using namespace DB;

namespace GTEST_TOPOLOGY
{
TEST(CnchServerTopology, DiffTopology)
{
    // Create a Empty Topology
    auto topo = CnchServerTopology();
    EXPECT_TRUE(topo.isSameTopologyWith(topo));
    EXPECT_EQ(topo.diffWith(topo).first.size(), 0);
    EXPECT_EQ(topo.diffWith(topo).second.size(), 0);

    auto topo2 = CnchServerTopology();

    HostWithPorts host1 = HostWithPorts("host1", 1, 2, 3,  "id");
    HostWithPorts host2 = HostWithPorts("host2", 1, 2, 3, "id");
    HostWithPorts host1_v2 = HostWithPorts("host1", 1, 1, 3, "id");
    HostWithPorts host2_v2 = HostWithPorts("host2", 1, 1, 3, "id");

    topo2.addServer(host1);
    topo2.addServer(host2);
    EXPECT_FALSE(topo.isSameTopologyWith(topo2));
    EXPECT_EQ(topo.diffWith(topo2).first.size(), 0);
    EXPECT_EQ(topo.diffWith(topo2).second.size(), 1);
    EXPECT_EQ(topo.diffWith(topo2).second.begin()->second.getServerList().size(), 2);

    topo.addServer(host1);
    EXPECT_FALSE(topo.isSameTopologyWith(topo2));
    EXPECT_EQ(topo.diffWith(topo2).first.size(), 1);
    EXPECT_EQ(topo.diffWith(topo2).second.size(), 1);
    EXPECT_EQ(topo.diffWith(topo2).first.begin()->second.getServerList().size(), 1);
    EXPECT_EQ(topo.diffWith(topo2).second.begin()->second.getServerList().size(), 2);

    topo.addServer(host2);
    EXPECT_TRUE(topo.isSameTopologyWith(topo2));
    EXPECT_EQ(topo.diffWith(topo2).first.size(), 0);
    EXPECT_EQ(topo.diffWith(topo2).second.size(), 0);

    auto topo3 = CnchServerTopology();
    topo3.addServer(host1_v2);
    topo3.addServer(host2_v2);
    EXPECT_FALSE(topo.isSameTopologyWith(topo3));
    EXPECT_EQ(topo.diffWith(topo3).first.size(), 1);
    EXPECT_EQ(topo.diffWith(topo3).second.size(), 1);
    EXPECT_EQ(topo.diffWith(topo3).first.begin()->second.getServerList().size(), 2);
    EXPECT_EQ(topo.diffWith(topo3).second.begin()->second.getServerList().size(), 2);
}

TEST(CnchServerTopology, Serialization)
{
    auto topo = CnchServerTopology();
    EXPECT_TRUE(topo.isSameTopologyWith(topo));
    EXPECT_EQ(topo.diffWith(topo).first.size(), 0);
    EXPECT_EQ(topo.diffWith(topo).second.size(), 0);
    HostWithPorts host1 = HostWithPorts("host1", 1, 2, 3, "id");
    HostWithPorts host2 = HostWithPorts("host2", 1, 2, 3, "id");
    topo.addServer(host1);
    topo.addServer(host2);

    pb::RepeatedPtrField<Protos::DataModelTopology> topology_versions;

    fillTopologyVersions({topo}, topology_versions);
    auto new_topo = createTopologyVersionsFromModel(topology_versions);

    EXPECT_EQ(new_topo.size(), 1);
    EXPECT_TRUE(topo.isSameTopologyWith(new_topo.front()));
}

TEST(CnchServerTopology, VWCompare)
{
    EXPECT_TRUE(CnchTopologyManager::vwStartsWith(
        "cnch-server-default-2.cnch-server-default-headless.cnch-yg.svc.cluster.local.", "cnch-server-default-2"));
    EXPECT_TRUE(CnchTopologyManager::vwStartsWith("cnch-server-default-2", "cnch-server-default-2"));
    EXPECT_TRUE(CnchTopologyManager::vwStartsWith("cnch-server-default-22-fasdfasf", "cnch-server-default-22"));
    EXPECT_FALSE(CnchTopologyManager::vwStartsWith(
        "cnch-server-default-22.cnch-server-default-headless.cnch-yg.svc.cluster.local.", "cnch-server-default-2"));
    EXPECT_FALSE(CnchTopologyManager::vwStartsWith(
        "cnch-server-default-2.cnch-server-default-headless.cnch-yg.svc.cluster.local.", "cnch-asdfasdfasfd"));
    EXPECT_FALSE(CnchTopologyManager::vwStartsWith("cnch-server-default-22-fasdfasf", "cnch-server-default-2"));
    EXPECT_FALSE(CnchTopologyManager::vwStartsWith("cnch-server-d", "cnch-server-default-2"));
}
}
