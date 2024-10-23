#include <MergeTreeCommon/CnchServerTopology.h>
#include <gtest/gtest.h>
#include "Protos/DataModelHelpers.h"



using namespace DB;

namespace GTEST_TOPOLOGY
{
TEST(CnchServerTopology, DiffTopology)
{
    // Create a Empty Topology
    auto topo = CnchServerTopology();
    EXPECT_TRUE(topo.isSameTopologyWith(topo));

    auto topo2 = CnchServerTopology();

    HostWithPorts host1 = HostWithPorts("host1", 1, 2, 3, 4, 5, "id");
    HostWithPorts host2 = HostWithPorts("host2", 1, 2, 3, 4, 5, "id");
    HostWithPorts host1_v2 = HostWithPorts("host1", 1, 2, 3, 4, 5, "id");
    host1_v2.exchange_status_port = 333;
    HostWithPorts host2_v2 = HostWithPorts("host2", 1, 2, 3, 4, 5, "id");
    host2_v2.exchange_port = 6;

    topo2.addServer(host1);
    topo2.addServer(host2);
    EXPECT_FALSE(topo.isSameTopologyWith(topo2));

    topo.addServer(host1);
    EXPECT_FALSE(topo.isSameTopologyWith(topo2));

    topo.addServer(host2);
    EXPECT_TRUE(topo.isSameTopologyWith(topo2));

    auto topo3 = CnchServerTopology();
    topo3.addServer(host1_v2);
    topo3.addServer(host2_v2);
    /// exchange_port or exchange_status_port will be ignored.
    EXPECT_TRUE(topo.isSameTopologyWith(topo3));
}

TEST(CnchServerTopology, Serialization)
{
    auto topo = CnchServerTopology();
    EXPECT_TRUE(topo.isSameTopologyWith(topo));
    HostWithPorts host1 = HostWithPorts("host1", 1, 2, 3, 4, 5, "id");
    HostWithPorts host2 = HostWithPorts("host2", 1, 2, 3, 4, 5, "id");
    topo.addServer(host1);
    topo.addServer(host2);

    pb::RepeatedPtrField<Protos::DataModelTopology> topology_versions;

    fillTopologyVersions({topo}, topology_versions);
    auto new_topo = createTopologyVersionsFromModel(topology_versions);

    EXPECT_EQ(new_topo.size(), 1);
    EXPECT_TRUE(topo.isSameTopologyWith(new_topo.front()));
}
}
