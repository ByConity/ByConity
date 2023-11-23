#include <Storages/DiskCache/FifoPolicy.h>

#include <gtest/gtest.h>

#include <Storages/DiskCache/EvictionPolicy.h>

namespace DB::HybridCache
{
namespace
{
    const Region kRegion0{RegionId{0}, 100};
    const Region kRegion1{RegionId{1}, 100};
    const Region kRegion2{RegionId{2}, 100};
    const Region kRegion3{RegionId{3}, 100};
}

TEST(EvictionPolicy, Fifo)
{
    FifoPolicy policy;
    policy.track(kRegion0);
    policy.track(kRegion2);
    policy.track(kRegion3);
    EXPECT_EQ(kRegion0.id(), policy.evict());
    EXPECT_EQ(kRegion2.id(), policy.evict());
    EXPECT_EQ(kRegion3.id(), policy.evict());
}

TEST(EvictionPolicy, FifoReset)
{
    FifoPolicy policy;
    policy.track(kRegion1);
    policy.track(kRegion2);
    policy.track(kRegion3);
    policy.touch(RegionId{1});
    policy.touch(RegionId{2});
    policy.touch(RegionId{3});
    policy.touch(RegionId{1});
    policy.reset();
    EXPECT_EQ(RegionId{}, policy.evict());
}

TEST(EvictionPolicy, SegmentedFifoSimple)
{
    Region region0{RegionId{0}, 100};
    Region region1{RegionId{1}, 100};
    Region region2{RegionId{2}, 100};
    Region region3{RegionId{3}, 100};
    region0.setPriority(1);
    region1.setPriority(1);
    region2.setPriority(1);
    region3.setPriority(1);

    SegmentedFifoPolicy policy{{1, 1}};

    policy.track(region0); // {[0], []}
    policy.track(region1); // {[0], [1]}
    policy.track(region2); // {[0, 1], [2]}
    policy.track(region3); // {[0, 1], [2, 3]}

    EXPECT_EQ(region0.id(), policy.evict());
    EXPECT_EQ(region1.id(), policy.evict());
    EXPECT_EQ(region2.id(), policy.evict());
    EXPECT_EQ(region3.id(), policy.evict());
    EXPECT_EQ(RegionId{}, policy.evict());
}

TEST(EvictionPolicy, SegmentedFifoRebalance)
{
    Region region0{RegionId{0}, 100};
    Region region1{RegionId{1}, 100};
    Region region2{RegionId{2}, 100};
    Region region3{RegionId{3}, 100};
    region0.setPriority(2);
    region1.setPriority(1);
    region2.setPriority(0);
    region3.setPriority(2);

    SegmentedFifoPolicy policy{{1, 1, 1}};

    policy.track(region0); // {[0], [], []}
    EXPECT_EQ(region0.id(), policy.evict());

    policy.track(region0); // {[0], [], []}
    policy.track(region1); // {[0, 1], [], []}
    policy.track(region2); // {[0, 1, 2], [], []}
    policy.track(region3); // {[0, 1, 2], [], [3]}

    EXPECT_EQ(region0.id(), policy.evict());
    EXPECT_EQ(region1.id(), policy.evict());
    EXPECT_EQ(region2.id(), policy.evict());

    region1.setPriority((1));
    region2.setPriority((1));
    policy.track(region1); // {[3, 1], [], []}
    policy.track(region2); // {[3, 1, 2], [], []}

    EXPECT_EQ(region3.id(), policy.evict());
    EXPECT_EQ(region1.id(), policy.evict());
    EXPECT_EQ(region2.id(), policy.evict());
}
}
