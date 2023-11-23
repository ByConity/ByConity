#include <gtest/gtest.h>

#include <thread>
#include <Storages/DiskCache/LruPolicy.h>
#include <Storages/DiskCache/Region.h>

namespace DB::HybridCache
{
namespace
{
    const RegionId kNone{};
    const RegionId kR0{0};
    const RegionId kR1{1};
    const RegionId kR2{2};
    const RegionId kR3{3};
    const Region kRegion0{RegionId{0}, 100};
    const Region kRegion1{RegionId{1}, 100};
    const Region kRegion2{RegionId{2}, 100};
    const Region kRegion3{RegionId{3}, 100};
}

TEST(EvictionPolicy, LruOrder)
{
    LruPolicy policy{0};
    policy.track(kRegion0);
    EXPECT_EQ(kR0, policy.evict());
    EXPECT_EQ(kNone, policy.evict());
    policy.track(kRegion0);
    policy.touch(kR0);
    EXPECT_EQ(kR0, policy.evict());
    EXPECT_EQ(kNone, policy.evict());
    policy.track(kRegion0);
    policy.track(kRegion1);
    EXPECT_EQ(kR0, policy.evict());
    EXPECT_EQ(kR1, policy.evict());
    EXPECT_EQ(kNone, policy.evict());
    policy.track(kRegion0);
    policy.track(kRegion1);
    policy.touch(kR0);

    EXPECT_EQ(kR1, policy.evict());
    EXPECT_EQ(kR0, policy.evict());
    EXPECT_EQ(kNone, policy.evict());
    policy.track(kRegion0);
    policy.track(kRegion1);
    policy.touch(kR0);
    policy.touch(kR1);

    EXPECT_EQ(kR0, policy.evict());
    EXPECT_EQ(kR1, policy.evict());
    EXPECT_EQ(kNone, policy.evict());
    policy.track(kRegion0);
    policy.track(kRegion1);
    policy.touch(kR1);
    policy.touch(kR0);
    policy.memorySize();

    EXPECT_EQ(kR1, policy.evict());
    EXPECT_EQ(kR0, policy.evict());
    EXPECT_EQ(kNone, policy.evict());
    policy.memorySize();
    policy.track(kRegion0);
    policy.track(kRegion1);
    policy.track(kRegion2);
    policy.track(kRegion3);
    policy.touch(kR1);
    policy.touch(kR2);
    policy.touch(kR0);
    policy.touch(kR3);
    policy.touch(kR1);

    EXPECT_EQ(kR2, policy.evict());
    EXPECT_EQ(kR0, policy.evict());
    EXPECT_EQ(kR3, policy.evict());
    EXPECT_EQ(kR1, policy.evict());
    EXPECT_EQ(kNone, policy.evict());

    policy.touch(kR0);
    EXPECT_EQ(kNone, policy.evict());
}

TEST(EvictionPolicy, LruReset)
{
    LruPolicy policy{0};
    policy.track(kRegion1);
    policy.track(kRegion2);
    policy.track(kRegion3);
    policy.touch(kR1);
    policy.touch(kR2);
    policy.touch(kR3);
    policy.touch(kR1);
    
    policy.reset();
    EXPECT_EQ(kNone, policy.evict());
}
}
